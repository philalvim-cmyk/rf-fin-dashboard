import streamlit as st
from pathlib import Path
import pandas as pd
import datetime as dt
from numbers import Number
import re
import io
import hashlib
from pandas.errors import OutOfBoundsDatetime


# ============================================================
# CONFIG
# ============================================================
EXPORTS_DIR = Path("data/exports")
PROCESSED_DIR = Path("data/processed")

EXPORT_GLOB = "export_rf_*.xlsx"
SHEET_DINAMICA = "DINAMICA_CONSOLIDADO"
SHEET_DETALHADO = "DETALHADO_CLASSIFICADO"
HIST_EXPORT_2025_NAME = "export_rf_hist_2025.xlsx"

TRANSFER_LABEL = "TRANSFERENCIA ENTRE CONTAS"


# ============================================================
# PAGE
# ============================================================
st.set_page_config(page_title="Dashboard TDC", layout="wide")
st.title("📊 Dashboard TDC")
st.caption("Modo produto: histórico ON + anti-duplicação ON (usa apenas o export mais novo de cada competência).")


# ============================================================
# HELPERS (dirs / datas / status / valores)
# ============================================================
def ensure_dirs():
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def list_exports():
    ensure_dirs()
    return sorted(EXPORTS_DIR.glob(EXPORT_GLOB), key=lambda p: p.stat().st_mtime)


def find_latest_export():
    files = sorted(list_exports(), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def safe_parse_timestamp(ts):
    """
    Garante Timestamp dentro do range do pandas datetime64[ns] (~1677..2262).
    Fora disso, retorna NaT.
    """
    if ts is None or ts is pd.NaT:
        return pd.NaT
    try:
        ts = pd.Timestamp(ts)
    except (OutOfBoundsDatetime, OverflowError, ValueError, TypeError):
        return pd.NaT

    try:
        if ts.year < 1900 or ts.year > 2262:
            return pd.NaT
    except Exception:
        return pd.NaT

    return ts


def excel_date_to_datetime(x):
    """
    Converte datas que podem vir como:
    - datetime/date
    - número serial do Excel (ex.: 46044) inclusive numpy (Number)
    - string numérica "46044" ou "46044.0"
    - string de data
    Retorna Timestamp válido OU NaT (nunca estoura OutOfBounds).
    """
    if x is None:
        return pd.NaT

    # NaNs
    try:
        if pd.isna(x):
            return pd.NaT
    except Exception:
        pass

    # datetime/date
    if isinstance(x, (dt.datetime, dt.date)):
        try:
            ts = pd.to_datetime(x, errors="coerce")
            return safe_parse_timestamp(ts)
        except (OutOfBoundsDatetime, OverflowError, ValueError):
            return pd.NaT

    # string
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return pd.NaT

        # string numérica serial: "46044" / "46044.0"
        if re.fullmatch(r"\d+(\.\d+)?", s):
            try:
                num = float(s)
                if num > 30000:
                    ts = pd.to_datetime("1899-12-30") + pd.to_timedelta(num, unit="D")
                    return safe_parse_timestamp(ts)
            except (OutOfBoundsDatetime, OverflowError, ValueError):
                return pd.NaT

        # string de data
        try:
            ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
            return safe_parse_timestamp(ts)
        except (OutOfBoundsDatetime, OverflowError, ValueError):
            return pd.NaT

    # número (inclui numpy)
    if isinstance(x, Number):
        try:
            num = float(x)
            if num > 30000:
                ts = pd.to_datetime("1899-12-30") + pd.to_timedelta(num, unit="D")
                return safe_parse_timestamp(ts)
            ts = pd.to_datetime(num, errors="coerce", unit="D", origin="1899-12-30")
            return safe_parse_timestamp(ts)
        except (OutOfBoundsDatetime, OverflowError, ValueError):
            return pd.NaT

    # fallback final
    try:
        ts = pd.to_datetime(x, errors="coerce", dayfirst=True)
        return safe_parse_timestamp(ts)
    except (OutOfBoundsDatetime, OverflowError, ValueError):
        return pd.NaT


def normalize_status(s):
    s = (s or "").strip().upper()
    if "CANCEL" in s:
        return "CANCELADO"
    if "QUIT" in s:
        return "QUITADO"
    if "PARCIAL" in s:
        return "PARCIAL"
    if "ABERTO" in s:
        return "ABERTO"
    if "BAIXADO" in s:
        return "BAIXADO"
    return s if s else "OUTRO"


def effective_value(row):
    """
    Valor correto para KPIs:
    - CANCELADO: 0
    - QUITADO: VALOR_QUITADO (se >0), senão VALOR_TITULO
    - ABERTO/PARCIAL/BAIXADO/OUTRO: VALOR_SALDO (se >0), senão VALOR_TITULO
    """
    stt = row.get("STATUS_NORM", "")
    vt = pd.to_numeric(row.get("VALOR_TITULO", 0), errors="coerce")
    vs = pd.to_numeric(row.get("VALOR_SALDO", 0), errors="coerce")
    vq = pd.to_numeric(row.get("VALOR_QUITADO", 0), errors="coerce")

    vt = 0 if pd.isna(vt) else vt
    vs = 0 if pd.isna(vs) else vs
    vq = 0 if pd.isna(vq) else vq

    if stt == "CANCELADO":
        return 0.0
    if stt == "QUITADO":
        return float(vq) if vq > 0 else float(vt)
    return float(vs) if vs > 0 else float(vt)


# ============================================================
# CACHE / LOADERS
# ============================================================
@st.cache_data(ttl=600)
def load_export(path: str, mtime: float):
    """Lê as 2 abas padrão. Cacheado por (path + mtime)."""
    df_dyn = pd.read_excel(path, sheet_name=SHEET_DINAMICA, engine="openpyxl")
    df_det = pd.read_excel(path, sheet_name=SHEET_DETALHADO, engine="openpyxl")
    return df_dyn, df_det


@st.cache_data(ttl=600)
def read_competencias_from_export(path: str, mtime: float):
    """Lê apenas DINAMICA_CONSOLIDADO para descobrir quais COMPETENCIA_MES existem no arquivo."""
    df_dyn = pd.read_excel(path, sheet_name=SHEET_DINAMICA, engine="openpyxl")
    if "COMPETENCIA_MES" not in df_dyn.columns:
        return set()
    comps = df_dyn["COMPETENCIA_MES"].dropna().astype(str).str.strip().tolist()
    return set([c for c in comps if c and c.lower() != "nan"])


def choose_latest_export_per_competencia(files):
    """
    Solução A: para cada COMPETENCIA_MES, escolhe o export mais recente (maior mtime).
    Retorna lista de arquivos únicos.
    """
    best = {}
    best_mtime = {}

    for f in files:
        mtime = f.stat().st_mtime
        comps = read_competencias_from_export(str(f), mtime)
        for comp in comps:
            if (comp not in best_mtime) or (mtime > best_mtime[comp]):
                best[comp] = f
                best_mtime[comp] = mtime

    comps_sorted = sorted(best.keys())  # YYYY-MM
    chosen_files = []
    seen = set()

    for comp in comps_sorted:
        f = best[comp]
        if f not in seen:
            chosen_files.append(f)
            seen.add(f)

    return chosen_files


# ============================================================
# SIDEBAR: modo produto (padrão ON)
# ============================================================
use_hist = st.sidebar.checkbox("📚 Usar histórico (2025+2026...)", value=True)
use_solution_a = st.sidebar.checkbox("✅ Anti-duplicação por competência", value=True)


# ============================================================
# 0) EXPORTS: carregar base
# ============================================================
latest = find_latest_export()
if latest is None:
    st.error("Não encontrei nenhum export em data/exports/export_rf_*.xlsx")
    st.info("Gere um export em 'Aplicar Histórico' e volte aqui.")
    st.stop()

st.caption(
    f"📁 Último export detectado: **{latest.name}** • "
    f"{dt.datetime.fromtimestamp(latest.stat().st_mtime)}"
)

col_btn, col_info = st.columns([1, 3])
with col_btn:
    if st.button("🔄 Atualizar dashboard (reler exports)"):
        st.cache_data.clear()
        st.rerun()
with col_info:
    st.write("Recarrega exports conforme histórico/anti-duplicação.")


all_files = list_exports()

if use_hist:
    files_to_load = choose_latest_export_per_competencia(all_files) if use_solution_a else all_files
else:
    files_to_load = [latest]

dyn_list, det_list = [], []
load_errors = []

for f in files_to_load:
    try:
        d_dyn, d_det = load_export(str(f), f.stat().st_mtime)
        dyn_list.append(d_dyn)
        det_list.append(d_det)
    except Exception as e:
        load_errors.append((f.name, str(e)))

df_dyn = pd.concat(dyn_list, ignore_index=True) if dyn_list else pd.DataFrame()
df_det = pd.concat(det_list, ignore_index=True) if det_list else pd.DataFrame()

if df_det.empty:
    st.warning("Não foi possível carregar DETALHADO_CLASSIFICADO. Verifique os exports em data/exports.")
    if load_errors:
        with st.expander("⚠️ Erros ao ler exports (diagnóstico)"):
            for name, err in load_errors[:20]:
                st.write(f"- {name}: {err}")
    st.stop()


# ============================================================
# 1) Preparação do DETALHADO
# ============================================================
df = df_det.copy()

for c in ["DATA_EMISSAO", "DATA_VENCIMENTO", "DATA_PAGAMENTO", "DATA_CREDITO"]:
    if c in df.columns:
        df[c] = df[c].apply(excel_date_to_datetime)

df["STATUS_NORM"] = df.get("SITUACAO", "").astype(str).apply(normalize_status)
df["VALOR_EFETIVO"] = df.apply(effective_value, axis=1)

if "COMPETENCIA_MES" in df.columns:
    df["COMPETENCIA_MES"] = df["COMPETENCIA_MES"].astype(str).str.strip()

df["IS_TRANSFERENCIA"] = (
    df.get("CLASSIFICACAO_RF", "").astype(str).str.upper().eq(TRANSFER_LABEL)
)


# ============================================================
# 2) Sidebar filtros
# ============================================================
with st.sidebar:
    st.header("Filtros do Dashboard")

    if "COMPETENCIA_MES" in df.columns:
        comp_list = sorted([c for c in df["COMPETENCIA_MES"].dropna().unique() if c and str(c).lower() != "nan"])
    else:
        comp_list = []

    anos = sorted({str(c)[:4] for c in comp_list if len(str(c)) >= 7})
    meses = [f"{m:02d}" for m in range(1, 13)]

    ano_sel = st.multiselect("Ano", anos, default=anos)
    mes_sel = st.multiselect("Mês", meses, default=[])

    tipo_sel = st.multiselect(
        "Tipo (PAGAR/RECEBER)",
        sorted(df.get("TIPO", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
        default=[],
    )
    status_sel = st.multiselect(
        "Situação",
        sorted(df.get("STATUS_NORM", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
        default=[],
    )
    cnpj_sel = st.multiselect(
        "CNPJ Empresa",
        sorted(df.get("CNPJ_EMPRESA", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
        default=[],
    )
    cc_sel = st.multiselect(
        "Centro de custo",
        sorted(df.get("NOME_CENTRO_CUSTO", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
        default=[],
    )
    rub_sel = st.multiselect(
        "Rubrica (CLASSIFICAÇÃO RF)",
        sorted(df.get("CLASSIFICACAO_RF", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
        default=[],
    )
    pessoa_sel = st.multiselect(
        "Pessoa (cliente/fornecedor)",
        sorted(df.get("NOME_PESSOA", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
        default=[],
    )
    banco_sel = st.multiselect(
        "Banco/Portador",
        sorted(df.get("NOME_PORTADOR", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
        default=[],
    )
    forma_sel = st.multiselect(
        "Forma de pagamento",
        sorted(df.get("FORMA_PAGAMENTO", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
        default=[],
    )

    st.divider()
    st.caption("Transferências são KPI próprio e entram no histórico sem duplicar por competência.")


# ============================================================
# 3) Aplicar filtros
# ============================================================
df_f = df.copy()

if ano_sel and "COMPETENCIA_MES" in df_f.columns:
    df_f = df_f[df_f["COMPETENCIA_MES"].astype(str).str[:4].isin(ano_sel)]

if mes_sel and "COMPETENCIA_MES" in df_f.columns:
    df_f = df_f[df_f["COMPETENCIA_MES"].astype(str).str[5:7].isin(mes_sel)]

if tipo_sel and "TIPO" in df_f.columns:
    df_f = df_f[df_f["TIPO"].astype(str).isin(tipo_sel)]

if status_sel and "STATUS_NORM" in df_f.columns:
    df_f = df_f[df_f["STATUS_NORM"].astype(str).isin(status_sel)]

if cnpj_sel and "CNPJ_EMPRESA" in df_f.columns:
    df_f = df_f[df_f["CNPJ_EMPRESA"].astype(str).isin(cnpj_sel)]

if cc_sel and "NOME_CENTRO_CUSTO" in df_f.columns:
    df_f = df_f[df_f["NOME_CENTRO_CUSTO"].astype(str).isin(cc_sel)]

if rub_sel and "CLASSIFICACAO_RF" in df_f.columns:
    df_f = df_f[df_f["CLASSIFICACAO_RF"].astype(str).isin(rub_sel)]

if pessoa_sel and "NOME_PESSOA" in df_f.columns:
    df_f = df_f[df_f["NOME_PESSOA"].astype(str).isin(pessoa_sel)]

if banco_sel and "NOME_PORTADOR" in df_f.columns:
    df_f = df_f[df_f["NOME_PORTADOR"].astype(str).isin(banco_sel)]

if forma_sel and "FORMA_PAGAMENTO" in df_f.columns:
    df_f = df_f[df_f["FORMA_PAGAMENTO"].astype(str).isin(forma_sel)]


# ============================================================
# 4) KPIs
# ============================================================
df_receber = df_f[df_f.get("TIPO", "").astype(str).str.upper() == "RECEBER"]
df_pagar = df_f[df_f.get("TIPO", "").astype(str).str.upper() == "PAGAR"]

rec_quitado = df_receber[df_receber["STATUS_NORM"] == "QUITADO"]
pag_quitado = df_pagar[df_pagar["STATUS_NORM"] == "QUITADO"]

rec_aberto = df_receber[df_receber["STATUS_NORM"].isin(["ABERTO", "PARCIAL"])]
pag_aberto = df_pagar[df_pagar["STATUS_NORM"].isin(["ABERTO", "PARCIAL"])]

kpi_recebido = rec_quitado["VALOR_EFETIVO"].sum()
kpi_pago = pag_quitado["VALOR_EFETIVO"].sum()
kpi_aberto_receber = rec_aberto["VALOR_EFETIVO"].sum()
kpi_aberto_pagar = pag_aberto["VALOR_EFETIVO"].sum()

def avg_days(df_in: pd.DataFrame, start_col: str, end_col: str) -> float:
    if df_in.empty or start_col not in df_in.columns or end_col not in df_in.columns:
        return 0.0
    aux = df_in[[start_col, end_col]].dropna()
    if aux.empty:
        return 0.0
    days = (aux[end_col] - aux[start_col]).dt.days
    days = days[days.notna()]
    return float(days.mean()) if not days.empty else 0.0

pm_receb = avg_days(rec_quitado, "DATA_EMISSAO", "DATA_CREDITO")
if pm_receb == 0:
    pm_receb = avg_days(rec_quitado, "DATA_EMISSAO", "DATA_PAGAMENTO")
if pm_receb == 0:
    pm_receb = avg_days(rec_quitado, "DATA_VENCIMENTO", "DATA_CREDITO")
if pm_receb == 0:
    pm_receb = avg_days(rec_quitado, "DATA_VENCIMENTO", "DATA_PAGAMENTO")

pm_pag = avg_days(pag_quitado, "DATA_EMISSAO", "DATA_PAGAMENTO")
if pm_pag == 0:
    pm_pag = avg_days(pag_quitado, "DATA_VENCIMENTO", "DATA_PAGAMENTO")

df_transf = df_f[df_f["IS_TRANSFERENCIA"]]
kpi_transf_total = df_transf["VALOR_EFETIVO"].sum()

fmt_money = lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
fmt_days = lambda x: f"{x:.0f} dias"

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Recebido (Quitado)", fmt_money(kpi_recebido))
c2.metric("Pago (Quitado)", fmt_money(kpi_pago))
c3.metric("A Receber (Aberto)", fmt_money(kpi_aberto_receber))
c4.metric("A Pagar (Aberto)", fmt_money(kpi_aberto_pagar))
c5.metric("Prazo médio receb.", fmt_days(pm_receb))
c6.metric("Transferências (R$)", fmt_money(kpi_transf_total))


# ============================================================
# 5) IMPORTADOR 2025 (robusto e "anti-piscar")
# ============================================================
st.divider()
st.subheader("📥 Importar base 2025 consolidada (gera export histórico)")
st.write(
    "Cria `export_rf_hist_2025.xlsx` em `data/exports/` com as duas abas padrão. "
    "Datas inválidas/futuras demais viram NaT e não quebram."
)

def detect_class_col(cols):
    candidates = [
        "CLASSIFICAÇÃO RF", "CLASSIFICACAO RF",
        "CLASSIFICACAO_RF", "CLASSIFICAÇÃO_RF",
        "CLASSIFICACAO", "CLASSIFICAÇÃO",
    ]
    for c in candidates:
        if c in cols:
            return c
    return None

def detect_compet_col(cols):
    candidates = ["MÊS / ANO", "MES / ANO", "MÊS/ANO", "MES/ANO", "MES_ANO", "MÊS_ANO"]
    for c in candidates:
        if c in cols:
            return c
    return None

def normalize_competencia(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    s = str(v).strip()
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s

    d = excel_date_to_datetime(v)
    if d is not pd.NaT:
        return str(d.to_period("M"))

    m = re.match(r"^\s*(\d{1,2})\s*/\s*(\d{4})\s*$", s)
    if m:
        mm = int(m.group(1))
        yy = int(m.group(2))
        if 1 <= mm <= 12:
            return f"{yy:04d}-{mm:02d}"

    return None

def build_hist_export_from_base(base_path: str, sheet_name: str, out_path: Path):
    df_base = pd.read_excel(base_path, sheet_name=sheet_name, engine="openpyxl")
    df_base.rename(columns={c: str(c).strip() for c in df_base.columns}, inplace=True)

    class_col = detect_class_col(df_base.columns)
    if class_col is None:
        raise ValueError("Não encontrei a coluna de classificação (ex.: 'CLASSIFICAÇÃO RF').")

    required = ["TIPO", "VALOR_TITULO", "DATA_EMISSAO", "NOME_CONTA", "NOME_PESSOA", "NOME_CENTRO_CUSTO"]
    missing = [c for c in required if c not in df_base.columns]
    if missing:
        raise ValueError(f"Base 2025 sem colunas mínimas esperadas: {missing}")

    df_hist = df_base.copy()

    for c in ["DATA_EMISSAO", "DATA_VENCIMENTO", "DATA_PAGAMENTO", "DATA_CREDITO"]:
        if c in df_hist.columns:
            df_hist[c] = df_hist[c].apply(excel_date_to_datetime)

    comp_col = detect_compet_col(df_hist.columns)
    if comp_col:
        df_hist["COMPETENCIA_MES"] = df_hist[comp_col].apply(normalize_competencia)
    else:
        df_hist["COMPETENCIA_MES"] = df_hist["DATA_EMISSAO"].dt.to_period("M").astype(str)

    df_hist["COMPETENCIA_MES"] = df_hist["COMPETENCIA_MES"].astype(str)
    df_hist.loc[df_hist["COMPETENCIA_MES"].isin(["NaT", "None", "nan", ""]), "COMPETENCIA_MES"] = None

    df_hist["NOME_CONTA_N"] = df_hist["NOME_CONTA"].astype(str).str.upper().str.strip()
    df_hist["NOME_PESSOA_N"] = df_hist["NOME_PESSOA"].astype(str).str.upper().str.strip()
    df_hist["NOME_CC_N"] = df_hist["NOME_CENTRO_CUSTO"].astype(str).str.upper().str.strip()

    df_hist["CLASSIFICACAO_RF"] = df_hist[class_col].astype(str).str.strip()
    df_hist["CLASSIFICACAO_ORIGEM"] = "IMPORT_2025"
    df_hist["FLAGS"] = ""

    for c in ["VALOR_SALDO", "VALOR_QUITADO", "SITUACAO", "NOME_PORTADOR", "FORMA_PAGAMENTO", "CNPJ_EMPRESA"]:
        if c not in df_hist.columns:
            df_hist[c] = ""

    df_hist["VALOR_TITULO"] = pd.to_numeric(df_hist["VALOR_TITULO"], errors="coerce").fillna(0)

    df_dyn_hist = (
        df_hist.dropna(subset=["COMPETENCIA_MES"])
        .groupby(["COMPETENCIA_MES", "CLASSIFICACAO_RF"], dropna=False)["VALOR_TITULO"]
        .sum()
        .reset_index()
        .rename(columns={"VALOR_TITULO": "VALOR"})
        .sort_values(["COMPETENCIA_MES", "CLASSIFICACAO_RF"])
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_dyn_hist.to_excel(writer, index=False, sheet_name=SHEET_DINAMICA)
        df_hist.to_excel(writer, index=False, sheet_name=SHEET_DETALHADO)

    return out_path, len(df_hist), len(df_dyn_hist)

# FORM para evitar reprocessamento durante upload (anti "piscar")
with st.form("import_2025_form", clear_on_submit=False):
    uploaded_2025 = st.file_uploader(
        "Envie o Excel 2025 (base classificada com 'CLASSIFICAÇÃO RF')",
        type=["xlsx"],
        key="uploader_2025",
    )

    sheet = None
    tmp_path = None

    if uploaded_2025 is not None:
        ensure_dirs()

        file_bytes = uploaded_2025.getvalue()
        file_hash = hashlib.sha256(file_bytes).hexdigest()[:16]

        tmp_path = PROCESSED_DIR / f"import_2025_{file_hash}.xlsx"

        # grava apenas se ainda não existe
        if not tmp_path.exists():
            tmp_path.write_bytes(file_bytes)

        # lê sheetnames com tratamento de erro
        try:
            xls = pd.ExcelFile(tmp_path, engine="openpyxl")
            sheet = st.selectbox(
                "Selecione a aba do arquivo 2025",
                xls.sheet_names,
                index=0,
                key="sheet_2025",
            )
        except Exception as e:
            st.error("Não consegui abrir o Excel 2025. Verifique se é um .xlsx válido e sem senha.")
            st.exception(e)

    submitted = st.form_submit_button("✅ Gerar export histórico 2025")

if submitted:
    if uploaded_2025 is None or tmp_path is None or sheet is None:
        st.warning("Envie o arquivo 2025 e selecione a aba antes de gerar o export histórico.")
    else:
        progress = st.progress(0, text="0% — Iniciando importação 2025...")
        log = st.empty()
        try:
            progress.progress(10, text="10% — Preparando diretórios")
            ensure_dirs()

            out = EXPORTS_DIR / HIST_EXPORT_2025_NAME
            log.info(f"Destino: {out.resolve()}")

            progress.progress(40, text="40% — Lendo e transformando dados 2025")
            out_path, n_det, n_dyn = build_hist_export_from_base(str(tmp_path), sheet, out)

            progress.progress(85, text="85% — Validando arquivo gerado")
            if not out_path.exists():
                raise RuntimeError(f"Arquivo não foi criado: {out_path.resolve()}")

            size_mb = out_path.stat().st_size / (1024 * 1024)
            progress.progress(100, text="100% — Concluído ✅")
            log.success(
                f"✅ Gerado: {out_path.name}\n"
                f"{size_mb:.2f} MB\n"
                f"detalhado: {n_det}\n"
                f"dinâmica: {n_dyn}"
            )

            st.cache_data.clear()
            st.rerun()

        except Exception as e:
            progress.progress(100, text="100% — Falhou ❌")
            st.error("Falha ao gerar o export histórico 2025. Veja detalhes abaixo.")
            st.exception(e)


# ============================================================
# 6) Gráficos simples (MVP)
# ============================================================
st.divider()
st.subheader("📈 Visões rápidas (MVP)")

if "COMPETENCIA_MES" in df_f.columns:
    g_rec = rec_quitado.groupby("COMPETENCIA_MES")["VALOR_EFETIVO"].sum().reset_index()
    g_pag = pag_quitado.groupby("COMPETENCIA_MES")["VALOR_EFETIVO"].sum().reset_index()
    g_tr = df_transf.groupby("COMPETENCIA_MES")["VALOR_EFETIVO"].sum().reset_index()

    colA, colB = st.columns(2)

    with colA:
        st.write("Recebido (Quitado) por mês")
        if not g_rec.empty:
            st.line_chart(g_rec.set_index("COMPETENCIA_MES")["VALOR_EFETIVO"])
        else:
            st.info("Sem dados de recebidos quitados no filtro atual.")

    with colB:
        st.write("Pago (Quitado) por mês")
        if not g_pag.empty:
            st.line_chart(g_pag.set_index("COMPETENCIA_MES")["VALOR_EFETIVO"])
        else:
            st.info("Sem dados de pagos quitados no filtro atual.")

    st.write("Transferências por mês")
    if not g_tr.empty:
        st.bar_chart(g_tr.set_index("COMPETENCIA_MES")["VALOR_EFETIVO"])
    else:
        st.info("Sem transferências no filtro atual.")

st.write("Top 10 clientes (recebidos quitados)")
if not rec_quitado.empty and "NOME_PESSOA" in rec_quitado.columns:
    top_cli = (
        rec_quitado.groupby("NOME_PESSOA")["VALOR_EFETIVO"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    st.bar_chart(top_cli)
else:
    st.info("Sem recebidos quitados no filtro atual.")

st.write("Top 10 fornecedores (pagos quitados)")
if not pag_quitado.empty and "NOME_PESSOA" in pag_quitado.columns:
    top_for = (
        pag_quitado.groupby("NOME_PESSOA")["VALOR_EFETIVO"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )
    st.bar_chart(top_for)
else:
    st.info("Sem pagos quitados no filtro atual.")


# ============================================================
# 7) Tabela (auditoria)
# ============================================================
st.divider()
st.subheader("🔎 Tabela detalhada (auditoria)")

cols_show = [
    "COMPETENCIA_MES", "TIPO", "STATUS_NORM", "NOME_PESSOA",
    "VALOR_TITULO", "VALOR_SALDO", "VALOR_QUITADO", "VALOR_EFETIVO",
    "DATA_EMISSAO", "DATA_VENCIMENTO", "DATA_PAGAMENTO", "DATA_CREDITO",
    "CLASSIFICACAO_RF", "NOME_CENTRO_CUSTO", "NOME_PORTADOR", "FORMA_PAGAMENTO",
    "CNPJ_EMPRESA"
]
cols_show = [c for c in cols_show if c in df_f.columns]

if cols_show:
    st.dataframe(
        df_f[cols_show].sort_values(["COMPETENCIA_MES"], ascending=False),
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("Não há colunas suficientes para exibir a tabela no filtro atual.")


# ============================================================
# 8) Arquivos encontrados (diagnóstico)
# ============================================================
with st.expander("📁 Arquivos encontrados em data/exports (diagnóstico)"):
    ensure_dirs()
    files = sorted(EXPORTS_DIR.glob(EXPORT_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        st.warning("Nenhum export_rf_*.xlsx encontrado em data/exports.")
    else:
        rows = []
        for f in files:
            rows.append({
                "arquivo": f.name,
                "modificado_em": dt.datetime.fromtimestamp(f.stat().st_mtime),
                "tamanho_mb": round(f.stat().st_size / (1024 * 1024), 2),
                "caminho": str(f.resolve()),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ============================================================
# 9) Diagnóstico opcional
# ============================================================
with st.expander("🧪 Diagnóstico (opcional)"):
    st.write("Último export detectado:", latest.name)
    st.write("Arquivos carregados:", [p.name for p in files_to_load])
    st.write("Linhas detalhado (concat):", len(df_det))
    st.write("Linhas após filtros:", len(df_f))

    # Diagnóstico PM: quantos pares válidos existem?
    if "DATA_EMISSAO" in rec_quitado.columns and ("DATA_CREDITO" in rec_quitado.columns or "DATA_PAGAMENTO" in rec_quitado.columns):
        n_pairs_cred = 0
        n_pairs_pag = 0
        try:
            if "DATA_CREDITO" in rec_quitado.columns:
                n_pairs_cred = len(rec_quitado[["DATA_EMISSAO", "DATA_CREDITO"]].dropna())
            if "DATA_PAGAMENTO" in rec_quitado.columns:
                n_pairs_pag = len(rec_quitado[["DATA_EMISSAO", "DATA_PAGAMENTO"]].dropna())
        except Exception:
            pass
        st.write("Pares válidos (EMISSAO->CREDITO):", n_pairs_cred)
        st.write("Pares válidos (EMISSAO->PAGAMENTO):", n_pairs_pag)