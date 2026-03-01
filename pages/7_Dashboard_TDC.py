import streamlit as st
from pathlib import Path
import pandas as pd
import datetime as dt
from numbers import Number
import re
import hashlib
import zipfile
from typing import Tuple, List, Optional, Set
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
# HELPERS (dirs / escrita atômica / validação XLSX)
# ============================================================
def ensure_dirs() -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def is_probably_xlsx(path: Path) -> Tuple[bool, str]:
    """
    XLSX é ZIP. Evita BadZipFile e também evita ler arquivo incompleto.
    """
    try:
        if not path.exists():
            return False, "arquivo não existe"
        if not path.is_file():
            return False, "não é arquivo"
        if path.suffix.lower() != ".xlsx":
            return False, "extensão não é .xlsx"
        if path.name.startswith("~$"):
            return False, "arquivo temporário do Excel (~$)"
        size = path.stat().st_size
        if size < 1024:
            return False, f"arquivo muito pequeno ({size} bytes)"
        if not zipfile.is_zipfile(path):
            return False, "não é ZIP válido (xlsx corrompido/incompleto)"
        return True, "ok"
    except Exception as e:
        return False, f"falha ao validar: {e}"


@st.cache_data(ttl=60)
def list_exports_validated_cached() -> Tuple[List[str], List[Tuple[str, str]]]:
    """
    Cache curto: lista exports e filtra inválidos.
    Retorna paths como string para cache ser estável no Cloud.
    """
    ensure_dirs()
    files = sorted(EXPORTS_DIR.glob(EXPORT_GLOB), key=lambda p: p.stat().st_mtime)
    good: List[str] = []
    bad: List[Tuple[str, str]] = []
    for f in files:
        ok, reason = is_probably_xlsx(f)
        if ok:
            good.append(str(f))
        else:
            bad.append((str(f), reason))
    return good, bad


def find_latest_export(valid_files: List[Path]) -> Optional[Path]:
    if not valid_files:
        return None
    return sorted(valid_files, key=lambda p: p.stat().st_mtime, reverse=True)[0]


# ============================================================
# HELPERS (datas / status / valores)
# ============================================================
def safe_parse_timestamp(ts):
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
    if x is None:
        return pd.NaT

    try:
        if pd.isna(x):
            return pd.NaT
    except Exception:
        pass

    if isinstance(x, (dt.datetime, dt.date)):
        try:
            ts = pd.to_datetime(x, errors="coerce")
            return safe_parse_timestamp(ts)
        except (OutOfBoundsDatetime, OverflowError, ValueError):
            return pd.NaT

    if isinstance(x, str):
        s = x.strip()
        if not s:
            return pd.NaT

        if re.fullmatch(r"\d+(\.\d+)?", s):
            try:
                num = float(s)
                if num > 30000:
                    ts = pd.to_datetime("1899-12-30") + pd.to_timedelta(num, unit="D")
                    return safe_parse_timestamp(ts)
            except (OutOfBoundsDatetime, OverflowError, ValueError):
                return pd.NaT

        try:
            ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
            return safe_parse_timestamp(ts)
        except (OutOfBoundsDatetime, OverflowError, ValueError):
            return pd.NaT

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

    try:
        ts = pd.to_datetime(x, errors="coerce", dayfirst=True)
        return safe_parse_timestamp(ts)
    except (OutOfBoundsDatetime, OverflowError, ValueError):
        return pd.NaT


def normalize_status(s: str) -> str:
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


def effective_value(row) -> float:
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
# LOADERS (cache)
# ============================================================
@st.cache_data(ttl=600)
def load_export(path: str, mtime: float):
    try:
        df_dyn = pd.read_excel(path, sheet_name=SHEET_DINAMICA, engine="openpyxl")
        df_det = pd.read_excel(path, sheet_name=SHEET_DETALHADO, engine="openpyxl")
        return df_dyn, df_det, None
    except Exception as e:
        return None, None, str(e)


@st.cache_data(ttl=600)
def read_competencias_from_export(path: str, mtime: float) -> Tuple[Set[str], Optional[str]]:
    try:
        df_dyn = pd.read_excel(path, sheet_name=SHEET_DINAMICA, engine="openpyxl")
        if "COMPETENCIA_MES" not in df_dyn.columns:
            return set(), None
        comps = df_dyn["COMPETENCIA_MES"].dropna().astype(str).str.strip().tolist()
        return set([c for c in comps if c and c.lower() != "nan"]), None
    except Exception as e:
        return set(), str(e)


def choose_latest_export_per_competencia(files: List[Path]) -> Tuple[List[Path], List[Tuple[str, str]]]:
    best = {}
    best_mtime = {}
    errors: List[Tuple[str, str]] = []

    for f in files:
        mtime = f.stat().st_mtime
        comps, err = read_competencias_from_export(str(f), mtime)
        if err:
            errors.append((f.name, err))
            continue
        for comp in comps:
            if (comp not in best_mtime) or (mtime > best_mtime[comp]):
                best[comp] = f
                best_mtime[comp] = mtime

    chosen_files: List[Path] = []
    seen: Set[Path] = set()
    for comp in sorted(best.keys()):
        f = best[comp]
        if f not in seen:
            chosen_files.append(f)
            seen.add(f)

    return chosen_files, errors


# ============================================================
# SIDEBAR (mantido)
# ============================================================
use_hist = st.sidebar.checkbox("📚 Usar histórico (2025+2026...)", value=True)
use_solution_a = st.sidebar.checkbox("✅ Anti-duplicação por competência", value=True)


# ============================================================
# 5) IMPORTADOR 2025 (mantido e colocado ANTES do carregamento pesado)
#   -> Isso reduz chance do upload falhar (Axios 400) por carga.
# ============================================================
st.divider()
st.subheader("📥 Importar base 2025 consolidada (gera export histórico)")
st.write(
    "Cria `export_rf_hist_2025.xlsx` em `data/exports/` com as duas abas padrão. "
    "Escrita atômica evita arquivo incompleto."
)

def detect_class_col(cols) -> Optional[str]:
    candidates = [
        "CLASSIFICAÇÃO RF", "CLASSIFICACAO RF",
        "CLASSIFICACAO_RF", "CLASSIFICACAO", "CLASSIFICAÇÃO",
    ]
    for c in candidates:
        if c in cols:
            return c
    return None

def detect_compet_col(cols) -> Optional[str]:
    candidates = ["MÊS / ANO", "MES / ANO", "MÊS/ANO", "MES/ANO", "MES_ANO", "MÊS_ANO"]
    for c in candidates:
        if c in cols:
            return c
    return None

def normalize_competencia(v) -> Optional[str]:
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

def build_hist_export_from_base_atomic(base_path: str, sheet_name: str, out_path: Path) -> Tuple[Path, int, int]:
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
    tmp_out = out_path.with_suffix(out_path.suffix + ".tmp")

    with pd.ExcelWriter(tmp_out, engine="openpyxl") as writer:
        df_dyn_hist.to_excel(writer, index=False, sheet_name=SHEET_DINAMICA)
        df_hist.to_excel(writer, index=False, sheet_name=SHEET_DETALHADO)

    tmp_out.replace(out_path)
    return out_path, len(df_hist), len(df_dyn_hist)


with st.form("import_2025_form", clear_on_submit=False):
    uploaded_2025 = st.file_uploader(
        "Envie o Excel 2025 (base classificada com 'CLASSIFICAÇÃO RF')",
        type=["xlsx"],
        key="uploader_2025",
        accept_multiple_files=False,
    )

    sheet_2025 = None
    tmp_path_2025 = None

    if uploaded_2025 is not None:
        ensure_dirs()
        file_bytes = uploaded_2025.getvalue()
        file_hash = hashlib.sha256(file_bytes).hexdigest()[:16]
        tmp_path_2025 = PROCESSED_DIR / f"import_2025_{file_hash}.xlsx"

        if not tmp_path_2025.exists():
            atomic_write_bytes(tmp_path_2025, file_bytes)

        try:
            xls = pd.ExcelFile(tmp_path_2025, engine="openpyxl")
            sheet_2025 = st.selectbox("Selecione a aba do arquivo 2025", xls.sheet_names, index=0, key="sheet_2025")
        except Exception as e:
            st.error("Não consegui abrir o Excel 2025. Verifique se é um .xlsx válido e sem senha.")
            st.exception(e)

    submitted_2025 = st.form_submit_button("✅ Gerar export histórico 2025")

if submitted_2025:
    if uploaded_2025 is None or tmp_path_2025 is None or sheet_2025 is None:
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
            out_path, n_det, n_dyn = build_hist_export_from_base_atomic(str(tmp_path_2025), sheet_2025, out)

            progress.progress(85, text="85% — Validando arquivo gerado")
            ok, reason = is_probably_xlsx(out_path)
            if not ok:
                raise RuntimeError(f"Arquivo gerado inválido: {out_path.name} ({reason})")

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
# Agora sim: carregar exports (lazy, cache) — reduz impacto no upload
# ============================================================
ensure_dirs()
valid_paths, invalid_pairs = list_exports_validated_cached()
valid_files = [Path(p) for p in valid_paths]
invalid_files = [(Path(p), reason) for p, reason in invalid_pairs]

latest = find_latest_export(valid_files)
if latest is None:
    st.error("Não encontrei nenhum export válido em data/exports/export_rf_*.xlsx")
    st.stop()

st.caption(f"📁 Último export detectado (válido): **{latest.name}** • {dt.datetime.fromtimestamp(latest.stat().st_mtime)}")

col_btn, col_info = st.columns([1, 3])
with col_btn:
    if st.button("🔄 Atualizar dashboard (reler exports)"):
        st.cache_data.clear()
        st.rerun()
with col_info:
    st.write("Recarrega exports conforme histórico/anti-duplicação.")

if use_hist:
    if use_solution_a:
        files_to_load, comp_errors = choose_latest_export_per_competencia(valid_files)
    else:
        files_to_load = valid_files
        comp_errors = []
else:
    files_to_load = [latest]
    comp_errors = []

dyn_list, det_list = [], []
load_errors: List[Tuple[str, str]] = []

for f in files_to_load:
    d_dyn, d_det, err = load_export(str(f), f.stat().st_mtime)
    if err:
        load_errors.append((f.name, err))
        continue
    dyn_list.append(d_dyn)
    det_list.append(d_det)

df_dyn = pd.concat(dyn_list, ignore_index=True) if dyn_list else pd.DataFrame()
df_det = pd.concat(det_list, ignore_index=True) if det_list else pd.DataFrame()

if df_det.empty:
    st.warning("Não foi possível carregar DETALHADO_CLASSIFICADO.")
    st.stop()


# ============================================================
# Preparação + filtros + KPIs + gráficos + tabela + diagnóstico
# (mantido do seu padrão; sem ternários soltos)
# ============================================================
df = df_det.copy()
for c in ["DATA_EMISSAO", "DATA_VENCIMENTO", "DATA_PAGAMENTO", "DATA_CREDITO"]:
    if c in df.columns:
        df[c] = df[c].apply(excel_date_to_datetime)

df["STATUS_NORM"] = df.get("SITUACAO", "").astype(str).apply(normalize_status)
df["VALOR_EFETIVO"] = df.apply(effective_value, axis=1)
if "COMPETENCIA_MES" in df.columns:
    df["COMPETENCIA_MES"] = df["COMPETENCIA_MES"].astype(str).str.strip()
df["IS_TRANSFERENCIA"] = df.get("CLASSIFICACAO_RF", "").astype(str).str.upper().eq(TRANSFER_LABEL)

with st.sidebar:
    st.header("Filtros do Dashboard")
    comp_list = sorted([c for c in df["COMPETENCIA_MES"].dropna().unique() if c and str(c).lower() != "nan"])
    anos = sorted({str(c)[:4] for c in comp_list if len(str(c)) >= 7})
    meses = [f"{m:02d}" for m in range(1, 13)]
    ano_sel = st.multiselect("Ano", anos, default=anos[-1:] if anos else [])
    mes_sel = st.multiselect("Mês", meses, default=[])
    tipo_sel = st.multiselect("Tipo (PAGAR/RECEBER)", sorted(df["TIPO"].dropna().astype(str).unique().tolist()), default=[])
    status_sel = st.multiselect("Situação", sorted(df["STATUS_NORM"].dropna().astype(str).unique().tolist()), default=[])
    cnpj_sel = st.multiselect("CNPJ Empresa", sorted(df["CNPJ_EMPRESA"].dropna().astype(str).unique().tolist()), default=[])
    cc_sel = st.multiselect("Centro de custo", sorted(df["NOME_CENTRO_CUSTO"].dropna().astype(str).unique().tolist()), default=[])
    rub_sel = st.multiselect("Rubrica (CLASSIFICAÇÃO RF)", sorted(df["CLASSIFICACAO_RF"].dropna().astype(str).unique().tolist()), default=[])
    pessoa_sel = st.multiselect("Pessoa (cliente/fornecedor)", sorted(df["NOME_PESSOA"].dropna().astype(str).unique().tolist()), default=[])
    banco_sel = st.multiselect("Banco/Portador", sorted(df["NOME_PORTADOR"].dropna().astype(str).unique().tolist()), default=[])
    forma_sel = st.multiselect("Forma de pagamento", sorted(df["FORMA_PAGAMENTO"].dropna().astype(str).unique().tolist()), default=[])
    st.divider()
    st.caption("Transferências são KPI próprio.")

df_f = df.copy()
if ano_sel:
    df_f = df_f[df_f["COMPETENCIA_MES"].astype(str).str[:4].isin(ano_sel)]
if mes_sel:
    df_f = df_f[df_f["COMPETENCIA_MES"].astype(str).str[5:7].isin(mes_sel)]
if tipo_sel:
    df_f = df_f[df_f["TIPO"].astype(str).isin(tipo_sel)]
if status_sel:
    df_f = df_f[df_f["STATUS_NORM"].astype(str).isin(status_sel)]
if cnpj_sel:
    df_f = df_f[df_f["CNPJ_EMPRESA"].astype(str).isin(cnpj_sel)]
if cc_sel:
    df_f = df_f[df_f["NOME_CENTRO_CUSTO"].astype(str).isin(cc_sel)]
if rub_sel:
    df_f = df_f[df_f["CLASSIFICACAO_RF"].astype(str).isin(rub_sel)]
if pessoa_sel:
    df_f = df_f[df_f["NOME_PESSOA"].astype(str).isin(pessoa_sel)]
if banco_sel:
    df_f = df_f[df_f["NOME_PORTADOR"].astype(str).isin(banco_sel)]
if forma_sel:
    df_f = df_f[df_f["FORMA_PAGAMENTO"].astype(str).isin(forma_sel)]

df_receber = df_f[df_f["TIPO"].astype(str).str.upper() == "RECEBER"]
df_pagar = df_f[df_f["TIPO"].astype(str).str.upper() == "PAGAR"]
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
pm_pag = avg_days(pag_quitado, "DATA_EMISSAO", "DATA_PAGAMENTO")

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
st.dataframe(df_f[cols_show], use_container_width=True, hide_index=True)

with st.expander("📁 Diagnóstico de exports (arquivos e integridade)"):
    st.write("Arquivos válidos:", len(valid_files))
    st.write("Arquivos inválidos:", len(invalid_files))
    if invalid_files:
        for f, reason in invalid_files[:50]:
            st.write(f"- {f.name}: {reason}")
    if use_hist and use_solution_a and comp_errors:
        st.write("Falhas ao ler DINÂMICA (anti-duplicação):")
        for name, err in comp_errors[:50]:
            st.write(f"- {name}: {err}")
    if load_errors:
        st.write("Falhas ao ler exports:")
        for name, err in load_errors[:50]:
            st.write(f"- {name}: {err}")
    st.write("Arquivos carregados:", [p.name for p in files_to_load])