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
# HELPERS (dirs / robustez de arquivos)
# ============================================================
def ensure_dirs() -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """
    Escrita atômica:
    - grava em .tmp
    - replace() para o destino final
    Evita arquivo “meio escrito” em caso de rerun/interrupção.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def is_probably_xlsx(path: Path) -> Tuple[bool, str]:
    """
    Validação rápida para evitar BadZipFile.
    XLSX é um ZIP. Se não for ZIP válido, openpyxl pode explodir.
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


def list_exports_validated() -> Tuple[List[Path], List[Tuple[Path, str]]]:
    """
    Lista exports e filtra inválidos, coletando motivos para diagnóstico.
    """
    ensure_dirs()
    files = sorted(EXPORTS_DIR.glob(EXPORT_GLOB), key=lambda p: p.stat().st_mtime)
    good: List[Path] = []
    bad: List[Tuple[Path, str]] = []
    for f in files:
        ok, reason = is_probably_xlsx(f)
        if ok:
            good.append(f)
        else:
            bad.append((f, reason))
    return good, bad


def find_latest_export(valid_files: List[Path]) -> Optional[Path]:
    if not valid_files:
        return None
    return sorted(valid_files, key=lambda p: p.stat().st_mtime, reverse=True)[0]


# ============================================================
# HELPERS (datas / status / valores)
# ============================================================
def safe_parse_timestamp(ts) -> pd.Timestamp:
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
    Retorna Timestamp válido OU NaT.
    """
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
# CACHE / LOADERS (robustos a arquivos ruins)
# ============================================================
@st.cache_data(ttl=600)
def load_export(path: str, mtime: float):
    """
    Lê as 2 abas padrão. Cacheado por (path + mtime).
    Retorna (df_dyn, df_det, err).
    """
    try:
        df_dyn = pd.read_excel(path, sheet_name=SHEET_DINAMICA, engine="openpyxl")
        df_det = pd.read_excel(path, sheet_name=SHEET_DETALHADO, engine="openpyxl")
        return df_dyn, df_det, None
    except Exception as e:
        return None, None, str(e)


@st.cache_data(ttl=600)
def read_competencias_from_export(path: str, mtime: float) -> Tuple[Set[str], Optional[str]]:
    """
    Lê apenas DINAMICA_CONSOLIDADO e retorna set de COMPETENCIA_MES.
    Se falhar, retorna set() e erro.
    """
    try:
        df_dyn = pd.read_excel(path, sheet_name=SHEET_DINAMICA, engine="openpyxl")
        if "COMPETENCIA_MES" not in df_dyn.columns:
            return set(), None
        comps = df_dyn["COMPETENCIA_MES"].dropna().astype(str).str.strip().tolist()
        return set([c for c in comps if c and c.lower() != "nan"]), None
    except Exception as e:
        return set(), str(e)


def choose_latest_export_per_competencia(files: List[Path]) -> Tuple[List[Path], List[Tuple[str, str]]]:
    """
    Solução A: para cada COMPETENCIA_MES, escolhe o export mais recente (maior mtime).
    Retorna (chosen_files, errors).
    """
    best: dict = {}
    best_mtime: dict = {}
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
# SIDEBAR
# ============================================================
use_hist = st.sidebar.checkbox("📚 Usar histórico (2025+2026...)", value=True)
use_solution_a = st.sidebar.checkbox("✅ Anti-duplicação por competência", value=True)


# ============================================================
# 0) EXPORTS
# ============================================================
valid_files, invalid_files = list_exports_validated()
latest = find_latest_export(valid_files)

if latest is None:
    st.error("Não encontrei nenhum export válido em data/exports/export_rf_*.xlsx")
    if invalid_files:
        with st.expander("📁 Exports ignorados (inválidos/corrompidos)"):
            for f, reason in invalid_files:
                st.write(f"- {f.name}: {reason}")
    st.info("Gere um export em 'Aplicar Histórico' e volte aqui.")
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
    st.warning("Não foi possível carregar DETALHADO_CLASSIFICADO dos exports válidos.")
    with st.expander("🧪 Diagnóstico de leitura"):
        if invalid_files:
            st.write("Arquivos inválidos/corrompidos (pré-filtro):")
            for f, reason in invalid_files[:30]:
                st.write(f"- {f.name}: {reason}")
        if comp_errors:
            st.write("Falhas ao ler DINAMICA (anti-duplicação):")
            for name, err in comp_errors[:30]:
                st.write(f"- {name}: {err}")
        if load_errors:
            st.write("Falhas ao ler abas padrão (load_export):")
            for name, err in load_errors[:30]:
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

df["IS_TRANSFERENCIA"] = df.get("CLASSIFICACAO_RF", "").astype(str).str.upper().eq(TRANSFER_LABEL)


# ============================================================
# 2) Sidebar filtros
# ============================================================
with st.sidebar:
    st.header("Filtros do Dashboard")

    comp_list = []
    if "COMPETENCIA_MES" in df.columns:
        comp_list = sorted([c for c in df["COMPETENCIA_MES"].dropna().unique() if c and str(c).lower() != "nan"])

    anos = sorted({str(c)[:4] for c in comp_list if len(str(c)) >= 7})
    meses = [f"{m:02d}" for m in range(1, 13)]

    ano_sel = st.multiselect("Ano", anos, default=anos)
    mes_sel = st.multiselect("Mês", meses, default=[])

    tipo_sel = st.multiselect("Tipo (PAGAR/RECEBER)",
                              sorted(df.get("TIPO", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
                              default=[])
    status_sel = st.multiselect("Situação",
                                sorted(df.get("STATUS_NORM", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
                                default=[])
    cnpj_sel = st.multiselect("CNPJ Empresa",
                              sorted(df.get("CNPJ_EMPRESA", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
                              default=[])
    cc_sel = st.multiselect("Centro de custo",
                            sorted(df.get("NOME_CENTRO_CUSTO", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
                            default=[])
    rub_sel = st.multiselect("Rubrica (CLASSIFICAÇÃO RF)",
                             sorted(df.get("CLASSIFICACAO_RF", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
                             default=[])
    pessoa_sel = st.multiselect("Pessoa (cliente/fornecedor)",
                                sorted(df.get("NOME_PESSOA", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
                                default=[])
    banco_sel = st.multiselect("Banco/Portador",
                               sorted(df.get("NOME_PORTADOR", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
                               default=[])
    forma_sel = st.multiselect("Forma de pagamento",
                               sorted(df.get("FORMA_PAGAMENTO", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()),
                               default=[])

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
# 6) GRÁFICOS (MVP) — CORRIGIDO: sem “ternário solto” (evita magic write)
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
    top_cli = rec_quitado.groupby("NOME_PESSOA")["VALOR_EFETIVO"].sum().sort_values(ascending=False).head(10)
    st.bar_chart(top_cli)
else:
    st.info("Sem recebidos quitados no filtro atual.")

st.write("Top 10 fornecedores (pagos quitados)")
if not pag_quitado.empty and "NOME_PESSOA" in pag_quitado.columns:
    top_for = pag_quitado.groupby("NOME_PESSOA")["VALOR_EFETIVO"].sum().sort_values(ascending=False).head(10)
    st.bar_chart(top_for)
else:
    st.info("Sem pagos quitados no filtro atual.")


# ============================================================
# 7) TABELA (auditoria)
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
    st.dataframe(df_f[cols_show].sort_values(["COMPETENCIA_MES"], ascending=False),
                 use_container_width=True, hide_index=True)
else:
    st.info("Não há colunas suficientes para exibir a tabela no filtro atual.")


# ============================================================
# 8) DIAGNÓSTICO
# ============================================================
with st.expander("📁 Diagnóstico de exports (arquivos e integridade)"):
    st.write("Arquivos válidos encontrados:", len(valid_files))
    st.write("Arquivos inválidos/ignorados:", len(invalid_files))

    if invalid_files:
        st.write("Inválidos (pré-filtro):")
        for f, reason in invalid_files[:50]:
            st.write(f"- {f.name}: {reason}")

    if comp_errors:
        st.write("Falhas ao ler DINAMICA durante anti-duplicação:")
        for name, err in comp_errors[:50]:
            st.write(f"- {name}: {err}")

    if load_errors:
        st.write("Falhas ao ler abas padrão durante load_export:")
        for name, err in load_errors[:50]:
            st.write(f"- {name}: {err}")

    st.write("Arquivos efetivamente carregados:", [p.name for p in files_to_load])

with st.expander("🧪 Diagnóstico (opcional)"):
    st.write("Último export válido:", latest.name)
    st.write("Linhas detalhado (concat):", len(df_det))
    st.write("Linhas após filtros:", len(df_f))