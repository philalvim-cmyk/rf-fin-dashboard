# app/services/export.py
import pandas as pd
from pathlib import Path
import datetime as dt

def export_excel(df_consolidado: pd.DataFrame, df_detalhado: pd.DataFrame, out_dir: str = "data/exports") -> Path:
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(out_dir) / f"export_rf_{ts}.xlsx"

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df_consolidado.to_excel(writer, index=False, sheet_name="DINAMICA_CONSOLIDADO")
        df_detalhado.to_excel(writer, index=False, sheet_name="DETALHADO_CLASSIFICADO")

    return out_path