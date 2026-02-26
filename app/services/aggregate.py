# app/services/aggregate.py
import pandas as pd

def consolidate_dinamica(df: pd.DataFrame, value_field: str = "VALOR_TITULO") -> pd.DataFrame:
    dff = df.copy()
    dff[value_field] = pd.to_numeric(dff[value_field], errors="coerce").fillna(0)
    return (
        dff.groupby(["COMPETENCIA_MES", "CLASSIFICACAO_RF"], dropna=False)[value_field]
           .sum()
           .reset_index()
           .rename(columns={value_field: "VALOR"})
           .sort_values(["COMPETENCIA_MES", "CLASSIFICACAO_RF"])
    )