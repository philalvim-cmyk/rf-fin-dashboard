# app/services/transform.py
import pandas as pd
from app.utils.strings import normalize_text

def add_competencia(df: pd.DataFrame, competencia_field: str = "DATA_EMISSAO") -> pd.DataFrame:
    df = df.copy()
    df[competencia_field] = pd.to_datetime(df[competencia_field], errors="coerce")
    df["COMPETENCIA_MES"] = df[competencia_field].dt.to_period("M").astype(str)
    return df

def normalize_keys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["NOME_CONTA_N"] = df["NOME_CONTA"].apply(normalize_text)
    df["NOME_PESSOA_N"] = df["NOME_PESSOA"].apply(normalize_text)
    df["NOME_CC_N"] = df["NOME_CENTRO_CUSTO"].apply(normalize_text)
    return df