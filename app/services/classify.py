# app/services/classify.py
import pandas as pd
from app.db.repositories.history_repo import get_best_match

def apply_classification(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    cls = []
    origem = []
    flags = []

    for _, r in df.iterrows():
        best, key_type = get_best_match(
            r.get("NOME_CONTA_N", ""),
            r.get("NOME_PESSOA_N", ""),
            r.get("NOME_CC_N", "")
        )
        if best:
            cls.append(best)
            origem.append(key_type)
            flags.append("")
        else:
            cls.append("NAO_CLASSIFICADO")
            origem.append("")
            flags.append("NAO_CLASSIFICADO")

    df["CLASSIFICACAO_RF"] = cls
    df["CLASSIFICACAO_ORIGEM"] = origem
    df["FLAGS"] = flags
    return df