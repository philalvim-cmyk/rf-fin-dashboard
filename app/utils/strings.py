# app/utils/strings.py
import re
import unicodedata

def normalize_text(value) -> str:
    """Normaliza texto para matching: maiúsculo, sem acentos, sem espaços extras."""
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)

    value = value.strip().upper()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c))
    value = re.sub(r"\s+", " ", value)
    return value