# app/utils/dates.py
import datetime as dt


def to_datetime(value):
    """
    Converte valor vindo do Excel em datetime.
    Suporta:
      - datetime/date (já convertidos)
      - strings em mm/dd/yyyy, dd/mm/yyyy, ISO e com hora
      - heurística: se uma das partes for > 12, define o padrão corretamente
    """
    if value is None:
        return None

    if isinstance(value, dt.datetime):
        return value

    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day)

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None

        # 1) tenta ISO direto
        try:
            return dt.datetime.fromisoformat(s)
        except ValueError:
            pass

        # 2) heurística para datas com "/"
        if "/" in s:
            parts = s.split(" ")[0].split("/")  # ignora hora se existir
            if len(parts) == 3 and all(p.isdigit() for p in parts):
                a, b, c = map(int, parts)

                # Se b > 12 -> é mm/dd (ex: 01/22/2026)
                if b > 12 and 1 <= a <= 12:
                    return dt.datetime(c, a, b)

                # Se a > 12 -> é dd/mm
                if a > 12 and 1 <= b <= 12:
                    return dt.datetime(c, b, a)

        # 3) fallback por formatos comuns (com e sem hora)
        for fmt in (
            "%m/%d/%Y",
            "%m/%d/%Y %H:%M:%S",
            "%d/%m/%Y",
            "%d/%m/%Y %H:%M:%S",
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                return dt.datetime.strptime(s, fmt)
            except ValueError:
                continue

    return None


def competencia_mes(dt_value):
    """Retorna competência no formato YYYY-MM."""
    if dt_value is None:
        return None
    return f"{dt_value.year:04d}-{dt_value.month:02d}"