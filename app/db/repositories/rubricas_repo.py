import sqlite3
import datetime as dt
from pathlib import Path

from app.utils.strings import normalize_text

DB_PATH = Path("data") / "rf_finance.sqlite"


def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def _table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    return {r[1] for r in rows}


def ensure_rubricas_schema():
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rubricas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rubrica_display TEXT NOT NULL,
                rubrica_norm TEXT NOT NULL UNIQUE,
                grupo TEXT NOT NULL DEFAULT 'DESPESA',
                natureza TEXT NOT NULL DEFAULT 'OPERACIONAL',
                ativo INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # Migração cirúrgica: adiciona colunas caso a tabela exista sem elas
        cols = _table_columns(conn, "rubricas")

        if "grupo" not in cols:
            conn.execute("ALTER TABLE rubricas ADD COLUMN grupo TEXT NOT NULL DEFAULT 'DESPESA'")
        if "natureza" not in cols:
            conn.execute("ALTER TABLE rubricas ADD COLUMN natureza TEXT NOT NULL DEFAULT 'OPERACIONAL'")

        conn.commit()


def list_rubricas(only_active: bool = True):
    ensure_rubricas_schema()
    with _connect() as conn:
        if only_active:
            rows = conn.execute("""
                SELECT id, rubrica_display, rubrica_norm, grupo, natureza, ativo, created_at, updated_at
                FROM rubricas
                WHERE ativo = 1
                ORDER BY rubrica_display
            """).fetchall()
        else:
            rows = conn.execute("""
                SELECT id, rubrica_display, rubrica_norm, grupo, natureza, ativo, created_at, updated_at
                FROM rubricas
                ORDER BY ativo DESC, rubrica_display
            """).fetchall()

    cols = ["id", "rubrica_display", "rubrica_norm", "grupo", "natureza", "ativo", "created_at", "updated_at"]
    return [dict(zip(cols, r)) for r in rows]


def upsert_rubrica(rubrica_display: str, grupo: str, natureza: str, ativo: int = 1):
    ensure_rubricas_schema()

    rubrica_display = (rubrica_display or "").strip()
    if not rubrica_display:
        raise ValueError("Rubrica vazia.")

    grupo = (grupo or "").strip().upper()
    natureza = (natureza or "").strip().upper()

    if grupo not in {"RECEITA", "DESPESA"}:
        raise ValueError("Grupo inválido. Use RECEITA ou DESPESA.")
    if natureza not in {"OPERACIONAL", "NAO_OPERACIONAL"}:
        raise ValueError("Natureza inválida. Use OPERACIONAL ou NAO_OPERACIONAL.")

    rubrica_norm = normalize_text(rubrica_display)
    now = dt.datetime.now().isoformat(timespec="seconds")

    with _connect() as conn:
        conn.execute("""
            INSERT INTO rubricas (rubrica_display, rubrica_norm, grupo, natureza, ativo, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rubrica_norm) DO UPDATE SET
                rubrica_display=excluded.rubrica_display,
                grupo=excluded.grupo,
                natureza=excluded.natureza,
                ativo=excluded.ativo,
                updated_at=excluded.updated_at
        """, (rubrica_display, rubrica_norm, grupo, natureza, int(ativo), now, now))
        conn.commit()


def set_rubrica_ativo(rubrica_id: int, ativo: int):
    ensure_rubricas_schema()
    now = dt.datetime.now().isoformat(timespec="seconds")
    with _connect() as conn:
        conn.execute("""
            UPDATE rubricas
            SET ativo = ?, updated_at = ?
            WHERE id = ?
        """, (int(ativo), now, int(rubrica_id)))
        conn.commit()


def delete_rubrica(rubrica_id: int):
    ensure_rubricas_schema()
    with _connect() as conn:
        conn.execute("DELETE FROM rubricas WHERE id = ?", (int(rubrica_id),))
        conn.commit()