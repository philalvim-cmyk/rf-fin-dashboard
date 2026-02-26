# app/db/repositories/history_repo.py
import sqlite3
from pathlib import Path

DB_PATH = Path("data") / "rf_finance.sqlite"

def get_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_tables():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS history_map (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            key_type TEXT NOT NULL,
            nome_conta TEXT NOT NULL,
            nome_pessoa TEXT,
            nome_centro_custo TEXT,
            classificacao_rf TEXT NOT NULL,
            hit_count INTEGER NOT NULL DEFAULT 1,
            last_used_at TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_history_key
        ON history_map (key_type, nome_conta, nome_pessoa, nome_centro_custo)
        """)
        conn.commit()
    finally:
        conn.close()

def bulk_upsert_history(records: list[dict]) -> int:
    if not records:
        return 0

    ensure_tables()
    conn = get_conn()
    affected = 0
    try:
        cur = conn.cursor()
        for r in records:
            key_type = r["key_type"]
            nome_conta = r["nome_conta"]
            nome_pessoa = r.get("nome_pessoa") or ""
            nome_cc = r.get("nome_centro_custo") or ""
            classificacao = r["classificacao_rf"]
            hit_count = int(r.get("hit_count", 1))
            last_used_at = r["last_used_at"]

            existing = cur.execute(
                """
                SELECT id, hit_count
                FROM history_map
                WHERE key_type = ?
                  AND nome_conta = ?
                  AND COALESCE(nome_pessoa,'') = COALESCE(?, '')
                  AND COALESCE(nome_centro_custo,'') = COALESCE(?, '')
                  AND classificacao_rf = ?
                LIMIT 1
                """,
                (key_type, nome_conta, nome_pessoa, nome_cc, classificacao),
            ).fetchone()

            if existing:
                new_count = int(existing["hit_count"]) + hit_count
                cur.execute(
                    "UPDATE history_map SET hit_count = ?, last_used_at = ? WHERE id = ?",
                    (new_count, last_used_at, existing["id"]),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO history_map
                    (key_type, nome_conta, nome_pessoa, nome_centro_custo, classificacao_rf, hit_count, last_used_at)
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (key_type, nome_conta, nome_pessoa, nome_cc, classificacao, hit_count, last_used_at),
                )
            affected += 1

        conn.commit()
        return affected
    finally:
        conn.close()

def get_best_match(nome_conta: str, nome_pessoa: str, nome_cc: str):
    """
    Busca melhor match na ordem:
    1) CONTA+PESSOA+CC
    2) CONTA+CC
    3) CONTA+PESSOA
    4) CONTA
    """
    ensure_tables()
    conn = get_conn()
    try:
        cur = conn.cursor()
        queries = [
            ("CONTA+PESSOA+CC", (nome_conta, nome_pessoa, nome_cc)),
            ("CONTA+CC",       (nome_conta, "",         nome_cc)),
            ("CONTA+PESSOA",   (nome_conta, nome_pessoa, "")),
            ("CONTA",          (nome_conta, "",         "")),
        ]

        for key_type, (c, p, cc) in queries:
            row = cur.execute(
                """
                SELECT classificacao_rf, hit_count
                FROM history_map
                WHERE key_type = ?
                  AND nome_conta = ?
                  AND COALESCE(nome_pessoa,'') = COALESCE(?, '')
                  AND COALESCE(nome_centro_custo,'') = COALESCE(?, '')
                ORDER BY hit_count DESC, id DESC
                LIMIT 1
                """,
                (key_type, c, p, cc),
            ).fetchone()

            if row:
                return row["classificacao_rf"], key_type

        return None, None
    finally:
        conn.close()