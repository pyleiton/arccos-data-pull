"""Run a SQL migration file against the configured Postgres database.

Reads connection settings from .env (same vars as arccos_data_pull_postgres.py).
Strips psql meta-commands (\\echo, etc.) so the SQL runs cleanly through psycopg2.
Prints the verification SELECTs from the end of the file.

Usage:
    python migrations/run_migration.py migrations/001_multitenant.sql
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def split_migration_and_verify(sql: str) -> tuple[str, str]:
    """Split the file at the final COMMIT; — everything before is the migration,
    everything after is verification queries."""
    last_commit = sql.rfind("COMMIT;")
    if last_commit == -1:
        return sql, ""
    end = last_commit + len("COMMIT;")
    return sql[:end], sql[end:]


def strip_psql_meta(sql: str) -> str:
    """Remove psql-only meta-commands like \\echo. psycopg2 chokes on these."""
    return re.sub(r"^\s*\\\w+.*$", "", sql, flags=re.MULTILINE)


def split_statements(sql: str) -> list[str]:
    """Split on semicolons, but keep DO $$ ... $$ blocks intact."""
    statements: list[str] = []
    buf: list[str] = []
    in_dollar = False
    for line in sql.splitlines():
        if "$$" in line:
            in_dollar = not in_dollar if line.count("$$") % 2 == 1 else in_dollar
        buf.append(line)
        if not in_dollar and line.rstrip().endswith(";"):
            stmt = "\n".join(buf).strip()
            if stmt and not stmt.startswith("--"):
                statements.append(stmt)
            buf = []
    tail = "\n".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <migration.sql>", file=sys.stderr)
        return 2

    sql_path = Path(sys.argv[1])
    if not sql_path.exists():
        print(f"Not found: {sql_path}", file=sys.stderr)
        return 2

    load_dotenv()
    conn_args = dict(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD", "postgres"),
        dbname=os.environ.get("PG_DATABASE", "arccos"),
    )
    print(f"Connecting to {conn_args['user']}@{conn_args['host']}:{conn_args['port']}/{conn_args['dbname']}")

    raw = sql_path.read_text(encoding="utf-8")
    migration_sql, verify_sql = split_migration_and_verify(raw)
    migration_sql = strip_psql_meta(migration_sql)
    verify_sql = strip_psql_meta(verify_sql)

    conn = psycopg2.connect(**conn_args)
    try:
        with conn.cursor() as cur:
            print(f"Running {sql_path.name} ...")
            cur.execute(migration_sql)
        conn.commit()
        print("Migration committed.")

        if verify_sql.strip():
            print("\n--- Verification ---")
            with conn.cursor() as cur:
                for stmt in split_statements(verify_sql):
                    if not stmt.strip().upper().startswith("SELECT"):
                        continue
                    cur.execute(stmt)
                    cols = [d[0] for d in cur.description]
                    rows = cur.fetchall()
                    print("\n" + " | ".join(cols))
                    print("-" * (sum(len(c) for c in cols) + 3 * len(cols)))
                    for row in rows:
                        print(" | ".join("" if v is None else str(v) for v in row))
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
