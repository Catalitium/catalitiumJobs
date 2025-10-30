#!/usr/bin/env python3
"""
Seed selected tables from local SQLite (data/catalitium.db) into Postgres (DATABASE_URL).

Usage:
  python scripts/seed_to_postgres.py --tables salary,jobs --truncate

Notes:
- Creates table 'salary' in Postgres if missing (compatible with CSV columns).
- Assumes Postgres 'Jobs' table already created by init_db(); inserts with ON CONFLICT(link) DO NOTHING.
- Requires psycopg (v3) and access to DATABASE_URL (sslmode=require recommended).
"""

import argparse
import os
import sqlite3
from pathlib import Path

try:
    import psycopg
except Exception as e:
    psycopg = None

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "catalitium.db"


def pg_connect(url: str):
    if not psycopg:
        raise RuntimeError("psycopg is required to seed Postgres. Install it and retry.")
    if url.startswith("postgres") and "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = url + sep + "sslmode=require"
    return psycopg.connect(url, autocommit=True)


def ensure_salary_table_pg(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS salary (
            GeoSalaryId TEXT,
            Location TEXT,
            MedianSalary TEXT,
            MinSalary TEXT,
            CurrencyTicker TEXT,
            City TEXT,
            Country TEXT,
            Region TEXT,
            RemoteType TEXT
        );
        """
    )


def seed_salary(sqlite_conn: sqlite3.Connection, pg_conn, truncate: bool = False):
    rows = sqlite_conn.execute(
        "SELECT GeoSalaryId,Location,MedianSalary,MinSalary,CurrencyTicker,City,Country,Region,RemoteType FROM salary"
    ).fetchall()
    with pg_conn.cursor() as cur:
        ensure_salary_table_pg(cur)
        if truncate:
            cur.execute("TRUNCATE TABLE salary")
        before = cur.execute("SELECT COUNT(1) FROM salary").fetchone()[0]
        q = (
            "INSERT INTO salary(GeoSalaryId,Location,MedianSalary,MinSalary,CurrencyTicker,City,Country,Region,RemoteType)"
            " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        for r in rows:
            cur.execute(q, r)
        after = cur.execute("SELECT COUNT(1) FROM salary").fetchone()[0]
    print(f"Seeded salary: +{after - before} rows (from {len(rows)} candidates)")


def seed_jobs(sqlite_conn: sqlite3.Connection, pg_conn):
    # Pull from SQLite Jobs table
    rows = sqlite_conn.execute(
        "SELECT job_title, job_description, link, job_title_norm, location, job_date, date FROM Jobs"
    ).fetchall()
    with pg_conn.cursor() as cur:
        # Ensure Jobs exists (do not enforce unique index here to avoid failing on existing duplicates)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Jobs (
                id SERIAL PRIMARY KEY,
                job_title TEXT NULL,
                job_description TEXT NULL,
                link TEXT NOT NULL,
                job_title_norm TEXT NULL,
                location TEXT,
                job_date TEXT NULL,
                date TIMESTAMP WITH TIME ZONE
            );
            """
        )
        # Insert rows additively, skip if link already exists (without requiring a unique index)
        before = cur.execute("SELECT COUNT(1) FROM Jobs").fetchone()[0]
        # Normalize job_date for potential bigint schemas: keep numeric else NULL
        norm_rows = []
        for r in rows:
            jdate = r[5]
            if isinstance(jdate, str):
                jdate_norm = int(jdate) if jdate.isdigit() else None
            else:
                try:
                    jdate_norm = int(jdate) if jdate is not None else None
                except Exception:
                    jdate_norm = None
            # rebuild row with normalized jdate
            r2 = (r[0], r[1], r[2], r[3], r[4], jdate_norm, r[6])
            norm_rows.append(r2)

        base_q = (
            "INSERT INTO Jobs(job_title, job_description, link, job_title_norm, location, job_date, date) "
            "SELECT %s,%s,%s,%s,%s,%s,%s WHERE NOT EXISTS (SELECT 1 FROM Jobs j WHERE j.link = %s)"
        )
        for i, r2 in enumerate(norm_rows):
            q = base_q + f" /*seed_{i}*/"
            cur.execute(q, tuple(list(r2) + [r2[2]]))
        after = cur.execute("SELECT COUNT(1) FROM Jobs").fetchone()[0]
    print(f"Seeded Jobs: +{after - before} rows (from {len(rows)} candidates)")


def main():
    ap = argparse.ArgumentParser(description="Seed tables from SQLite to Postgres")
    ap.add_argument("--tables", default="salary", help="Comma-separated list: salary,jobs")
    ap.add_argument("--truncate", action="store_true", help="Truncate target tables before insert (where applicable)")
    args = ap.parse_args()

    tables = {t.strip().lower() for t in args.tables.split(",") if t.strip()}
    db_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL (or SUPABASE_URL) is required for Postgres destination")

    if not DB_PATH.exists():
        raise SystemExit(f"SQLite DB not found: {DB_PATH}")

    print(f"Connecting to SQLite: {DB_PATH}")
    sqlite_conn = sqlite3.connect(str(DB_PATH))
    try:
        print("Connecting to Postgres...")
        pg_conn = pg_connect(db_url)
        try:
            if "salary" in tables:
                seed_salary(sqlite_conn, pg_conn, truncate=args.truncate)
            if "jobs" in tables:
                seed_jobs(sqlite_conn, pg_conn)
        finally:
            pg_conn.close()
    finally:
        sqlite_conn.close()


if __name__ == "__main__":
    main()
