from pathlib import Path
from app.models import db
import sqlite3


def main():
    use_sqlite = db._should_use_sqlite()
    print("use_sqlite:", use_sqlite)
    print("sqlite_path:", db._sqlite_path())
    print("supabase_url set:", bool(db.SUPABASE_URL))

    if use_sqlite:
        p = Path(db._sqlite_path())
        print("exists:", p.exists(), "size:", p.stat().st_size if p.exists() else None)
        if p.exists():
            conn = sqlite3.connect(str(p))
            cur = conn.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','index') ORDER BY type, name;")
            rows = cur.fetchall()
            if not rows:
                print("No tables or indexes found in sqlite file.")
            else:
                print("sqlite objects:")
                for r in rows:
                    print("  ", r)
            conn.close()
    else:
        # Try Postgres via the project's helper (may raise if not available)
        try:
            conn = db._pg_connect()
            with conn.cursor() as cur:
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;")
                rows = cur.fetchall()
                if not rows:
                    print("No public tables found in Postgres.")
                else:
                    print("postgres public tables:")
                    for r in rows:
                        print("  ", r)
            conn.close()
        except Exception as e:
            print("Postgres inspect failed:", e)


if __name__ == '__main__':
    main()
