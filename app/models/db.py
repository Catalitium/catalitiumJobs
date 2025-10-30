# app/models/db.py - Database connection and utility functions

import os
import re
import logging
import hashlib
import uuid
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

try:
    import psycopg  # psycopg v3
except Exception:
    psycopg = None  # optional, only required when SUPABASE_URL is set

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ------------------------- Config --------------------------------------------

def _normalize_pg_url(url: str) -> str:
    if not url or not url.startswith(("postgres://", "postgresql://")):
        return url
    parsed = urlparse(url)
    query_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() != "pgbouncer"]
    has_ssl = any(k.lower() == "sslmode" for k, _ in query_pairs)
    if not has_ssl:
        query_pairs.append(("sslmode", "require"))
    new_query = urlencode(query_pairs)
    return urlunparse(parsed._replace(query=new_query))

def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_SQLITE_PATH = str(PROJECT_ROOT / "data" / "catalitium.db")

def _sqlite_path() -> str:
    return os.getenv("DB_PATH") or _DEFAULT_SQLITE_PATH

def _should_use_sqlite() -> bool:
    force = os.getenv("FORCE_SQLITE")
    if force is not None:
        return _truthy(force)
    return not bool(os.getenv("DATABASE_URL") or os.getenv("SUPABASE_URL"))

# Prefer DATABASE_URL for Postgres; fallback to SUPABASE_URL for backwards-compat
_SUPABASE_RAW = (os.getenv("DATABASE_URL") or os.getenv("SUPABASE_URL") or "").strip()
SUPABASE_URL = _normalize_pg_url(_SUPABASE_RAW)
ANALYTICS_SALT = os.getenv('ANALYTICS_SALT','dev')
SECRET_KEY = os.getenv("SECRET_KEY", "").strip()
GTM_ID = os.getenv("GTM_CONTAINER_ID", "GTM-MNJ9SSL9")
PER_PAGE_MAX = 100  # safety cap
RATELIMIT_STORAGE_URL = os.getenv("RATELIMIT_STORAGE_URL", "memory://")

# ------------------------- Logging -------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("catalitium")

# ------------------------- Database Connection Functions ----------------------

def _pg_connect():
    """Connect to PostgreSQL database."""
    import psycopg
    if not SUPABASE_URL:
        raise RuntimeError("SUPABASE_URL not set")
    conn = psycopg.connect(SUPABASE_URL, autocommit=True)
    # Apply safe session settings (best-effort)
    try:
        with conn.cursor() as cur:
            # Keep queries snappy and fail fast; units in ms
            cur.execute("SET statement_timeout TO 800")
            cur.execute("SET idle_in_transaction_session_timeout TO 5000")
            cur.execute("SET application_name TO 'catalitium'")
    except Exception:
        pass
    return conn

class _SQLiteCursor(sqlite3.Cursor):
    """SQLite cursor supporting context manager and %s-style placeholders."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.connection.rollback()
            else:
                self.connection.commit()
        finally:
            try:
                self.close()
            except Exception:
                pass
        # Propagate exceptions
        return False

    def _transform_sql(self, sql: str) -> str:
        return sql.replace("%s", "?")

    def execute(self, sql, parameters=None):
        sql = self._transform_sql(sql)
        if parameters is None:
            return super().execute(sql)
        return super().execute(sql, parameters)

    def executemany(self, sql, seq_of_parameters):
        sql = self._transform_sql(sql)
        return super().executemany(sql, seq_of_parameters)

class _SQLiteConnection(sqlite3.Connection):
    """SQLite connection that produces compatible cursors."""

    def cursor(self, factory=None):
        factory = factory or _SQLiteCursor
        return super().cursor(factory)

def _sqlite_connect():
    """Connect to SQLite database (testing / dev fallback)."""
    path = Path(_sqlite_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,
        factory=_SQLiteConnection,
    )
    conn.row_factory = sqlite3.Row
    return conn

def is_sqlite_connection(conn) -> bool:
    """Return True when the connection object comes from sqlite3."""
    return isinstance(conn, sqlite3.Connection)

def get_db():
    """Get database connection from Flask g object."""
    from flask import g, current_app

    if "db" not in g:
        if _should_use_sqlite():
            try:
                g.db = _sqlite_connect()
            except Exception as e:
                logger.error("SQLite connection failed: %s", e)
                raise
        else:
            try:
                g.db = _pg_connect()
            except Exception as e:
                logger.error("Postgres connection failed: %s", e)
                raise
    return g.db

def close_db(_e=None):
    """Close database connection."""
    from flask import g
    db = g.pop("db", None)
    if db:
        db.close()

# ------------------------- Subscriber & Analytics Helpers --------------------

def insert_subscriber(email: str) -> str:
    """Insert a subscriber and log a subscribe_event.
    Returns 'ok' or 'duplicate'.
    """
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(
                "INSERT INTO subscribers(email, created_at) VALUES(%s, %s)",
                (email, _now_iso()),
            )
        with db.cursor() as cur:
            cur.execute(
                "INSERT INTO subscribe_events(created_at, email_hash, status) VALUES(%s,%s,%s)",
                (_now_iso(), _hash(email), "subscribed"),
            )
        return "ok"
    except Exception:
        try:
            with db.cursor() as cur:
                cur.execute(
                    "INSERT INTO subscribe_events(created_at, email_hash, status) VALUES(%s,%s,%s)",
                    (_now_iso(), _hash(email), "duplicate"),
                )
        except Exception:
            pass
        return "duplicate"


def get_recent_searches(limit: int = 20):
    """Return the last N entries from search_logs (most recent first)."""
    db = get_db()
    limit = max(1, int(limit or 1))
    with db.cursor() as cur:
        cur.execute(
            "SELECT term, country, created_at FROM search_logs ORDER BY created_at DESC LIMIT %s",
            (limit,),
        )
        return cur.fetchall()

def get_analytics_summary():
    """Return basic analytics summary using existing tables.
    - total searches today
    - top 5 most viewed jobs (by job_view_events count)
    - new subscribers today
    - conversion rate (subscribed events / searches today)
    """
    db = get_db()
    # Determine today's ISO date prefix (YYYY-MM-DD)
    today = datetime.now(timezone.utc).date().isoformat()
    summary = {
        "searches_today": 0,
        "top_viewed_jobs": [],
        "new_subscribers_today": 0,
        "conversion_rate": 0.0,
    }
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(1) FROM search_events WHERE DATE(created_at) = %s",
                (today,),
            )
            row = cur.fetchone()
            summary["searches_today"] = int(row[0]) if row else 0
        with db.cursor() as cur:
            cur.execute(
                "SELECT COUNT(1) FROM subscribe_events WHERE status='subscribed' AND DATE(created_at) = %s",
                (today,),
            )
            row = cur.fetchone()
            summary["new_subscribers_today"] = int(row[0]) if row else 0
        with db.cursor() as cur:
            cur.execute(
                """
                SELECT job_title, COUNT(1) AS c
                FROM job_view_events
                WHERE DATE(created_at) = %s
                GROUP BY job_title
                ORDER BY c DESC
                LIMIT 5
                """,
                (today,),
            )
            summary["top_viewed_jobs"] = [(title, count) for title, count in cur.fetchall()]
        searches = summary["searches_today"] or 0
        subs = summary["new_subscribers_today"] or 0
        summary["conversion_rate"] = round((subs / searches) * 100, 2) if searches else 0.0
    except Exception:
        pass
    return summary

# ------------------------- Description Parsing ------------------------------

# Small multilingual stopword set to keep summarizer lightweight
_STOPWORDS = {
    # EN
    "a","about","above","after","again","against","all","am","an","and","any","are","as","at",
    "be","because","been","before","being","below","between","both","but","by","can","could",
    "did","do","does","doing","down","during","each","few","for","from","further","had","has",
    "have","having","he","her","here","hers","herself","him","himself","his","how","i","if","in",
    "into","is","it","its","itself","me","more","most","my","myself","no","nor","not","of","off",
    "on","once","only","or","other","our","ours","ourselves","out","over","own","same","she","should",
    "so","some","such","than","that","the","their","theirs","them","themselves","then","there","these",
    "they","this","those","through","to","too","under","until","up","very","was","we","were","what",
    "when","where","which","while","who","whom","why","with","you","your","yours","yourself","yourselves",
    # ES/FR minimal
    "de","la","el","en","y","los","las","que","es","un","una","con","por","para","le","et","Ã ",
    "les","des","est","pour","dans"
}

def summarize_two_sentences(text: str) -> str:
    """Extract two most representative sentences from text (pure stdlib)."""
    import re
    from collections import Counter
    if not text:
        return ""
    s = text.strip()
    sentences = re.split(r"(?<=[.!?])\s+", s)
    if len(sentences) < 2:
        return s
    words = re.findall(r"\b\w+\b", s.lower())
    freqs = Counter(w for w in words if w not in _STOPWORDS)
    scores = {}
    for sent in sentences:
        tokens = re.findall(r"\b\w+\b", sent.lower())
        if not tokens:
            continue
        score = sum(freqs.get(w, 0) for w in tokens if w not in _STOPWORDS) / max(len(tokens), 1)
        scores[sent] = score
    top = sorted(scores.items(), key=lambda x: (-x[1], sentences.index(x[0])))[:2]
    final = sorted([t[0] for t in top], key=lambda x: sentences.index(x))
    return " ".join(final)

def parse_job_description(text: str) -> str:
    """Clean and summarize a raw job description to a short, readable preview."""
    t = clean_job_description_text(text or "")
    return summarize_two_sentences(t)

def init_db():
    """Ensure required tables exist in the primary Postgres database."""
    db = get_db()
    if is_sqlite_connection(db):
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                email TEXT PRIMARY KEY,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS search_logs (
                term TEXT,
                country TEXT,
                created_at TEXT
            );
            CREATE TABLE IF NOT EXISTS Jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_title TEXT,
                job_description TEXT,
                link TEXT UNIQUE,
                job_title_norm TEXT,
                location TEXT,
                job_date TEXT,
                date TEXT
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_link_unique ON Jobs(link);
            CREATE INDEX IF NOT EXISTS idx_jobs_title_norm ON Jobs(job_title_norm);
            CREATE INDEX IF NOT EXISTS idx_jobs_location ON Jobs(location);
            CREATE TABLE IF NOT EXISTS search_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT,
                raw_title TEXT,
                raw_country TEXT,
                norm_title TEXT,
                norm_country TEXT,
                sal_floor INTEGER,
                sal_ceiling INTEGER,
                result_count INTEGER,
                page INTEGER,
                per_page INTEGER,
                user_agent TEXT,
                referer TEXT,
                ip_hash TEXT,
                session_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_search_events_created ON search_events(created_at);
            CREATE INDEX IF NOT EXISTS idx_search_events_title ON search_events(norm_title);
            CREATE INDEX IF NOT EXISTS idx_search_events_country ON search_events(norm_country);
            CREATE TABLE IF NOT EXISTS job_view_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT,
                job_id TEXT,
                job_title TEXT,
                company TEXT,
                location TEXT,
                norm_country TEXT,
                user_agent TEXT,
                ip_hash TEXT,
                session_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_job_view_created ON job_view_events(created_at);
            CREATE INDEX IF NOT EXISTS idx_job_view_country ON job_view_events(norm_country);
            CREATE TABLE IF NOT EXISTS subscribe_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT,
                email_hash TEXT,
                status TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_subscribe_created ON subscribe_events(created_at);
            """
        )
        db.commit()
        return
    with db.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                email TEXT UNIQUE,
                created_at TIMESTAMP
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS search_logs (
                term TEXT,
                country TEXT,
                created_at TIMESTAMP
            );
            """
        )
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
            CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_link_unique ON Jobs(link);
            CREATE INDEX IF NOT EXISTS idx_jobs_title_norm ON Jobs(job_title_norm);
            CREATE INDEX IF NOT EXISTS idx_jobs_location ON Jobs(location);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS search_events (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP,
                raw_title TEXT,
                raw_country TEXT,
                norm_title TEXT,
                norm_country TEXT,
                sal_floor INTEGER,
                sal_ceiling INTEGER,
                result_count INTEGER,
                page INTEGER,
                per_page INTEGER,
                user_agent TEXT,
                referer TEXT,
                ip_hash TEXT,
                session_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_search_events_created ON search_events(created_at);
            CREATE INDEX IF NOT EXISTS idx_search_events_title ON search_events(norm_title);
            CREATE INDEX IF NOT EXISTS idx_search_events_country ON search_events(norm_country);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_view_events (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP,
                job_id TEXT,
                job_title TEXT,
                company TEXT,
                location TEXT,
                norm_country TEXT,
                user_agent TEXT,
                ip_hash TEXT,
                session_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_job_view_created ON job_view_events(created_at);
            CREATE INDEX IF NOT EXISTS idx_job_view_country ON job_view_events(norm_country);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribe_events (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP,
                email_hash TEXT,
                status TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_subscribe_created ON subscribe_events(created_at);
            """
        )
# ------------------------- Analytics and Session Functions -------------------

def _hash(value: str) -> str:
    """Hash value for analytics."""
    salt = ANALYTICS_SALT
    v = (value or '').encode('utf-8')
    return hashlib.sha256(salt.encode('utf-8') + v).hexdigest()

def _ensure_sid():
    """Ensure session ID exists in request."""
    from flask import request, g
    sid = request.cookies.get('sid')
    if not sid:
        sid = uuid.uuid4().hex
        # store to set on response
        setattr(g, '_sid_new', sid)
    return sid

def _client_meta():
    """Get client metadata for analytics."""
    from flask import request, g
    ua = request.headers.get('User-Agent','')
    ref = request.headers.get('Referer','')
    xff = request.headers.get('X-Forwarded-For', '')
    ip = (xff.split(',')[0].strip() if xff else (request.remote_addr or ''))
    sid = request.cookies.get('sid') or getattr(g, '_sid_new', '') or ''
    return ua, ref, _hash(ip), sid

def _apply_sid_cookie(r):
    """Apply session ID cookie to response."""
    from flask import g, request
    sid = getattr(g, '_sid_new', None)
    if sid:
        # Respect proxies (e.g., X-Forwarded-Proto: https) when deciding on Secure
        xfp = (request.headers.get('X-Forwarded-Proto', '') or '').split(',')[0].strip().lower()
        secure_flag = bool(request.is_secure) or xfp == 'https'
        r.set_cookie('sid', sid, max_age=31536000, httponly=True, samesite='Lax', secure=secure_flag)
    return r

def _now_iso():
    """Get current timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

# ------------------------- Analytics Logging Functions -----------------------

def log_search_event(raw_title, raw_country, norm_title, norm_country, sal_floor, sal_ceiling, result_count, page, per_page):
    """Log search event to database."""
    from flask import request, g
    ua, ref, ip_h, sid = _client_meta()
    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            'INSERT INTO search_events(created_at,raw_title,raw_country,norm_title,norm_country,sal_floor,sal_ceiling,result_count,page,per_page,user_agent,referer,ip_hash,session_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            (_now_iso(), raw_title or '', raw_country or '', norm_title or '', norm_country or '', sal_floor, sal_ceiling, result_count, page, per_page, ua[:300], ref[:300], ip_h, sid)
        )

def log_job_view_event(job_id, job_title, company, location, norm_country):
    """Log job view event to database."""
    from flask import request, g
    ua, ref, ip_h, sid = _client_meta()
    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            'INSERT INTO job_view_events(created_at,job_id,job_title,company,location,norm_country,user_agent,ip_hash,session_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            (_now_iso(), str(job_id or ''), job_title or '', company or '', location or '', norm_country or '', ua[:300], ip_h, sid)
        )

def log_search(term, country):
    """Log basic search to database."""
    if not term and not country:
        return

    db = get_db()

    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO search_logs(term, country, created_at) VALUES(%s, %s, %s)",
            (term or "", country or "", _now_iso()),
        )

# ------------------------- Helper Functions ----------------------------------

def _tokens(text: str):
    """Tokenize text for fuzzy matching."""
    return [t for t in re.split(r"[^\w+]+", text.lower()) if t]

def _fuzzy_match(needle: str, hay: str) -> bool:
    """Very loose containment check for tokens (case insensitive).
    - Matches if any token appears in hay.
    - Empty needle matches everything.
    """
    if not needle:
        return True
    n_tokens = _tokens(needle)
    if not n_tokens:
        return True
    hay_l = hay.lower()
    return any(tok in hay_l for tok in n_tokens)

# ------------------------- Normalization Functions ---------------------------

COUNTRY_NORM = {
    "deutschland":"DE","germany":"DE","deu":"DE","de":"DE",
    "switzerland":"CH","schweiz":"CH","suisse":"CH","svizzera":"CH","ch":"CH",
    "austria":"AT","Ã¶sterreich":"AT","at":"AT",
    "europe":"EU","eu":"EU",
    "uk":"UK","gb":"UK","england":"UK","united kingdom":"UK",
    "usa":"US","united states":"US","america":"US","us":"US",
    "spain":"ES","es":"ES","france":"FR","fr":"FR","italy":"IT","it":"IT",
    "netherlands":"NL","nl":"NL","belgium":"BE","be":"BE","sweden":"SE","se":"SE",
    "poland":"PL","colombia":"CO","mexico":"MX",
}

TITLE_SYNONYMS = {
    "swe":"software engineer","software eng":"software engineer","sw eng":"software engineer",
    "frontend":"front end","front-end":"front end","backend":"back end","back-end":"back end",
    "fullstack":"full stack","full-stack":"full stack",
    "pm":"product manager","prod mgr":"product manager","product owner":"product manager",
    "ds":"data scientist","ml":"machine learning","mle":"machine learning engineer",
    "sre":"site reliability engineer","devops":"devops","sec eng":"security engineer","infosec":"security",
}

def normalize_country(q: str) -> str:
    """Return normalized country code if possible."""
    if not q:
        return ""
    t = q.strip().lower()
    if t in COUNTRY_NORM:
        return COUNTRY_NORM[t]
    if len(t) == 2 and t.isalpha():
        return t.upper()
    for token, code in COUNTRY_NORM.items():
        if token in t:
            return code
    return q.strip()

def extract_country_code(loc: str, country_fallback: str = "") -> str:
    """Try to extract country code from location string or fallback country."""
    if loc:
        parts = re.split(r"[^A-Za-z0-9]+", loc)
        for token in reversed([p for p in parts if p]):
            t = token.lower()
            if t in COUNTRY_NORM:
                return COUNTRY_NORM[t]
            if len(t) == 2 and t.isalpha():
                return t.upper()
    return normalize_country(country_fallback)

def normalize_title(q: str) -> str:
    """Normalize job title query."""
    if not q:
        return ""
    s = q.lower()
    for k, v in TITLE_SYNONYMS.items():
        if k in s:
            s = s.replace(k, v)
    s = re.sub(r"[^\w\s\-\/]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ------------------------- Job Model ----------------------------------------

class Job:
    table = "Jobs"

    @staticmethod
    def _normalize_title(value: Optional[str]) -> str:
        return (value or "").strip().lower()

    @staticmethod
    def _escape_like(value: str) -> str:
        value = value.replace("\\", "\\\\")
        value = value.replace("%", r"\%")
        value = value.replace("_", r"\_")
        return value

    @staticmethod
    def exists() -> bool:
        """Return True when the Jobs table is present."""
        try:
            db = get_db()
            with db.cursor() as cur:
                if is_sqlite_connection(db):
                    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Jobs'")
                    return bool(cur.fetchone())
                cur.execute("SELECT to_regclass('public.Jobs') IS NOT NULL")
                row = cur.fetchone()
                return bool(row and row[0])
        except Exception:
            return False

    @staticmethod
    def count(title: Optional[str] = None, country: Optional[str] = None) -> int:
        """Return number of jobs matching optional filters."""
        where_sql, params_sqlite, params_pg = Job._where(title, country)
        db = get_db()
        with db.cursor() as cur:
            if is_sqlite_connection(db):
                cur.execute(f"SELECT COUNT(1) FROM Jobs {where_sql['sqlite']}", params_sqlite)
            else:
                cur.execute(f"SELECT COUNT(1) FROM Jobs {where_sql['pg']}", params_pg)
            row = cur.fetchone()
            return int(row[0] if row else 0)

    @staticmethod
    def search(
        title: Optional[str] = None,
        country: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict]:
        """Return matching jobs ordered by recency."""
        where_sql, params_sqlite, params_pg = Job._where(title, country)
        db = get_db()
        use_sqlite = is_sqlite_connection(db)
        where_clause = where_sql["sqlite"] if use_sqlite else where_sql["pg"]
        params = list(params_sqlite if use_sqlite else params_pg)
        sql = f"""
            SELECT id, job_title, job_description, link, job_title_norm, location, job_date, date
            FROM Jobs {where_clause}
            {Job._order_by()}
            LIMIT %s OFFSET %s
        """
        params.extend([int(limit), int(offset)])
        with db.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    @staticmethod
    def insert_many(rows: List[Dict]) -> int:
        """Bulk insert jobs, ignoring duplicates by link."""
        if not rows:
            return 0
        cols = ["job_title", "job_description", "link", "job_title_norm", "location", "job_date", "date"]
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO Jobs ({', '.join(cols)}) VALUES ({placeholders}) ON CONFLICT (link) DO NOTHING"

        payload = []
        for row in rows:
            title = row.get("job_title") or row.get("title") or ""
            payload.append(
                (
                    title,
                    row.get("job_description") or row.get("description") or "",
                    row.get("link") or "",
                    Job._normalize_title(row.get("job_title_norm") or title),
                    row.get("location") or row.get("country") or row.get("City") or "",
                    row.get("job_date") or row.get("date_posted") or "",
                    row.get("date") or row.get("created_at") or None,
                )
            )

        db = get_db()
        with db.cursor() as cur:
            cur.executemany(sql, payload)
            return cur.rowcount or 0

    @staticmethod
    def get_link(job_id: Optional[str]) -> Optional[str]:
        """Return the outbound link for a job id if available."""
        if job_id is None:
            return None
        value = str(job_id).strip()
        if not value:
            return None
        try:
            value_param = int(value)
        except (TypeError, ValueError):
            value_param = value

        db = get_db()
        with db.cursor() as cur:
            cur.execute("SELECT link FROM Jobs WHERE id = %s", [value_param])
            row = cur.fetchone()
            if not row:
                return None
            link = None
            if hasattr(row, 'keys'):
                try:
                    link = row.get('link') if hasattr(row, 'get') else row['link']
                except Exception:
                    link = None
            if link is None:
                cols = [desc[0] for desc in cur.description]
                try:
                    row_dict = dict(zip(cols, row))
                    link = row_dict.get('link')
                except Exception:
                    link = None
            if link is None and hasattr(row, 'link'):
                link = getattr(row, 'link')
            if isinstance(link, str):
                return link.strip()
            return None

    @staticmethod
    def _where(title: Optional[str], country: Optional[str]) -> Tuple[Dict[str, str], Tuple[str, ...], Tuple[str, ...]]:
        clauses_pg: List[str] = []
        clauses_sqlite: List[str] = []
        params_pg: List[str] = []
        params_sqlite: List[str] = []

        if title:
            t_norm = Job._normalize_title(title)
            if t_norm:
                like = f"%{Job._escape_like(t_norm)}%"
                clause_pg = "(job_title_norm ILIKE %s ESCAPE '\\' OR LOWER(job_title) LIKE %s ESCAPE '\\' OR LOWER(job_description) LIKE %s ESCAPE '\\')"
                clause_sqlite = "(job_title_norm LIKE ? ESCAPE '\\' OR LOWER(job_title) LIKE ? ESCAPE '\\' OR LOWER(job_description) LIKE ? ESCAPE '\\')"
                clauses_pg.append(clause_pg)
                clauses_sqlite.append(clause_sqlite)
                params_pg.extend([like, like, like])
                params_sqlite.extend([like, like, like])

        if country:
            c_raw = (country or "").strip().lower()
            if c_raw:
                code = c_raw.upper() if len(c_raw) == 2 and c_raw.isalpha() else None
                patterns: List[str] = []
                if code:
                    token = Job._escape_like(code.lower())
                    seps_before = [" ", "(", ",", "/", "-"]
                    seps_after = [" ", ")", ",", "/", "-"]
                    for before in seps_before:
                        for after in seps_after:
                            patterns.append(f"%{before}{token}{after}%")
                        patterns.append(f"%{before}{token}")
                    names = sorted({name for name, mapped in COUNTRY_NORM.items() if mapped.upper() == code})
                    for name in names:
                        patterns.append(f"%{Job._escape_like(name.lower())}%")
                else:
                    patterns.append(f"%{Job._escape_like(c_raw)}%")
                if patterns:
                    clause_pg = "(" + " OR ".join(["LOWER(location) LIKE %s ESCAPE '\\'"] * len(patterns)) + ")"
                    clause_sqlite = "(" + " OR ".join(["LOWER(location) LIKE ? ESCAPE '\\'"] * len(patterns)) + ")"
                    clauses_pg.append(clause_pg)
                    clauses_sqlite.append(clause_sqlite)
                    params_pg.extend(patterns)
                    params_sqlite.extend(patterns)

        where_pg = f"WHERE {' AND '.join(clauses_pg)}" if clauses_pg else ""
        where_sqlite = f"WHERE {' AND '.join(clauses_sqlite)}" if clauses_sqlite else ""
        return {"pg": where_pg, "sqlite": where_sqlite}, tuple(params_sqlite), tuple(params_pg)

    @staticmethod
    def _order_by() -> str:
        return "ORDER BY (date IS NULL) ASC, date DESC, id DESC"

# ------------------------- Salary Parsing Functions --------------------------

def parse_money_numbers(text: str):
    """Parse money numbers from text."""
    if not text:
        return []
    nums = []
    for raw in re.findall(r'(?i)\d[\d,.\s]*k?', text):
        clean = raw.lower().replace(",", "").replace(" ", "")
        mult = 1000 if clean.endswith("k") else 1
        clean = clean.rstrip("k").replace(".", "")
        if clean.isdigit():
            nums.append(int(clean) * mult)
    return nums

def parse_salary_range_from_text(text: str):
    """Parse salary range from text."""
    nums = parse_money_numbers(text)
    if not nums:
        return (None, None)
    return (min(nums), max(nums) if len(nums) > 1 else None)

def parse_salary_query(q: str):
    """Parse inline salary filters like '80k-120k', '>100k', '<=90k', '120k'."""
    if not q:
        return ("", None, None)
    s = q.strip()

    # Range (80k-120k)
    m = re.search(r'(?i)(\d[\d,.\s]*k?)\s*[-â]\s*(\d[\d,.\s]*k?)', s)
    if m:
        low = parse_money_numbers(m.group(1))
        high = parse_money_numbers(m.group(2))
        return (s[:m.start()] + s[m.end():]).strip(), low[0] if low else None, high[-1] if high else None

    # Greater than
    m = re.search(r'(?i)>\s*=?\s*(\d[\d,.\s]*k?)', s)
    if m:
        v = parse_money_numbers(m.group(1))
        return (s[:m.start()] + s[m.end():]).strip(), v[0] if v else None, None

    # Less than
    m = re.search(r'(?i)<\s*=?\s*(\d[\d,.\s]*k?)', s)
    if m:
        v = parse_money_numbers(m.group(1))
        return (s[:m.start()] + s[m.end():]).strip(), None, v[0] if v else None

    # Single number
    m = re.search(r'(?i)(\d[\d,.\s]*k?)', s)
    if m:
        v = parse_money_numbers(m.group(1))
        return (s[:m.start()] + s[m.end():]).strip(), v[0] if v else None, None

    return (s, None, None)

# ------------------------- Formatting Helpers ------------------------------

def format_job_date_string(s: str) -> str:
    """Normalize job date strings for display.
    - If 'YYYYMMDD' -> 'YYYY.MM.DD'
    - If 'YYYY-MM-DD' -> 'YYYY.MM.DD'
    - Otherwise return original trimmed string
    """
    if not s:
        return ""
    s = str(s).strip()
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", s)
    if m:
        y, mo, d = m.groups()
        return f"{y}.{mo}.{d}"
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        y, mo, d = m.groups()
        return f"{y}.{mo}.{d}"
    return s

def clean_job_description_text(text: str) -> str:
    """Clean description by removing leading relative-age prefixes and stray labels."""
    if not text:
        return ""
    t = str(text)
    # Strip any leading non-word characters followed by an 8 digit date (e.g., 20251009)
    t = re.sub(r"^\s*\W*\d{8}\s*\n?", "", t)
    # Strip leading relative age prefixes like "11 hours ago - "
    t = re.sub(r"^\s*\d+\s*(minutes?|hours?|days?|weeks?)\s+ago\s+[^\w\s]\s*", "", t, flags=re.IGNORECASE)
    # Remove a standalone leading 'Details' line
    t = re.sub(r"^\s*Details\s*\n+", "", t, flags=re.IGNORECASE)
    return t.strip()


