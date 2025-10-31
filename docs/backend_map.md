# Backend Map

## Routes

- **GET /** – Renders the main search page.
  - Query params: `title`, `country`, `page` (>=1), `per_page` (10–PER_PAGE_MAX).
  - Side effects: logs search event when filters provided.
  - Response: HTML (200) with job cards or demo data when no search results.
- **GET /api/jobs** – JSON API mirror of the index.
  - Query params identical to `/`.
  - Response: `{"items": [...], "meta": {...}}` (200). Links in `BLACKLIST_LINKS` removed.
  - Errors: returns empty list on data issues (logged).
- **POST /subscribe** – Newsletter opt-in (rate limited `5/minute;50/hour`).
  - Accepts JSON (`email`, optional `job_id`) or form data.
  - Success JSON: `{"status": "ok", "redirect": <job link?>}` (200).
  - Duplicate JSON: `{"error": "duplicate"}` (200).
  - Invalid email JSON: `{"error": "invalid_email"}` (400).
  - HTML branch flashes messages and redirects.
  - Side effects: inserts into `subscribers`, logs `subscribe_events`, optionally redirects to job link.
- **POST /events/apply** – Analytics hook for Apply button (rate limited `30/minute;300/hour`).
  - Body: JSON containing `status`, `job_id`, job metadata.
  - Response: `{"status": "ok"}` (200).
  - Side effects: appends an `event_type="apply"` row in `search_events`.
- **GET /api/salary-insights** – Lightweight salary feed.
  - Query params: `title`, `country`.
  - Response: `{"count": int, "items": [...], "meta": {...}}` (200).
- **GET /health** – Readiness probe.
  - Success: `{"status": "ok", "db": "connected"}` (200).
  - Failure: `{"status": "error", "db": "failed"}` (503).
- Built-in error handlers:
  - 404 → `{"error": "not found"}`.
  - 500 → `{"error": "internal error"}` + log entry.

## Models & DB Helpers (`app/models/db.py`)

- `get_db()` – Returns a connection (psycopg or sqlite) stored on `flask.g`.
- `close_db()` – Closes the connection at teardown.
- `init_db()` – Ensures tables/indexes exist depending on backend.
- `Job.count(title, country)` – Counts matching jobs.
- `Job.search(title, country, limit, offset)` – Returns list of jobs with normalized columns.
- `Job.get_link(job_id)` – Fetches job URL (or `None`).
- `Job.insert_many(rows)` – Bulk insert jobs, ignoring duplicates via `link` unique index.
- `insert_subscriber(email)` – Inserts subscriber, returns `"ok"` or `"duplicate"`/`"error"`.
- `insert_subscribe_event(...)` – Records newsletter analytics (best effort).
- `insert_search_event(...)` – Records search or apply analytics.
- Parsing helpers:
  - `parse_salary_query`, `normalize_title`, `normalize_country`.
  - `_coerce_datetime`, `_job_is_new`, `_to_lc` for formatting.

## Environment Variables

- `SECRET_KEY` (required) – App aborts if unset or default placeholder.
- `SUPABASE_URL` / `DATABASE_URL` – Primary Postgres DSN; required unless `FORCE_SQLITE` truthy.
- `FORCE_SQLITE` – Forces bundled SQLite database for dev/test.
- `RATELIMIT_STORAGE_URL` – Backend for `flask_limiter`; warns if `memory://` in production.
- `ENV` / `FLASK_ENV` – Controls production toggles (e.g., template reload, cookie security).
- `FLASK_HOST`, `PORT`, `FLASK_PORT`, `FLASK_DEBUG` – Runtime overrides in `run.py`.
- Optional: `DIRECT_URL` (migrations), `GTM_CONTAINER_ID`, etc. are read indirectly via env file.

## Request & Response Invariants

- Pagination: `page` clamped to `>=1`; `per_page` clamped to `[10, PER_PAGE_MAX]`.
- `parse_salary_query` strips inline salary hints while capturing ranges.
- Apply analytics: `search_events` row stores fallback `"N/A"` for empty title/country and flags `event_type="apply"`.
- `Job.search` excludes links in `BLACKLIST_LINKS`.
- `_job_is_new` marks postings within last 2 days (UTC-aware).
- Subscribe JSON returns `duplicate` with HTTP 200 to avoid leaking subscriber status.

## Known Caveats & Side Effects

- Unique constraint on `Jobs.link`; duplicates skipped silently.
- `search_events` and `subscribe_events` are best-effort; errors are swallowed and logged at DEBUG.
- On startup `init_db()` logs a warning instead of crashing if schema creation fails.
- Production warning emitted when rate limiter uses in-memory storage (`RATELIMIT_STORAGE_URL`).
- Demo jobs render only when DB search returns empty results without filters.
