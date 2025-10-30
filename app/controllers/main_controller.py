# app/controllers/main_controller.py — Route handlers as Flask blueprint

import os
import logging
from flask import Blueprint, render_template, request, redirect, url_for, flash, g, jsonify, current_app
from email_validator import validate_email, EmailNotValidError
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from ..models.db import (
    get_db, log_search, log_search_event, log_job_view_event, normalize_country,
    normalize_title, parse_salary_query, _apply_sid_cookie, _ensure_sid,
    _now_iso, _hash, format_job_date_string, clean_job_description_text,
    insert_subscriber, get_analytics_summary, Job
)

# ------------------------- Blueprint Setup -----------------------------------

main_bp = Blueprint('main', __name__)

# Quick, hardcoded blacklist for known-bad job links
_BLACKLIST_LINKS = {
    "https://example.com/job/1",
}

# Rate limiting
_env = os.getenv("FLASK_ENV") or os.getenv("ENV") or "development"
_rl_storage = os.getenv("RATELIMIT_STORAGE_URL", "memory://")
if _env == "production" and _rl_storage.startswith("memory://"):
    logging.getLogger("catalitium").warning(
        "Rate limiter disabled: memory:// storage in production is not shared; set RATELIMIT_STORAGE_URL to a shared backend (e.g., redis://) to enable limits."
    )
limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=_rl_storage,
    default_limits=["200 per hour"]  # global sane default
)

# ------------------------- Context Processors --------------------------------

@main_bp.context_processor
def inject_globals():
    return {"gtm_container_id": current_app.config.get("GTM_CONTAINER_ID")}

@main_bp.before_request
def ensure_session():
    _ensure_sid()

@main_bp.after_request
def apply_session_cookie(r):
    return _apply_sid_cookie(r)

# ------------------------- Routes --------------------------------------------

@main_bp.get("/")
def index():
    """Catalitium — Main job listings page.
    DB-backed only: lists jobs from SQLite/Postgres via Job model.
    """

    # DB-only: no CSV preloading

    # --- Parse incoming search and pagination parameters
    raw_title = (request.args.get("title") or "").strip()
    raw_country = (request.args.get("country") or "").strip()
    # Safe, bounded pagination params
    page = request.args.get("page", default=1, type=int) or 1
    if page < 1:
        page = 1
    per_page_req = request.args.get("per_page", default=20, type=int) or 20
    # clamp to [10, PER_PAGE_MAX]
    per_page_req = max(10, min(per_page_req, int(current_app.config.get("PER_PAGE_MAX", 100))))

    # --- Normalize queries and extract salary filters
    cleaned_title, sal_floor, sal_ceiling = parse_salary_query(raw_title)
    title_q = normalize_title(cleaned_title)
    country_q = normalize_country(raw_country)

    # DB-only search + pagination
    try:
        q_title = cleaned_title or None
        q_country = country_q or None
        total = Job.count(q_title, q_country)
        per_page = per_page_req
        pages = (total + per_page - 1) // per_page if total else 1
        offset = (max(1, page) - 1) * per_page
        rows = Job.search(q_title, q_country, limit=per_page, offset=offset)
        # Map DB rows to template job model (preserve original human-readable titles)
        items = []
        import re
        from ..models.db import parse_job_description
        for r in rows:
            _t = (r.get("job_title") or "(Untitled)").strip()
            _t = re.sub(r"\s+", " ", _t)
            job_date_raw = r.get("job_date")
            if job_date_raw is None:
                job_date_fmt = ""
            else:
                job_date_str = str(job_date_raw).strip()
                job_date_fmt = format_job_date_string(job_date_str) if job_date_str else ""
            _link = r.get("link")
            if _link in _BLACKLIST_LINKS:
                _link = None
            items.append({
                "id": r.get("id"),
                "title": _t,
                "company": "",  # no company field in DB schema
                "location": r.get("location") or "Remote / Anywhere",
                "description": parse_job_description(r.get("job_description") or ""),
                "date_posted": job_date_fmt,
                "link": _link,
            })
        page_data = {
            "items": items,
            "page": max(1, page),
            "per_page": per_page,
            "total": total,
            "pages": pages,
            "has_prev": page > 1,
            "has_next": page < pages,
        }
    except Exception:
        # If DB path fails, keep items empty but avoid crashing
        page_data = {"items": [], "page": 1, "per_page": per_page_req, "total": 0, "pages": 1, "has_prev": False, "has_next": False}

    # --- Optional: analytics / search logging
    if raw_title or raw_country:
        # Non-blocking analytics
        try:
            log_search(raw_title, raw_country)
        except Exception:
            pass
        try:
            log_search_event(
                raw_title,
                raw_country,
                title_q,
                country_q,
                sal_floor,
                sal_ceiling,
                page_data["total"],
                page_data["page"],
                page_data["per_page"],
            )
        except Exception:
            pass

    # --- Clean up unused keys for rendering
    for j in page_data["items"]:
        j.pop("country_code", None)

    # --- Ensure default cards on first load (no filters, empty results): show demo jobs
    if not raw_title and not raw_country and not page_data["items"]:
        demo_jobs = [
            {
                "id": f"demo-{i}",
                "title": title,
                "company": company,
                "location": location,
                "description": desc,
                "date_posted": date,
                "link": "",
            }
            for i, (title, company, location, desc, date, link) in enumerate([
                ("Senior Software Engineer (AI)", "Catalitium", "Remote / EU", "Own end‑to‑end features across ingestion, ranking, and AI‑assisted matching.", "2025.10.01", ""),
                ("Data Engineer", "Catalitium", "Berlin, DE", "Build reliable pipelines and optimize warehouse performance.", "2025.09.28", ""),
                ("Product Manager", "Stealth", "Zurich, CH", "Partner with design and engineering to deliver user‑value quickly.", "2025.09.27", ""),
                ("Frontend Developer", "Acme Corp", "Barcelona, ES", "Ship delightful UI with Tailwind + Alpine and strong accessibility.", "2025.09.26", ""),
                ("Cloud DevOps Engineer", "Nimbus", "Munich, DE", "Automate infra, observability, and release workflows.", "2025.09.25", ""),
                ("ML Engineer", "Quantix", "Remote", "Deploy LLM‑powered ranking and semantic matching at scale.", "2025.09.24", ""),
            ], start=1)
        ]
        page_data = {
            "items": demo_jobs,
            "page": 1,
            "per_page": len(demo_jobs),
            "total": len(demo_jobs),
            "pages": 1,
            "has_prev": False,
            "has_next": False,
        }

    # --- Pagination URLs
    def _page_url(p):
        return url_for(
            "main.index",
            title=title_q or None,
            country=country_q or None,
            page=p,
            per_page=page_data["per_page"],
        )

    pagination = {
        "page": page_data["page"],
        "pages": page_data["pages"],
        "total": page_data["total"],
        "per_page": page_data["per_page"],
        "has_prev": page_data["has_prev"],
        "has_next": page_data["has_next"],
        "prev_url": _page_url(page_data["page"] - 1) if page_data["has_prev"] else None,
        "next_url": _page_url(page_data["page"] + 1) if page_data["has_next"] else None,
    }

    # --- Render the interactive cards
    return render_template(
        "index.html",
        results=page_data["items"],
        count=page_data["total"],
        title_q=title_q,
        country_q=country_q,
        pagination=pagination,
    )


@main_bp.get("/api/jobs")
def api_jobs():
    """Return jobs as JSON with filters and pagination.
    Filters: title, country; Pagination: page, per_page. Data source: DB only.
    """
    raw_title = (request.args.get("title") or "").strip()
    raw_country = (request.args.get("country") or "").strip()
    page = request.args.get("page", default=1, type=int) or 1
    if page < 1:
        page = 1
    per_page_req = request.args.get("per_page", default=20, type=int) or 20
    per_page = max(10, min(per_page_req, int(current_app.config.get("PER_PAGE_MAX", 100))))
    cleaned_title, _, _ = parse_salary_query(raw_title)
    country_q = normalize_country(raw_country)
    title_q = normalize_title(cleaned_title)
    try:
        total = Job.count(title_q or None, country_q or None)
        pages = (total + per_page - 1) // per_page if per_page else 1
        offset = (max(1, page) - 1) * per_page
        rows = Job.search(title_q or None, country_q or None, limit=per_page, offset=offset)
    except Exception:
        # Fail safe: no 500s for API consumers; return empty payload
        total = 0
        pages = 1
        rows = []


    import re
    def to_lc(s: str) -> str:
        parts = [p for p in re.split(r"[^A-Za-z0-9]+", s or "") if p]
        return parts[0].lower() + ''.join(p.capitalize() for p in parts[1:]) if parts else (s or "")
    items = []
    for r in rows:
        job_date_raw = r.get("job_date")
        job_date_str = str(job_date_raw).strip() if job_date_raw is not None else ""
        _link = r.get("link")
        if _link in _BLACKLIST_LINKS:
            _link = None
        items.append({
            "id": r.get("id"),
            "title": to_lc(r.get("job_title") or ""),
            "description": clean_job_description_text(r.get("job_description") or ""),
            "link": _link,
            "location": r.get("location"),
            "job_date": format_job_date_string(job_date_str) if job_date_str else "",
            "date": r.get("date"),
        })

    resp = {
        "items": items,
        "meta": {
            "page": max(1, page),
            "per_page": per_page,
            "total": total,
            "pages": pages,
            "has_prev": (page > 1),
            "has_next": (page < pages),
        },
    }
    return jsonify(resp)


@main_bp.post("/subscribe")
@limiter.limit("5/minute;50/hour")
def subscribe():
    """Handle email subscription."""
    email = (request.form.get("email") or "").strip()
    job_id_raw = (request.form.get("job_id") or "").strip()

    # RFC-compliant validation
    try:
        email = validate_email(email, check_deliverability=False).normalized
    except EmailNotValidError:
        flash("Please enter a valid email.", "error")
        return redirect(url_for("main.index"))

    job_link = Job.get_link(job_id_raw)
    status = insert_subscriber(email)
    if job_link:
        return redirect(job_link)

    if status == "ok":
        flash("You're subscribed! You're all set.", "success")
    else:
        flash("You're already on the list.", "success")

    return redirect(url_for("main.index"))

@main_bp.post('/events/job_view')
def events_job_view():
    """Handle job view analytics event."""
    data = request.get_json(silent=True) or {}
    log_job_view_event(
        data.get('job_id'),
        data.get('job_title'),
        data.get('company'),
        data.get('location'),
        normalize_country(data.get('location') or ''),
    )
    return {"ok": True}, 200

# ------------------------- JSON Subscribe (API) -------------------------------

@main_bp.post('/subscribe.json')
@limiter.limit("5/minute;50/hour")
def subscribe_json():
    """JSON-friendly subscription endpoint: returns {status|error}."""
    payload = request.get_json(silent=True) or {}
    email = (payload.get('email') or '').strip()
    job_id_raw = (payload.get('job_id') or '').strip()
    try:
        email = validate_email(email, check_deliverability=False).normalized
    except EmailNotValidError:
        return jsonify({"error": "invalid_email"}), 400

    job_link = Job.get_link(job_id_raw)
    status = insert_subscriber(email)
    body = {}
    if status == "ok":
        body["status"] = "ok"
    else:
        body["error"] = "duplicate"
    if job_link:
        body["redirect"] = job_link
    return jsonify(body), 200

# ------------------------- Health + Unified Events ----------------------------

@main_bp.get('/ping')
def ping():
    return jsonify({"status": "ok"}), 200

@main_bp.post('/events/log')
def events_log():
    """Unified logger for frontend analytics.
    Expects JSON: {type: 'job_view'|'search', payload: {...}}
    """
    # Basic size guard (10KB)
    if request.content_length and request.content_length > 10 * 1024:
        return jsonify({"error": "payload_too_large"}), 413

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "invalid_json"}), 400
    etype = (str(data.get('type') or '')).strip().lower()
    p = data.get('payload') or {}
    if not isinstance(p, dict):
        return jsonify({"error": "invalid_payload"}), 400
    # Coercion helpers
    def _s(val, maxlen=200):
        x = (str(val or '')).strip()
        return x[:maxlen]
    def _i(val, default=0, lo=None, hi=None):
        try:
            v = int(val)
        except Exception:
            v = default
        if lo is not None and v < lo: v = lo
        if hi is not None and v > hi: v = hi
        return v
    if etype == 'job_view':
        log_job_view_event(
            _s(p.get('job_id'), 80),
            _s(p.get('job_title'), 180),
            _s(p.get('company'), 120),
            _s(p.get('location'), 120),
            normalize_country(_s(p.get('location'), 120)),
        )
        return jsonify({"ok": True}), 200
    if etype == 'search':
        raw_title = _s(p.get('raw_title'), 180)
        raw_country = _s(p.get('raw_country'), 64)
        norm_title = normalize_title(p.get('norm_title') or raw_title)
        norm_country = normalize_country(p.get('norm_country') or raw_country)
        log_search_event(
            raw_title,
            raw_country,
            norm_title,
            norm_country,
            _i(p.get('sal_floor'), None),
            _i(p.get('sal_ceiling'), None),
            _i(p.get('result_count'), 0, 0, 100000),
            _i(p.get('page'), 1, 1, 100000),
            _i(p.get('per_page'), 10, 1, 1000),
        )
        return jsonify({"ok": True}), 200
    if etype == 'subscribe':
        status = _s(p.get('status') or 'clicked', 32).lower() or 'clicked'
        email = _s(p.get('email'), 254)
        db = get_db()
        try:
            with db.cursor() as cur:
                cur.execute(
                    "INSERT INTO subscribe_events(created_at, email_hash, status) VALUES(%s,%s,%s)",
                    (_now_iso(), _hash(email), status),
                )
        except Exception as e:
            logging.getLogger("catalitium").warning("events_log subscribe write failed: %s", e)
        return jsonify({"ok": True}), 200
    return jsonify({"error": "unsupported_type"}), 400


@main_bp.get('/admin/analytics')
def admin_analytics():
    key = request.args.get('key') or request.args.get('token') or ''
    admin_key = os.getenv('ADMIN_KEY') or os.getenv('ADMIN_TOKEN') or ''
    if not admin_key or key != admin_key:
        return ('forbidden', 403)
    summary = get_analytics_summary()
    return jsonify(summary), 200


@main_bp.get('/admin/metrics')
def admin_metrics():
    """Admin metrics page."""
    token = request.args.get('token')
    if token != os.getenv('ADMIN_TOKEN'):
        return ('forbidden', 403)
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT norm_title, COUNT(*) c FROM search_events WHERE norm_title!='' GROUP BY norm_title ORDER BY c DESC LIMIT 20")
        top_titles = cur.fetchall()
        cur.execute("SELECT norm_country, COUNT(*) c FROM search_events WHERE norm_country!='' GROUP BY norm_country ORDER BY c DESC LIMIT 20")
        top_countries = cur.fetchall()
        cur.execute("SELECT created_at, norm_title, norm_country, result_count FROM search_events ORDER BY created_at DESC LIMIT 50")
        recent = cur.fetchall()

    return render_template('admin_metrics.html', top_titles=top_titles, top_countries=top_countries, recent=recent)

# ------------------------- Premium-ready API ----------------------------------

@main_bp.get("/api/salary-insights")
def api_salary_insights():
    """Lightweight DB-only insights using recent jobs subset."""
    raw_title = (request.args.get("title") or "").strip()
    raw_country = (request.args.get("country") or "").strip()
    title_q = normalize_title(raw_title)
    country_q = normalize_country(raw_country)
    rows = Job.search(title_q or None, country_q or None, limit=100, offset=0)
    import re
    def to_lc(s: str) -> str:
        parts = [p for p in re.split(r"[^A-Za-z0-9]+", s or "") if p]
        return parts[0].lower() + ''.join(p.capitalize() for p in parts[1:]) if parts else (s or "")
    items = [
        {
            "title": to_lc(r.get("job_title") or ""),
            "location": r.get("location"),
            "job_date": format_job_date_string((r.get("job_date") or "").strip()),
            "link": r.get("link"),
        }
        for r in rows
    ]
    return jsonify({
        "count": len(items),
        "items": items,
        "meta": {"title": title_q, "country": country_q}
    })
