import os
import logging
import re
from datetime import datetime, timezone, timedelta

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    jsonify,
    g,
)
from email_validator import validate_email, EmailNotValidError
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

from .models.db import (
    SECRET_KEY,
    SUPABASE_URL,
    PER_PAGE_MAX,
    RATELIMIT_STORAGE_URL,
    logger,
    close_db,
    init_db,
    get_db,
    normalize_country,
    normalize_title,
    parse_salary_query,
    parse_job_description,
    format_job_date_string,
    clean_job_description_text,
    insert_subscriber,
    insert_search_event,
    insert_subscribe_event,
    Job,
)


BLACKLIST_LINKS = {
    "https://example.com/job/1",
}

ENVIRONMENT = os.getenv("FLASK_ENV") or os.getenv("ENV") or "development"

if ENVIRONMENT == "production" and RATELIMIT_STORAGE_URL.startswith("memory://"):
    logging.getLogger("catalitium").warning(
        "Rate limiter disabled: configure RATELIMIT_STORAGE_URL with a shared backend."
    )

limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=RATELIMIT_STORAGE_URL,
    default_limits=["200 per hour"],
)


def create_app():
    app = Flask(__name__, template_folder="views/templates")
    env = ENVIRONMENT or "production"

    if not SUPABASE_URL and os.getenv("FORCE_SQLITE") not in {"1", "true", "on", "yes"}:
        logger.error("SUPABASE_URL (or DATABASE_URL) must be configured before starting the app.")
        raise SystemExit(1)

    app.config.update(
        SECRET_KEY=SECRET_KEY,
        TEMPLATES_AUTO_RELOAD=(env != "production"),
        PER_PAGE_MAX=PER_PAGE_MAX,
        SUPABASE_URL=SUPABASE_URL,
        SESSION_COOKIE_SECURE=True,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
    )

    limiter.init_app(app)
    app.teardown_appcontext(close_db)

    @app.after_request
    def apply_analytics_cookie(response):
        sid_info = getattr(g, "_analytics_sid_new", None)
        if sid_info:
            cookie_name, sid = sid_info
            secure_cookie = env == "production"
            response.set_cookie(
                cookie_name,
                sid,
                max_age=31536000,
                httponly=True,
                samesite="Lax",
                secure=secure_cookie,
            )
        return response

    if not SECRET_KEY or SECRET_KEY == "dev-insecure-change-me":
        logger.error("SECRET_KEY must be set via environment. Aborting.")
        raise SystemExit(1)

    try:
        with app.app_context():
            init_db()
    except Exception as exc:
        logger.warning("init_db failed: %s", exc)

    @app.errorhandler(404)
    def handle_not_found(_error):
        return jsonify({"error": "not found"}), 404

    @app.errorhandler(500)
    def handle_server_error(error):
        logger.exception("Unhandled error", exc_info=error)
        return jsonify({"error": "internal error"}), 500

    @app.get("/")
    def index():
        raw_title = (request.args.get("title") or "").strip()
        raw_country = (request.args.get("country") or "").strip()
        page = request.args.get("page", default=1, type=int) or 1
        if page < 1:
            page = 1
        per_page_req = request.args.get("per_page", default=20, type=int) or 20
        per_page_req = max(10, min(per_page_req, int(app.config.get("PER_PAGE_MAX", 100))))
        per_page = per_page_req

        cleaned_title, sal_floor, sal_ceiling = parse_salary_query(raw_title)
        title_q = normalize_title(cleaned_title)
        country_q = normalize_country(raw_country)

        q_title = title_q or None
        q_country = country_q or None

        try:
            total = Job.count(q_title, q_country)
            pages = (total + per_page - 1) // per_page if total else 1
            offset = (max(1, page) - 1) * per_page
            rows = Job.search(q_title, q_country, limit=per_page, offset=offset)
            if raw_title or raw_country:
                try:
                    insert_search_event(
                        raw_title=raw_title,
                        raw_country=raw_country,
                        norm_title=title_q,
                        norm_country=country_q,
                        sal_floor=sal_floor,
                        sal_ceiling=sal_ceiling,
                        result_count=total,
                        page=max(1, page),
                        per_page=per_page,
                        source="web",
                    )
                except Exception:
                    pass

        except Exception:
            total = 0
            pages = 1
            rows = []

        items = []
        for row in rows:
            title = (row.get("job_title") or "(Untitled)").strip()
            title = re.sub(r"\s+", " ", title)
            job_date_raw = row.get("job_date")
            job_date_str = str(job_date_raw).strip() if job_date_raw is not None else ""
            link = row.get("link")
            if link in BLACKLIST_LINKS:
                link = None
            items.append(
                {
                    "id": row.get("id"),
                    "title": title,
                    "company": "",
                    "location": row.get("location") or "Remote / Anywhere",
                    "description": parse_job_description(row.get("job_description") or ""),
                    "date_posted": format_job_date_string(job_date_str) if job_date_str else "",
                    "link": link,
                    "is_new": _job_is_new(job_date_raw, row.get("date")),
                }
            )

        if not raw_title and not raw_country and not items:
            demo_jobs = [
                {
                    "id": f"demo-{i}",
                    "title": title,
                    "company": company,
                    "location": location,
                    "description": desc,
                    "date_posted": date,
                    "link": "",
                    "is_new": False,
                }
                for i, (title, company, location, desc, date) in enumerate(
                    [
                        (
                            "Senior Software Engineer (AI)",
                            "Catalitium",
                            "Remote / EU",
                            "Own end-to-end features across ingestion, ranking, and AI-assisted matching.",
                            "2025.10.01",
                        ),
                        (
                            "Data Engineer",
                            "Catalitium",
                            "Berlin, DE",
                            "Build reliable pipelines and optimize warehouse performance.",
                            "2025.09.28",
                        ),
                        (
                            "Product Manager",
                            "Stealth",
                            "Zurich, CH",
                            "Partner with design and engineering to deliver user value quickly.",
                            "2025.09.27",
                        ),
                        (
                            "Frontend Developer",
                            "Acme Corp",
                            "Barcelona, ES",
                            "Ship delightful UI with Tailwind and strong accessibility.",
                            "2025.09.26",
                        ),
                        (
                            "Cloud DevOps Engineer",
                            "Nimbus",
                            "Munich, DE",
                            "Automate infrastructure, observability, and release workflows.",
                            "2025.09.25",
                        ),
                        (
                            "ML Engineer",
                            "Quantix",
                            "Remote",
                            "Deploy ranking and semantic matching at scale.",
                            "2025.09.24",
                        ),
                    ],
                    start=1,
                )
            ]
            items = demo_jobs
            total = len(demo_jobs)
            pages = 1
            page = 1
            per_page = len(demo_jobs)

        pagination = {
            "page": page,
            "pages": pages if pages else 1,
            "total": total,
            "per_page": per_page,
            "has_prev": page > 1,
            "has_next": page < (pages if pages else 1),
            "prev_url": url_for("index", title=title_q or None, country=country_q or None, page=page - 1)
            if page > 1
            else None,
            "next_url": url_for("index", title=title_q or None, country=country_q or None, page=page + 1)
            if page < (pages if pages else 1)
            else None,
        }

        return render_template(
            "index.html",
            results=items,
            count=total,
            title_q=title_q,
            country_q=country_q,
            pagination=pagination,
        )

    @app.get("/api/jobs")
    def api_jobs():
        raw_title = (request.args.get("title") or "").strip()
        raw_country = (request.args.get("country") or "").strip()
        page = request.args.get("page", default=1, type=int) or 1
        if page < 1:
            page = 1
        per_page_req = request.args.get("per_page", default=20, type=int) or 20
        per_page = max(10, min(per_page_req, int(app.config.get("PER_PAGE_MAX", 100))))

        cleaned_title, _, _ = parse_salary_query(raw_title)
        country_q = normalize_country(raw_country)
        title_q = normalize_title(cleaned_title)

        try:
            total = Job.count(title_q or None, country_q or None)
            pages = (total + per_page - 1) // per_page if per_page else 1
            offset = (max(1, page) - 1) * per_page
            rows = Job.search(title_q or None, country_q or None, limit=per_page, offset=offset)
        except Exception:
            total = 0
            pages = 1
            rows = []

        items = []
        for row in rows:
            job_date_raw = row.get("job_date")
            job_date_str = str(job_date_raw).strip() if job_date_raw is not None else ""
            link = row.get("link")
            if link in BLACKLIST_LINKS:
                link = None
            items.append(
                {
                    "id": row.get("id"),
                    "title": _to_lc(row.get("job_title") or ""),
                    "description": clean_job_description_text(row.get("job_description") or ""),
                    "link": link,
                    "location": row.get("location"),
                    "job_date": format_job_date_string(job_date_str) if job_date_str else "",
                    "date": row.get("date"),
                    "is_new": _job_is_new(job_date_raw, row.get("date")),
                }
            )

        return jsonify(
            {
                "items": items,
                "meta": {
                    "page": max(1, page),
                    "per_page": per_page,
                    "total": total,
                    "pages": pages,
                    "has_prev": page > 1,
                    "has_next": page < pages,
                },
            }
        )

    @app.post("/subscribe")
    @limiter.limit("5/minute;50/hour")
    def subscribe():
        is_json = request.is_json
        payload = request.get_json(silent=True) or {} if is_json else request.form
        email = (payload.get("email") or "").strip()
        job_id_raw = (payload.get("job_id") or "").strip()

        try:
            email = validate_email(email, check_deliverability=False).normalized
        except EmailNotValidError:
            if is_json:
                return jsonify({"error": "invalid_email"}), 400
            flash("Please enter a valid email.", "error")
            return redirect(url_for("index"))

        job_link = Job.get_link(job_id_raw)
        status = insert_subscriber(email)
        source = "api" if is_json else "form"
        if job_link:
            source = f"{source}_job"
        insert_subscribe_event(email=email, status=status, source=source)

        if job_link:
            if status == "error":
                if is_json:
                    return jsonify({"error": "subscribe_failed"}), 500
                flash("We couldn't process your email. Please try again later.", "error")
                return redirect(url_for("index"))
            if is_json:
                body = {"status": status, "redirect": job_link}
                return jsonify(body), 200
            if status == "ok":
                flash("You're subscribed! You're all set.", "success")
            elif status == "duplicate":
                flash("You're already on the list.", "success")
            return redirect(job_link)

        if is_json:
            if status == "ok":
                return jsonify({"status": "ok"}), 200
            if status == "duplicate":
                return jsonify({"error": "duplicate"}), 200
            return jsonify({"error": "subscribe_failed"}), 500

        if status == "ok":
            flash("You're subscribed! You're all set.", "success")
        elif status == "duplicate":
            flash("You're already on the list.", "success")
        else:
            flash("We couldn't process your email. Please try again later.", "error")
        return redirect(url_for("index"))

    @app.post("/events/apply")
    @limiter.limit("30/minute;300/hour")
    def events_apply():
        payload = request.get_json(silent=True) or {}
        status = (payload.get("status") or "").strip() or "unknown"
        job_id = (payload.get("job_id") or payload.get("jobId") or "").strip()
        job_title = (payload.get("job_title") or payload.get("jobTitle") or "").strip()
        job_company = (payload.get("job_company") or payload.get("jobCompany") or "").strip()
        job_location = (payload.get("job_location") or payload.get("jobLocation") or "").strip()
        job_link = (payload.get("job_link") or payload.get("jobLink") or "").strip()
        job_summary = (payload.get("job_summary") or payload.get("jobSummary") or "").strip()
        source = (payload.get("source") or "web").strip() or "web"
        insert_search_event(
            raw_title=job_title or "N/A",
            raw_country=job_location or "N/A",
            norm_title="",
            norm_country="",
            sal_floor=None,
            sal_ceiling=None,
            result_count=0,
            page=0,
            per_page=0,
            source=source,
            event_type="apply",
            event_status=status,
            job_id=job_id,
            job_title=job_title,
            job_company=job_company,
            job_location=job_location,
            job_link=job_link,
            job_summary=job_summary,
        )
        return jsonify({"status": "ok"}), 200

    @app.get("/api/salary-insights")
    def api_salary_insights():
        raw_title = (request.args.get("title") or "").strip()
        raw_country = (request.args.get("country") or "").strip()
        title_q = normalize_title(raw_title)
        country_q = normalize_country(raw_country)
        rows = Job.search(title_q or None, country_q or None, limit=100, offset=0)
        items = [
            {
                "title": _to_lc(row.get("job_title") or ""),
                "location": row.get("location"),
                "job_date": format_job_date_string((row.get("job_date") or "").strip()),
                "link": row.get("link"),
                "is_new": _job_is_new(row.get("job_date"), row.get("date")),
            }
            for row in rows
        ]
        return jsonify(
            {
                "count": len(items),
                "items": items,
                "meta": {"title": title_q, "country": country_q},
            }
        )

    @app.get("/health")
    def health():
        try:
            db = get_db()
            with db.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        except Exception:
            return jsonify({"status": "error", "db": "failed"}), 503
        return jsonify({"status": "ok", "db": "connected"}), 200

    return app


def _job_is_new(job_date_raw, row_date) -> bool:
    dt = _coerce_datetime(row_date) or _coerce_datetime(job_date_raw)
    if not dt:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - dt) <= timedelta(days=2)


def _coerce_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_datetime"):
        try:
            return value.to_datetime()
        except Exception:
            pass
    if hasattr(value, "isoformat"):
        try:
            iso = value.isoformat()
            return datetime.fromisoformat(iso)
        except Exception:
            pass
    text = str(value).strip()
    if not text:
        return None
    # Attempt ISO parsing first
    try:
        return datetime.fromisoformat(text)
    except Exception:
        pass
    formats = ("%Y-%m-%d", "%Y.%m.%d", "%Y%m%d", "%Y/%m/%d")
    for fmt in formats:
        try:
            dt = datetime.strptime(text[: len(fmt)], fmt)
            return dt
        except Exception:
            continue
    return None


def _to_lc(value: str) -> str:
    parts = [p for p in re.split(r"[^A-Za-z0-9]+", value or "") if p]
    if not parts:
        return value or ""
    head, *tail = parts
    return head.lower() + "".join(part.capitalize() for part in tail)


if __name__ == "__main__":
    application = create_app()
    application.run(debug=True)
