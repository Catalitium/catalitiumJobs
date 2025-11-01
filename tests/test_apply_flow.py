import re
import pytest

from app.app import create_app
from app.models.db import get_db, Job


@pytest.fixture
def app_with_job(tmp_path, monkeypatch):
    db_path = tmp_path / "apply-modal.db"
    monkeypatch.setenv("SECRET_KEY", "test-secret")
    monkeypatch.setenv("FORCE_SQLITE", "1")
    monkeypatch.setenv("DB_PATH", str(db_path))
    app = create_app()
    app.config.update(TESTING=True)
    with app.app_context():
        Job.insert_many(
            [
                {
                    "job_title": "Apply Modal Tester",
                    "job_description": "Short description for testing apply modal functionality.",
                    "link": "https://example.com/apply-modal",
                    "location": "Remote",
                    "job_date": "2024-01-01",
                },
                {
                    "job_title": "Early Feature Engineer",
                    "job_description": "Placeholder description for the first job to fill layout.",
                    "link": "https://example.com/early-feature",
                    "location": "Remote",
                    "job_date": "2024-01-02",
                },
                {
                    "job_title": "Second Feature Engineer",
                    "job_description": "Placeholder description for the second job to fill layout.",
                    "link": "https://example.com/second-feature",
                    "location": "Remote",
                    "job_date": "2024-01-03",
                },
            ]
        )
        db = get_db()
        with db.cursor() as cur:
            cur.execute("SELECT id FROM Jobs WHERE link = %s", ("https://example.com/apply-modal",))
            row = cur.fetchone()
            job_id = row[0] if row else None
    yield app, job_id


def test_apply_button_contains_summary(app_with_job):
    app, job_id = app_with_job
    client = app.test_client()
    resp = client.get("/")
    html = resp.get_data(as_text=True)
    desc_values = re.findall(r'data-description="([^"]+)"', html)
    summary_values = re.findall(r'data-job-summary="([^"]+)"', html)
    assert any("Short description for testing apply modal functionality." in val for val in desc_values)
    assert any("Short description for testing apply modal functionality." in val for val in summary_values)


def test_subscribe_json_records_email_and_redirects(app_with_job):
    app, job_id = app_with_job
    client = app.test_client()
    email = "apply-user@example.com"
    payload = {"email": email, "job_id": str(job_id)}
    resp = client.post("/subscribe", json=payload)
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get("redirect") == "https://example.com/apply-modal"
    assert data.get("status") == "ok"

    # Duplicate submissions should be acknowledged and still supply redirect
    resp_dup = client.post("/subscribe", json=payload)
    assert resp_dup.status_code == 200
    data_dup = resp_dup.get_json()
    assert data_dup.get("status") == "duplicate"
    assert data_dup.get("redirect") == "https://example.com/apply-modal"

    with app.app_context():
        db = get_db()
        with db.cursor() as cur:
            cur.execute("SELECT email FROM subscribers WHERE email = %s", (email,))
            row = cur.fetchone()
        if isinstance(row, tuple):
            stored_email = row[0]
        else:
            stored_email = row["email"]
        assert stored_email == email


def test_events_apply_records_status(app_with_job):
    app, job_id = app_with_job
    client = app.test_client()
    payload = {
        "status": "modal_open",
        "job_id": str(job_id),
        "job_title": "Apply Modal Tester",
        "job_company": "Test Co",
        "job_location": "Remote",
        "job_link": "https://example.com/apply-modal",
        "job_summary": "Short description for testing apply modal functionality.",
        "source": "web",
    }
    resp = client.post("/events/apply", json=payload)
    assert resp.status_code == 200
    with app.app_context():
        db = get_db()
        with db.cursor() as cur:
            cur.execute(
                """
                SELECT event_type, event_status, job_title_event, job_summary_event
                FROM search_events
                ORDER BY id DESC LIMIT 1
                """
            )
            row = cur.fetchone()
        if isinstance(row, tuple):
            event_type, event_status, title_val, summary_val = row
        else:
            event_type = row["event_type"]
            event_status = row["event_status"]
            title_val = row["job_title_event"]
            summary_val = row["job_summary_event"]
    assert event_type == "apply"
    assert event_status == "modal_open"
    assert "Apply Modal Tester" in (title_val or "")
    assert "Short description" in (summary_val or "")
