import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from app.app import create_app, limiter
from app.models.db import Job, get_db, close_db


class ProdTestCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="catalitium-tests-")
        self.db_path = os.path.join(self.temp_dir, "functional.sqlite")
        os.environ["DB_PATH"] = self.db_path
        os.environ["FORCE_SQLITE"] = "1"
        os.environ["SECRET_KEY"] = "test-secret-key"
        os.environ["RATELIMIT_STORAGE_URL"] = "memory://"
        os.environ.setdefault("SUPABASE_URL", "")
        self.app = create_app()
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()
        self.ctx = self.app.app_context()
        self.ctx.push()
        limiter.reset()
        self.ip_counter = 10
        self.job_counter = 0

    def tearDown(self):
        close_db()
        self.ctx.pop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def next_ip(self):
        self.ip_counter += 1
        return f"127.0.0.{self.ip_counter}"

    def add_job(self, **overrides):
        self.job_counter += 1
        payload = {
            "job_title": overrides.get("job_title", "Data Engineer"),
            "job_description": overrides.get("job_description", "Build and maintain data pipelines."),
            "link": overrides.get("link", f"https://jobs.example.com/{self.job_counter}"),
            "job_title_norm": overrides.get("job_title_norm"),
            "location": overrides.get("location", "Berlin, DE"),
            "job_date": overrides.get("job_date", "2025-01-01"),
            "date": overrides.get("date"),
        }
        Job.insert_many([payload])
        db = get_db()
        with db.cursor() as cur:
            cur.execute("SELECT id FROM Jobs WHERE link = %s", [payload["link"]])
            row = cur.fetchone()
        return row[0] if row else None

    def subscriber_emails(self):
        db = get_db()
        with db.cursor() as cur:
            cur.execute("SELECT email FROM subscribers ORDER BY email")
            return [row[0] for row in cur.fetchall()]

    # -------------------- Index ------------------------------------------------

    def test_index_returns_demo_jobs_when_empty(self):
        resp = self.client.get("/")
        body = resp.get_data(as_text=True)
        self.assertEqual(resp.status_code, 200)
        self.assertIn("demo-1", body)
        self.assertNotIn("No jobs found.", body)

    def test_index_lists_inserted_job(self):
        self.add_job(job_title="Platform Engineer", location="Zurich, CH")
        resp = self.client.get("/")
        body = resp.get_data(as_text=True)
        self.assertIn("Platform Engineer", body)
        self.assertIn("Zurich, CH", body)

    def test_index_filters_by_title(self):
        self.add_job(job_title="Front End Developer")
        resp = self.client.get("/?title=frontend")
        body = resp.get_data(as_text=True)
        self.assertIn("Front End Developer", body)

    def test_index_filters_by_country(self):
        self.add_job(job_title="Backend Engineer", location="Madrid, ES")
        resp = self.client.get("/?country=ES")
        body = resp.get_data(as_text=True)
        self.assertIn("Backend Engineer", body)
        self.assertIn("Madrid, ES", body)

    def test_index_invalid_page_defaults_to_one(self):
        for i in range(25):
            self.add_job(job_title=f"Paged {i}", link=f"https://jobs.example.com/paged{i}")
        resp = self.client.get("/?page=0")
        self.assertIn("Page 1 of", resp.get_data(as_text=True))

    def test_index_shows_no_jobs_message_on_filter_miss(self):
        self.add_job(job_title="Security Engineer", location="Paris, FR")
        resp = self.client.get("/?title=astronaut")
        self.assertIn("No jobs found.", resp.get_data(as_text=True))

    def test_index_blacklists_known_link(self):
        self.add_job(link="https://example.com/job/1")
        body = self.client.get("/").get_data(as_text=True)
        self.assertNotIn("https://example.com/job/1", body)

    def test_index_pagination_links_present(self):
        for i in range(30):
            self.add_job(job_title=f"Role {i}", link=f"https://jobs.example.com/{i}")
        body = self.client.get("/").get_data(as_text=True)
        self.assertIn('rel="next"', body)
        self.assertIn('aria-disabled="false"', body)

    # -------------------- API /jobs -------------------------------------------

    def test_api_jobs_returns_empty_collection(self):
        data = self.client.get("/api/jobs").get_json()
        self.assertEqual(data["items"], [])
        self.assertEqual(data["meta"]["total"], 0)

    def test_api_jobs_returns_inserted_job(self):
        self.add_job(job_title="Applied Scientist", location="London, UK")
        data = self.client.get("/api/jobs").get_json()
        self.assertEqual(data["meta"]["total"], 1)
        self.assertEqual(data["items"][0]["title"], "appliedScientist")
        self.assertEqual(data["items"][0]["location"], "London, UK")

    def test_api_jobs_applies_filters(self):
        self.add_job(job_title="Data Analyst", location="Madrid, ES")
        self.add_job(job_title="Support Engineer", location="Berlin, DE")
        data = self.client.get("/api/jobs?title=analyst&country=ES").get_json()
        self.assertEqual(data["meta"]["total"], 1)
        self.assertEqual(data["items"][0]["location"], "Madrid, ES")

    def test_api_jobs_blacklists_link(self):
        self.add_job(link="https://example.com/job/1")
        data = self.client.get("/api/jobs").get_json()
        self.assertIsNone(data["items"][0]["link"])

    def test_api_jobs_clamps_per_page(self):
        self.add_job()
        data = self.client.get("/api/jobs?per_page=500").get_json()
        self.assertEqual(data["meta"]["per_page"], 100)

    def test_api_jobs_enforces_page_floor(self):
        self.add_job()
        data = self.client.get("/api/jobs?page=0").get_json()
        self.assertEqual(data["meta"]["page"], 1)

    # -------------------- API /api/salary-insights ----------------------------

    def test_api_salary_insights_empty(self):
        data = self.client.get("/api/salary-insights").get_json()
        self.assertEqual(data["count"], 0)
        self.assertEqual(data["items"], [])

    def test_api_salary_insights_filters(self):
        self.add_job(job_title="Cloud Architect", location="Munich, DE")
        self.add_job(job_title="QA Engineer", location="Madrid, ES")
        data = self.client.get("/api/salary-insights?country=DE").get_json()
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["items"][0]["location"], "Munich, DE")

    # -------------------- Subscribe (HTML) ------------------------------------

    def test_subscribe_form_success(self):
        email = "user1@example.com"
        resp = self.client.post(
            "/subscribe",
            data={"email": email},
            follow_redirects=True,
            environ_overrides={"REMOTE_ADDR": self.next_ip()},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertIn(email, self.subscriber_emails())

    def test_subscribe_form_duplicate(self):
        email = "dupe@example.com"
        ip = self.next_ip()
        self.client.post(
            "/subscribe",
            data={"email": email},
            follow_redirects=True,
            environ_overrides={"REMOTE_ADDR": ip},
        )
        resp = self.client.post(
            "/subscribe",
            data={"email": email},
            follow_redirects=True,
            environ_overrides={"REMOTE_ADDR": ip},
        )
        self.assertIn("You&#39;re already on the list.", resp.get_data(as_text=True))

    def test_subscribe_form_invalid_email_flash(self):
        resp = self.client.post(
            "/subscribe",
            data={"email": "invalid"},
            follow_redirects=True,
            environ_overrides={"REMOTE_ADDR": self.next_ip()},
        )
        self.assertIn("Please enter a valid email.", resp.get_data(as_text=True))

    def test_subscribe_form_redirects_when_link_present(self):
        job_id = self.add_job(link="https://jobs.example.com/redirect")
        resp = self.client.post(
            "/subscribe",
            data={"email": "form@example.com", "job_id": str(job_id)},
            follow_redirects=False,
            environ_overrides={"REMOTE_ADDR": self.next_ip()},
        )
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.headers["Location"], "https://jobs.example.com/redirect")

    def test_subscribe_form_ignores_invalid_job_id(self):
        resp = self.client.post(
            "/subscribe",
            data={"email": "safe@example.com", "job_id": "abc"},
            follow_redirects=True,
            environ_overrides={"REMOTE_ADDR": self.next_ip()},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertIn("You&#39;re subscribed! You&#39;re all set.", resp.get_data(as_text=True))

    # -------------------- Subscribe (JSON) ------------------------------------

    def test_subscribe_json_success(self):
        resp = self.client.post(
            "/subscribe",
            json={"email": "json@example.com"},
            environ_overrides={"REMOTE_ADDR": self.next_ip()},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["status"], "ok")

    def test_subscribe_json_duplicate(self):
        email = "dupjson@example.com"
        ip = self.next_ip()
        self.client.post(
            "/subscribe",
            json={"email": email},
            environ_overrides={"REMOTE_ADDR": ip},
        )
        resp = self.client.post(
            "/subscribe",
            json={"email": email},
            environ_overrides={"REMOTE_ADDR": ip},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["error"], "duplicate")

    def test_subscribe_json_invalid_email(self):
        resp = self.client.post(
            "/subscribe",
            json={"email": "bad"},
            environ_overrides={"REMOTE_ADDR": self.next_ip()},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.get_json()["error"], "invalid_email")

    def test_subscribe_json_returns_redirect(self):
        job_id = self.add_job(link="https://jobs.example.com/api-redirect")
        resp = self.client.post(
            "/subscribe",
            json={"email": "jsonredir@example.com", "job_id": str(job_id)},
            environ_overrides={"REMOTE_ADDR": self.next_ip()},
        )
        data = resp.get_json()
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["redirect"], "https://jobs.example.com/api-redirect")

    def test_subscribe_rate_limit_enforced(self):
        ip = "127.0.0.200"
        for idx in range(5):
            self.client.post(
                "/subscribe",
                data={"email": f"limit{idx}@example.com"},
                follow_redirects=False,
                environ_overrides={"REMOTE_ADDR": ip},
            )
        resp = self.client.post(
            "/subscribe",
            data={"email": "limit-final@example.com"},
            follow_redirects=False,
            environ_overrides={"REMOTE_ADDR": ip},
        )
        self.assertEqual(resp.status_code, 429)

    # -------------------- Events ----------------------------------------------

    # -------------------- Health & Errors -------------------------------------

    def test_health_ok(self):
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["db"], "connected")

    def test_health_failure_returns_503(self):
        with patch("app.app.get_db", side_effect=RuntimeError("down")):
            resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 503)
        self.assertEqual(resp.get_json()["status"], "error")

    def test_404_handler_returns_json(self):
        resp = self.client.get("/missing")
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.get_json()["error"], "not found")

    def test_500_handler_returns_json(self):
        @self.app.get("/boom")
        def boom():
            raise RuntimeError("boom")

        previous = self.app.config.get("PROPAGATE_EXCEPTIONS", None)
        self.app.config["PROPAGATE_EXCEPTIONS"] = False
        try:
            resp = self.client.get("/boom")
        finally:
            if previous is None:
                self.app.config.pop("PROPAGATE_EXCEPTIONS", None)
            else:
                self.app.config["PROPAGATE_EXCEPTIONS"] = previous
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(resp.get_json()["error"], "internal error")

    # -------------------- Model Helpers ---------------------------------------

    def test_job_get_link_ignores_invalid_id(self):
        self.add_job(link="https://jobs.example.com/keep")
        resp = self.client.post(
            "/subscribe",
            json={"email": "guard@example.com", "job_id": "abc"},
            environ_overrides={"REMOTE_ADDR": self.next_ip()},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["status"], "ok")

    def test_job_count_matches_inserted_records(self):
        for i in range(3):
            self.add_job(job_title=f"Role {i}", link=f"https://jobs.example.com/count{i}")
        self.assertEqual(Job.count(), 3)


if __name__ == "__main__":
    unittest.main()
