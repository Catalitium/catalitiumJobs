import os
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

os.environ.setdefault("ENV", "production")
os.environ.setdefault("FLASK_ENV", "production")
os.environ.setdefault("FORCE_SQLITE", "1")
os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("RATELIMIT_STORAGE_URL", "memory://")

from app.app import create_app  # noqa: E402  (env is prepared above)


class AppRoutesTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._init_patcher = patch("app.app.init_db", autospec=True)
        cls._init_patcher.start()
        cls.app = create_app()
        cls.client = cls.app.test_client()

    @classmethod
    def tearDownClass(cls):
        cls._init_patcher.stop()

    def test_create_app_registers_expected_routes(self):
        expected_endpoints = {"index", "api_jobs", "subscribe", "api_salary_insights", "health"}
        self.assertTrue(expected_endpoints.issubset(self.app.view_functions.keys()))

    def test_health_ok(self):
        mock_conn = MagicMock()
        mock_cursor_cm = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor_cm.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value = mock_cursor_cm
        with patch("app.app.get_db", return_value=mock_conn):
            response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"status": "ok", "db": "connected"})
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    def test_health_failure(self):
        with patch("app.app.get_db", side_effect=RuntimeError("db down")):
            response = self.client.get("/health")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json(), {"status": "error", "db": "failed"})

    def test_index_with_results_logs_search(self):
        mock_rows = [
            {
                "id": 1,
                "job_title": "Backend Engineer",
                "job_description": "Build services",
                "location": "Berlin, DE",
                "job_date": "2024-10-01",
                "date": "2024-10-01T00:00:00",
                "link": "https://example.com/backend",
            }
        ]
        with patch("app.app.Job.count", return_value=1), patch(
            "app.app.Job.search", return_value=mock_rows
        ), patch("app.app.insert_search_event") as mock_event:
            response = self.client.get("/?title=Engineer&country=DE")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Backend Engineer", html)
        mock_event.assert_called_once()

    def test_index_demo_jobs_when_no_results(self):
        with patch("app.app.Job.count", return_value=0), patch(
            "app.app.Job.search", return_value=[]
        ), patch("app.app.insert_search_event") as mock_event:
            response = self.client.get("/")
        html = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Senior Software Engineer (AI)", html)
        mock_event.assert_not_called()

    def test_api_jobs_clamps_per_page_and_filters_blacklist(self):
        mock_rows = [
            {
                "id": 1,
                "job_title": "Data Scientist",
                "job_description": "",
                "location": "Remote",
                "job_date": "2024-09-10",
                "date": "2024-09-11T00:00:00",
                "link": "https://example.com/job/1",
            }
        ]
        with patch("app.app.Job.count", return_value=1), patch(
            "app.app.Job.search", return_value=mock_rows
        ):
            response = self.client.get("/api/jobs?per_page=5")
        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["meta"]["per_page"], 10)
        self.assertIsNone(payload["items"][0]["link"])

    def test_subscribe_json_success_with_redirect(self):
        with patch("app.app.Job.get_link", return_value="https://example.com/apply"), patch(
            "app.app.insert_subscriber", return_value="ok"
        ), patch("app.app.insert_subscribe_event") as mock_event:
            response = self.client.post(
                "/subscribe",
                json={"email": "user@example.com", "job_id": "1"},
            )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["redirect"], "https://example.com/apply")
        self.assertEqual(payload["status"], "ok")
        mock_event.assert_called_once()

    def test_subscribe_json_duplicate(self):
        with patch("app.app.Job.get_link", return_value=""), patch(
            "app.app.insert_subscriber", return_value="duplicate"
        ), patch("app.app.insert_subscribe_event"):
            response = self.client.post(
                "/subscribe",
                json={"email": "user@example.com"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {"error": "duplicate"})

    def test_subscribe_invalid_email_returns_error(self):
        response = self.client.post("/subscribe", json={"email": "not-an-email"})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json(), {"error": "invalid_email"})

    def test_subscribe_form_redirects(self):
        with patch("app.app.Job.get_link", return_value="https://example.com/apply"), patch(
            "app.app.insert_subscriber", return_value="ok"
        ), patch("app.app.insert_subscribe_event"):
            response = self.client.post(
                "/subscribe",
                data={"email": "user@example.com", "job_id": "1"},
                follow_redirects=False,
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "https://example.com/apply")

    def test_api_salary_insights_flags_recent_jobs(self):
        recent = datetime.now(timezone.utc).isoformat()
        mock_rows = [
            {
                "job_title": "ML Engineer",
                "location": "Remote",
                "job_date": recent,
                "date": recent,
                "link": "https://example.com/ml",
            }
        ]
        with patch("app.app.Job.search", return_value=mock_rows):
            response = self.client.get("/api/salary-insights?title=ml")
        payload = response.get_json()
        self.assertEqual(payload["count"], 1)
        self.assertTrue(payload["items"][0]["is_new"])

    def test_events_apply_records_analytics(self):
        with patch("app.app.insert_search_event") as mock_event:
            response = self.client.post(
                "/events/apply",
                json={
                    "status": "modal_open",
                    "job_id": "123",
                    "job_title": "UX Designer",
                    "job_company": "Acme",
                    "job_location": "Remote",
                    "job_link": "https://example.com/ux",
                    "job_summary": "Design delightful flows.",
                    "source": "web",
                },
            )
        self.assertEqual(response.status_code, 200)
        mock_event.assert_called_once()
        kwargs = mock_event.call_args.kwargs
        self.assertEqual(kwargs["event_type"], "apply")
        self.assertEqual(kwargs["event_status"], "modal_open")
        self.assertEqual(kwargs["job_title"], "UX Designer")


if __name__ == "__main__":
    unittest.main()
