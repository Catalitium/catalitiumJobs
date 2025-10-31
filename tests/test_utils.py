import unittest
from datetime import datetime, timedelta, timezone

from app.app import _coerce_datetime, _job_is_new, _to_lc


class UtilsTestCase(unittest.TestCase):
    def test_coerce_datetime_iso_string(self):
        dt = _coerce_datetime("2024-10-01T12:30:00")
        self.assertIsInstance(dt, datetime)
        self.assertEqual(dt.year, 2024)

    def test_coerce_datetime_returns_none_for_invalid(self):
        self.assertIsNone(_coerce_datetime("not-a-date"))

    def test_job_is_new_true(self):
        recent = datetime.now(timezone.utc) - timedelta(hours=12)
        self.assertTrue(_job_is_new(recent, None))

    def test_job_is_new_false(self):
        older = datetime.now(timezone.utc) - timedelta(days=3)
        self.assertFalse(_job_is_new(older, None))

    def test_to_lc_converts_with_digits(self):
        self.assertEqual(_to_lc("Senior ML Engineer"), "seniorMlEngineer")


if __name__ == "__main__":
    unittest.main()
