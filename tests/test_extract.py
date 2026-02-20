"""
test_extract.py — unit tests for the Socrata extraction logic.
Uses unittest.mock to avoid real API calls.
"""

from unittest.mock import MagicMock, patch
import pytest
from src.extract import COLUMNS


def make_page(size, offset=0):
    """Generate synthetic Socrata API rows."""
    return [
        {"license_nbr": f"LIC-{offset + i:06d}", "business_name": f"Biz {i}"}
        for i in range(size)
    ]


class TestExtractPagination:
    def test_single_page_stops(self, tmp_path):
        """When the API returns fewer rows than PAGE_SIZE, extraction stops."""
        page = make_page(10)

        with patch("src.extract.requests.get") as mock_get, \
             patch("src.extract.RAW_DATA_DIR", str(tmp_path)):

            resp = MagicMock()
            resp.json.return_value = page
            resp.raise_for_status.return_value = None
            mock_get.return_value = resp

            from src.extract import fetch_snapshot
            from datetime import date
            path = fetch_snapshot(date(2026, 1, 1))

        assert mock_get.call_count == 1, "Should stop after one partial page"
        import csv
        with open(path) as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 10

    def test_csv_has_correct_columns(self, tmp_path):
        """Output CSV must contain exactly the contracted column set."""
        page = make_page(5)

        with patch("src.extract.requests.get") as mock_get, \
             patch("src.extract.RAW_DATA_DIR", str(tmp_path)):

            resp = MagicMock()
            resp.json.return_value = page
            resp.raise_for_status.return_value = None
            mock_get.return_value = resp

            from src.extract import fetch_snapshot
            from datetime import date
            path = fetch_snapshot(date(2026, 1, 2))

        import csv
        with open(path) as f:
            reader = csv.DictReader(f)
            assert set(reader.fieldnames) == set(COLUMNS)

    def test_api_error_raises(self, tmp_path):
        """HTTP errors should propagate — never silently produce empty files."""
        with patch("src.extract.requests.get") as mock_get, \
             patch("src.extract.RAW_DATA_DIR", str(tmp_path)):

            resp = MagicMock()
            resp.raise_for_status.side_effect = Exception("HTTP 429 Too Many Requests")
            mock_get.return_value = resp

            from src.extract import fetch_snapshot
            from datetime import date
            with pytest.raises(Exception, match="HTTP 429"):
                fetch_snapshot(date(2026, 1, 3))
