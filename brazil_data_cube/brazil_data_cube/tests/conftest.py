# tests/conftest.py
import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_fetcher():
    fetcher = MagicMock()
    fetcher.fetch_image.return_value = {
        "B04": {"href": "fake_href_B04"},
        "B03": {"href": "fake_href_B03"},
        "B02": {"href": "fake_href_B02"}
    }
    return fetcher

@pytest.fixture
def mock_downloader():
    downloader = MagicMock()
    downloader.download.side_effect = lambda asset, filename: filename
    return downloader