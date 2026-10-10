"""Tests for the extract module (mocked HTTP, no real API calls)."""

from unittest.mock import MagicMock, patch

import pytest

from src.extract.fetch_data import DataExtractor, ExtractionError

VALID_URL = "https://api.open-meteo.com/v1/forecast"

SAMPLE_RESPONSE = {
    "daily": {
        "time": ["2026-10-01", "2026-10-02"],
        "temperature_2m_max": [20.1, 22.3],
        "temperature_2m_min": [10.0, 11.5],
        "rain_sum": [0.0, 1.2],
    }
}


@pytest.fixture
def mock_response() -> MagicMock:
    """Build a mocked requests.Response returning sample JSON."""
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = SAMPLE_RESPONSE
    response.raise_for_status.return_value = None
    return response


class TestDataExtractorInit:
    def test_instantiation_with_valid_url(self) -> None:
        extractor = DataExtractor(VALID_URL)
        assert extractor.base_url == VALID_URL

    def test_invalid_url_raises_error(self) -> None:
        with pytest.raises(ValueError):
            DataExtractor("not-a-valid-url")

    def test_empty_url_raises_error(self) -> None:
        with pytest.raises(ValueError):
            DataExtractor("")


class TestParameterValidation:
    @pytest.mark.parametrize("lat,lon", [(0.0, 0.0), (90.0, 180.0), (-90.0, -180.0)])
    def test_valid_coords_accepted(self, lat: float, lon: float, mock_response: MagicMock) -> None:
        extractor = DataExtractor(VALID_URL)
        with patch("requests.get", return_value=mock_response):
            data = extractor.fetch_forecast(lat, lon)
        assert "daily" in data

    @pytest.mark.parametrize("lat,lon", [(91.0, 0.0), (-91.0, 0.0), (0.0, 181.0), (0.0, -181.0)])
    def test_out_of_bounds_coords_raise_error(self, lat: float, lon: float) -> None:
        extractor = DataExtractor(VALID_URL)
        with pytest.raises(ValueError):
            extractor.fetch_forecast(lat, lon)

    def test_invalid_forecast_days_raise_error(self) -> None:
        extractor = DataExtractor(VALID_URL)
        with pytest.raises(ValueError):
            extractor.fetch_forecast(52.0, 21.0, days=0)


class TestFetching:
    def test_fetch_forecast_parses_json(self, mock_response: MagicMock) -> None:
        extractor = DataExtractor(VALID_URL)
        with patch("requests.get", return_value=mock_response) as mock_get:
            data = extractor.fetch_forecast(52.23, 21.01, days=7)
        assert data == SAMPLE_RESPONSE
        assert mock_get.call_args.kwargs["timeout"] == 10

    def test_http_error_raises_extraction_error(self) -> None:
        import requests

        response = MagicMock()
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 error")
        extractor = DataExtractor(VALID_URL)
        with patch("requests.get", return_value=response):
            with pytest.raises(ExtractionError):
                extractor.fetch_forecast(52.0, 21.0)

    def test_fetch_historical_validates_date_format(self) -> None:
        extractor = DataExtractor(VALID_URL)
        with pytest.raises(ValueError):
            extractor.fetch_historical(52.0, 21.0, "2026/01/01", "2026-01-10")

    def test_fetch_historical_returns_json(self, mock_response: MagicMock) -> None:
        extractor = DataExtractor(VALID_URL)
        with patch("requests.get", return_value=mock_response):
            data = extractor.fetch_historical(52.0, 21.0, "2026-01-01", "2026-01-10")
        assert data == SAMPLE_RESPONSE
