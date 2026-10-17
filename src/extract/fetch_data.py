"""Fetch weather data from the Open-Meteo public API."""

import logging
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)

VALID_LAT_RANGE = (-90.0, 90.0)
VALID_LON_RANGE = (-180.0, 180.0)


class ExtractionError(Exception):
    """Raised when data extraction from the API fails."""


MAX_FORECAST_DAYS = 16
MAX_PAST_DAYS = 92


class DataExtractor:
    """Client for the Open-Meteo weather API.

    Attributes:
        base_url: Root URL of the Open-Meteo endpoint.
        timeout: Request timeout in seconds.
    """

    DAILY_FIELDS = ["temperature_2m_max", "temperature_2m_min", "rain_sum"]

    def __init__(self, base_url: str, timeout: int = 10) -> None:
        """Initialize the extractor.

        Args:
            base_url: Base URL of the API, e.g.
                ``https://api.open-meteo.com/v1/forecast``.
            timeout: Maximum seconds to wait for a response.

        Raises:
            ValueError: If ``base_url`` is not a valid http(s) URL.
        """
        if not base_url.startswith(("http://", "https://")):
            raise ValueError(f"Invalid base URL: {base_url!r}")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    @staticmethod
    def _validate_coords(lat: float, lon: float) -> None:
        """Validate latitude/longitude bounds.

        Raises:
            ValueError: If coordinates are out of range.
        """
        if not VALID_LAT_RANGE[0] <= lat <= VALID_LAT_RANGE[1]:
            raise ValueError(f"Latitude {lat} out of range [-90, 90]")
        if not VALID_LON_RANGE[0] <= lon <= VALID_LON_RANGE[1]:
            raise ValueError(f"Longitude {lon} out of range [-180, 180]")

    def fetch_forecast(
        self, lat: float, lon: float, days: int = 16, past_days: int = 0
    ) -> dict[str, Any]:
        """Fetch a daily forecast for the given coordinates.

        Args:
            lat: Latitude in decimal degrees.
            lon: Longitude in decimal degrees.
            days: Number of forecast days (1-16, API maximum).
            past_days: Number of past days to prepend (0-92, API
                maximum). Allows gathering recent historical data in
                the same request.

        Returns:
            Parsed JSON response containing ``daily`` metrics:
            temperature_2m_max, temperature_2m_min and rain_sum.

        Raises:
            ValueError: On invalid coordinates or time range.
            ExtractionError: On network or HTTP errors.
        """
        self._validate_coords(lat, lon)
        if not 1 <= days <= MAX_FORECAST_DAYS:
            raise ValueError(
                f"Forecast days must be between 1 and {MAX_FORECAST_DAYS}, got {days}"
            )
        if not 0 <= past_days <= MAX_PAST_DAYS:
            raise ValueError(
                f"Past days must be between 0 and {MAX_PAST_DAYS}, got {past_days}"
            )

        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": ",".join(self.DAILY_FIELDS),
            "timezone": "auto",
            "forecast_days": days,
            "past_days": past_days,
        }
        return self._get(params)

    def fetch_historical(
        self, lat: float, lon: float, start_date: str, end_date: str
    ) -> dict[str, Any]:
        """Fetch historical daily weather for the given coordinates.

        Args:
            lat: Latitude in decimal degrees.
            lon: Longitude in decimal degrees.
            start_date: Start date in ``YYYY-MM-DD`` format.
            end_date: End date in ``YYYY-MM-DD`` format.

        Returns:
            Parsed JSON response with the same structure as
            :meth:`fetch_forecast`.

        Raises:
            ValueError: On invalid coordinates or date strings.
            ExtractionError: On network or HTTP errors.
        """
        self._validate_coords(lat, lon)
        for name, value in (("start_date", start_date), ("end_date", end_date)):
            if len(value) != 10 or value[4] != "-" or value[7] != "-":
                raise ValueError(f"{name} must use YYYY-MM-DD format, got {value!r}")
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        if end < start:
            raise ValueError("end_date must not be before start_date")
        if (end - start).days > MAX_PAST_DAYS:
            raise ValueError(
                f"Historical range must not exceed {MAX_PAST_DAYS} days (~3 months)"
            )

        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": ",".join(self.DAILY_FIELDS),
            "timezone": "auto",
            "start_date": start_date,
            "end_date": end_date,
        }
        return self._get(params)

    def _get(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute a GET request and parse the JSON response.

        Raises:
            ExtractionError: On connection errors, timeouts, non-2xx
                responses or malformed JSON.
        """
        try:
            response = requests.get(self.base_url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout as exc:
            raise ExtractionError(f"Request timed out after {self.timeout}s") from exc
        except requests.exceptions.ConnectionError as exc:
            raise ExtractionError(f"Connection to {self.base_url} failed: {exc}") from exc
        except requests.exceptions.HTTPError as exc:
            raise ExtractionError(f"HTTP error {response.status_code}: {exc}") from exc
        except requests.exceptions.RequestException as exc:
            raise ExtractionError(f"Unexpected request failure: {exc}") from exc
        except ValueError as exc:
            raise ExtractionError(f"Response is not valid JSON: {exc}") from exc
