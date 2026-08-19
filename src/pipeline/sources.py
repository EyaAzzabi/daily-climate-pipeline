"""Open-Meteo archive client.

Chosen because it needs no API key, which means a reviewer can clone this repo and
run it immediately. A pipeline that requires the reader to register for a service
before it does anything is a pipeline nobody verifies.

Every network call is retried with exponential backoff. The failure mode this
guards against is not the API being down -- that is rare and obvious -- but the
occasional 5xx or timeout that would otherwise leave a single day missing from the
warehouse, silently, until somebody plots a gap six weeks later.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, timedelta

from .config import ARCHIVE_LAG_DAYS, DAILY_VARIABLES, City

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
USER_AGENT = "daily-climate-pipeline/0.1 (portfolio project)"


class SourceError(RuntimeError):
    """Raised when the source cannot be read after all retries."""


def target_date(today: date | None = None) -> date:
    """The most recent date the archive can be expected to have settled."""
    today = today or date.today()
    return today - timedelta(days=ARCHIVE_LAG_DAYS)


def fetch_city(
    city: City,
    start: date,
    end: date,
    *,
    max_attempts: int = 4,
    backoff_seconds: float = 2.0,
    timeout: float = 30.0,
) -> dict:
    """Fetch daily observations for one city over an inclusive date range."""
    if end < start:
        raise ValueError(f"end {end} precedes start {start}")

    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": "UTC",
    }
    url = f"{ARCHIVE_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if "daily" not in payload:
                raise SourceError(f"response for {city.name} has no 'daily' block")
            return payload
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, SourceError) as exc:
            last_error = exc
            if attempt == max_attempts:
                break
            # Exponential backoff. No jitter: this pipeline runs one request at a
            # time, so there is no thundering herd to spread out.
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))

    raise SourceError(
        f"failed to fetch {city.name} for {start}..{end} "
        f"after {max_attempts} attempts: {last_error}"
    )


def to_records(city: City, payload: dict) -> list[dict]:
    """Flatten Open-Meteo's column-oriented response into row records.

    The API returns parallel arrays, one per variable. They are documented to be
    the same length, but this asserts it rather than trusting it: mismatched
    lengths would silently shift every value against the wrong date.
    """
    daily = payload.get("daily", {})
    times = daily.get("time", [])
    for variable in DAILY_VARIABLES:
        values = daily.get(variable)
        if values is None:
            raise SourceError(f"missing variable {variable} for {city.name}")
        if len(values) != len(times):
            raise SourceError(
                f"length mismatch for {variable} in {city.name}: "
                f"{len(values)} values against {len(times)} dates"
            )

    records = []
    for i, observed in enumerate(times):
        record = {
            "city_id": city.city_id,
            "city_name": city.name,
            "country": city.country,
            "latitude": city.latitude,
            "longitude": city.longitude,
            "observed_date": observed,
        }
        for variable in DAILY_VARIABLES:
            record[variable] = daily[variable][i]
        records.append(record)
    return records
