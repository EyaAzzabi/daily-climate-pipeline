"""Pipeline configuration.

Cities are configuration, not code: adding one should never require touching the
ingestion logic. The set below spans the Maghreb and the northern Mediterranean so
the data contains genuinely different climates -- a pipeline tested only on
similar values will not notice when a range check is too loose.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.getenv("PIPELINE_DATA_DIR", ROOT / "data"))
REPORTS_DIR = Path(os.getenv("PIPELINE_REPORTS_DIR", ROOT / "reports"))

BRONZE_DIR = DATA_DIR / "bronze"
WAREHOUSE_PATH = DATA_DIR / "climate.duckdb"


@dataclass(frozen=True)
class City:
    city_id: str
    name: str
    country: str
    latitude: float
    longitude: float


CITIES: tuple[City, ...] = (
    City("tun", "Tunis", "TN", 36.8065, 10.1815),
    City("sfa", "Sfax", "TN", 34.7406, 10.7603),
    City("tza", "Tozeur", "TN", 33.9197, 8.1335),
    City("alg", "Algiers", "DZ", 36.7538, 3.0588),
    City("cas", "Casablanca", "MA", 33.5731, -7.5898),
    City("cai", "Cairo", "EG", 30.0444, 31.2357),
    City("mar", "Marseille", "FR", 43.2965, 5.3698),
    City("bcn", "Barcelona", "ES", 41.3874, 2.1686),
    City("rom", "Rome", "IT", 41.9028, 12.4964),
    City("ath", "Athens", "GR", 37.9838, 23.7275),
)

CITY_BY_ID = {c.city_id: c for c in CITIES}

# Daily variables requested from Open-Meteo. Keep this aligned with the schema in
# quality.py -- a variable added here without a matching expectation is a column
# that nothing validates.
DAILY_VARIABLES = (
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "windspeed_10m_max",
)

# The archive API lags real time by about five days while observations are
# reconciled. Requesting yesterday returns nulls, which would look like a source
# outage rather than the documented behaviour it is.
ARCHIVE_LAG_DAYS = int(os.getenv("PIPELINE_ARCHIVE_LAG_DAYS", "6"))
