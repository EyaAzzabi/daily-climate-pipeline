"""Warehouse tests, against a real DuckDB file in a temp directory.

Mocking the database here would test the mock. These run in well under a second.
"""

from datetime import date

import pytest

from pipeline import warehouse
from pipeline.config import CITIES


@pytest.fixture()
def con(tmp_path):
    connection = warehouse.connect(tmp_path / "test.duckdb")
    warehouse.upsert_cities(connection, CITIES)
    yield connection
    connection.close()


def rows(city_id="tun", start_day=1, n=3):
    return [
        {
            "city_id": city_id,
            "observed_date": f"2026-08-{start_day + i:02d}",
            "temperature_2m_max": 34.0 + i,
            "temperature_2m_min": 22.0 + i,
            "temperature_2m_mean": 28.0 + i,
            "precipitation_sum": float(i),
            "windspeed_10m_max": 18.0,
        }
        for i in range(n)
    ]


def count(con):
    return con.execute("SELECT count(*) FROM fact_daily_weather").fetchone()[0]


def test_load_inserts_rows(con):
    assert warehouse.load_records(con, rows()) == 3
    assert count(con) == 3


def test_running_twice_does_not_duplicate(con):
    """The property the whole scheduled pipeline depends on."""
    warehouse.load_records(con, rows())
    warehouse.load_records(con, rows())
    assert count(con) == 3


def test_reload_updates_values_in_place(con):
    warehouse.load_records(con, rows())
    corrected = rows(n=1)
    corrected[0]["temperature_2m_max"] = 99.0
    warehouse.load_records(con, corrected)

    assert count(con) == 3
    value = con.execute(
        "SELECT temperature_2m_max FROM fact_daily_weather "
        "WHERE city_id = 'tun' AND observed_date = '2026-08-01'"
    ).fetchone()[0]
    assert value == 99.0


def test_load_is_partitioned_by_city(con):
    warehouse.load_records(con, rows(city_id="tun"))
    warehouse.load_records(con, rows(city_id="alg"))
    assert count(con) == 6


def test_empty_load_is_a_no_op(con):
    assert warehouse.load_records(con, []) == 0
    assert count(con) == 0


def test_cities_upsert_is_idempotent(con):
    warehouse.upsert_cities(con, CITIES)
    assert con.execute("SELECT count(*) FROM dim_city").fetchone()[0] == len(CITIES)


def test_latest_date_reports_the_maximum(con):
    assert warehouse.latest_date(con) is None
    warehouse.load_records(con, rows())
    assert warehouse.latest_date(con) == date(2026, 8, 3)


def test_missing_dates_finds_the_gap(con):
    warehouse.load_records(con, rows(start_day=1, n=2))  # 01, 02
    warehouse.load_records(con, rows(start_day=5, n=1))  # 05
    gaps = warehouse.missing_dates(con, date(2026, 8, 1), date(2026, 8, 5), "tun")
    assert gaps == [date(2026, 8, 3), date(2026, 8, 4)]


def test_gold_tables_aggregate_by_month(con):
    warehouse.load_records(con, rows(n=3))
    warehouse.build_gold(con)
    row = con.execute(
        "SELECT days_observed, avg_temp_mean FROM gold_monthly_city WHERE city_id = 'tun'"
    ).fetchone()
    assert row[0] == 3
    assert row[1] == pytest.approx(29.0)


def test_gold_summary_computes_diurnal_range(con):
    warehouse.load_records(con, rows(n=3))
    warehouse.build_gold(con)
    value = con.execute(
        "SELECT avg_diurnal_range FROM gold_city_summary WHERE city_id = 'tun'"
    ).fetchone()[0]
    assert value == pytest.approx(12.0)


def test_gold_rebuild_is_repeatable(con):
    warehouse.load_records(con, rows())
    warehouse.build_gold(con)
    warehouse.build_gold(con)
    assert con.execute("SELECT count(*) FROM gold_monthly_city").fetchone()[0] == 1
