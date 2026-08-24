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


def test_facts_survive_a_csv_round_trip(con, tmp_path):
    """The committed CSV must be able to rebuild the warehouse exactly.

    The DuckDB file is gitignored, so this CSV is the only durable record of the
    history. If the round trip loses or alters a row, the history is gone.
    """
    warehouse.load_records(con, rows(n=3))
    warehouse.load_records(con, rows(city_id="alg", n=2))
    csv_path = tmp_path / "facts.csv"
    exported = warehouse.export_facts(con, csv_path)
    assert exported == 5
    assert csv_path.exists()

    fresh = warehouse.connect(tmp_path / "fresh.duckdb")
    warehouse.upsert_cities(fresh, CITIES)
    restored = warehouse.import_facts(fresh, csv_path)
    assert restored == 5

    original = con.execute(
        f"SELECT {warehouse.FACT_COLUMNS} FROM fact_daily_weather ORDER BY 1, 2"
    ).fetchall()
    rebuilt = fresh.execute(
        f"SELECT {warehouse.FACT_COLUMNS} FROM fact_daily_weather ORDER BY 1, 2"
    ).fetchall()
    fresh.close()
    assert original == rebuilt


def test_export_excludes_loaded_at(con, tmp_path):
    """loaded_at changes every run; including it would dirty every row in git."""
    warehouse.load_records(con, rows(n=2))
    csv_path = tmp_path / "facts.csv"
    warehouse.export_facts(con, csv_path)
    header = csv_path.read_text(encoding="utf-8").splitlines()[0]
    assert "loaded_at" not in header
    assert header.startswith("city_id,observed_date")


def test_export_is_byte_stable_across_runs(con, tmp_path):
    """Same data must produce an identical file, or every run shows a spurious diff."""
    warehouse.load_records(con, rows(n=3))
    first = tmp_path / "a.csv"
    second = tmp_path / "b.csv"
    warehouse.export_facts(con, first)
    warehouse.export_facts(con, second)
    assert first.read_bytes() == second.read_bytes()


def test_import_is_idempotent(con, tmp_path):
    warehouse.load_records(con, rows(n=3))
    csv_path = tmp_path / "facts.csv"
    warehouse.export_facts(con, csv_path)
    warehouse.import_facts(con, csv_path)
    warehouse.import_facts(con, csv_path)
    assert count(con) == 3


def test_import_of_a_missing_file_is_a_no_op(con, tmp_path):
    assert warehouse.import_facts(con, tmp_path / "absent.csv") == 0
