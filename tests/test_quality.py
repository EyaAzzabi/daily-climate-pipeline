from datetime import date

from pipeline import quality


def record(**overrides):
    base = {
        "city_id": "tun",
        "observed_date": "2026-08-01",
        "temperature_2m_max": 34.0,
        "temperature_2m_min": 22.0,
        "temperature_2m_mean": 28.0,
        "precipitation_sum": 0.0,
        "windspeed_10m_max": 18.0,
    }
    base.update(overrides)
    return base


def named(report, name):
    return next(r for r in report.results if r.name == name)


def test_clean_batch_passes():
    report = quality.validate([record(), record(city_id="alg")])
    assert report.passed
    assert report.row_count == 2


def test_empty_batch_is_blocking():
    report = quality.validate([])
    assert not report.passed
    assert named(report, "not_empty").blocking


def test_duplicate_city_date_blocks_the_load():
    report = quality.validate([record(), record()])
    assert not report.passed
    assert named(report, "unique_city_date").failing_rows == 1


def test_same_date_different_cities_is_not_a_duplicate():
    report = quality.validate([record(), record(city_id="cai")])
    assert named(report, "unique_city_date").passed


def test_out_of_range_temperature_blocks():
    # 400 C is a unit error or a broken sensor, not weather.
    report = quality.validate([record(temperature_2m_max=400.0)])
    assert not report.passed
    assert named(report, "range_temperature_2m_max").failing_rows == 1


def test_negative_precipitation_blocks():
    report = quality.validate([record(precipitation_sum=-5.0)])
    assert not report.passed


def test_max_below_min_blocks():
    report = quality.validate([record(temperature_2m_max=10.0, temperature_2m_min=30.0)])
    assert not report.passed
    assert named(report, "max_not_below_min").failing_rows == 1


def test_nulls_warn_but_do_not_block():
    records = [record(city_id=f"c{i}", precipitation_sum=None) for i in range(10)]
    report = quality.validate(records)
    assert report.passed, "null columns must not block the whole load"
    assert not named(report, "nulls_precipitation_sum").passed
    assert report.warnings


def test_nulls_are_ignored_by_range_checks():
    report = quality.validate([record(temperature_2m_max=None)])
    assert named(report, "range_temperature_2m_max").passed


def test_missing_city_is_a_warning():
    report = quality.validate([record()], expected_cities=10)
    assert report.passed
    assert named(report, "completeness_cities").failing_rows == 9


def test_freshness_counts_days_behind():
    assert quality.freshness_days(date(2026, 8, 1), today=date(2026, 8, 7)) == 6
    assert quality.freshness_days(None) is None


def test_report_serialises_for_the_dashboard():
    payload = quality.validate([record()]).to_dict()
    assert payload["passed"] is True
    assert {"name", "passed", "blocking", "detail"} <= set(payload["results"][0])
