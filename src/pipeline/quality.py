"""Data quality checks run at the boundary between raw and modelled data.

The rule this module exists to enforce: bad data must never reach the warehouse
silently. Every check returns a result, every result is written to a report, and
checks marked `blocking` stop the load.

Checks are deliberately plain Python rather than a framework. The point is to show
what a data contract *is* -- a set of assertions someone decided on, with a
consequence attached to each -- not to demonstrate a library.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from .config import DAILY_VARIABLES

# Physically plausible ranges. Deliberately wide: these catch unit changes, sensor
# faults and schema drift, not unusual weather. A check that fires on a hot day is
# a check that gets ignored.
RANGES: dict[str, tuple[float, float]] = {
    "temperature_2m_max": (-60.0, 60.0),
    "temperature_2m_min": (-70.0, 50.0),
    "temperature_2m_mean": (-65.0, 55.0),
    "precipitation_sum": (0.0, 500.0),
    "windspeed_10m_max": (0.0, 200.0),
}

# Fraction of null values tolerated per column before the load is blocked.
NULL_TOLERANCE = 0.10


@dataclass
class CheckResult:
    name: str
    passed: bool
    blocking: bool
    detail: str
    failing_rows: int = 0


@dataclass
class QualityReport:
    checked_at: str
    row_count: int
    results: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results if r.blocking)

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and not r.blocking]

    def to_dict(self) -> dict:
        return {
            "checked_at": self.checked_at,
            "row_count": self.row_count,
            "passed": self.passed,
            "results": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "blocking": r.blocking,
                    "detail": r.detail,
                    "failing_rows": r.failing_rows,
                }
                for r in self.results
            ],
        }


def validate(records: list[dict], expected_cities: int | None = None) -> QualityReport:
    report = QualityReport(
        checked_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        row_count=len(records),
    )
    add = report.results.append

    add(
        CheckResult(
            name="not_empty",
            passed=bool(records),
            blocking=True,
            detail=f"{len(records)} rows received",
        )
    )
    if not records:
        return report

    # Uniqueness on the natural key. Without this, a re-run that appends instead of
    # upserting doubles every metric downstream, and averages quietly stay correct
    # while counts and sums do not -- the hardest class of bug to notice.
    keys = [(r["city_id"], r["observed_date"]) for r in records]
    duplicates = len(keys) - len(set(keys))
    add(
        CheckResult(
            name="unique_city_date",
            passed=duplicates == 0,
            blocking=True,
            detail=f"{duplicates} duplicate (city_id, observed_date) pairs",
            failing_rows=duplicates,
        )
    )

    for column, (low, high) in RANGES.items():
        out_of_range = [
            r for r in records if r.get(column) is not None and not low <= r[column] <= high
        ]
        add(
            CheckResult(
                name=f"range_{column}",
                passed=not out_of_range,
                blocking=True,
                detail=f"expected {low} to {high}; {len(out_of_range)} rows outside",
                failing_rows=len(out_of_range),
            )
        )

    for column in DAILY_VARIABLES:
        nulls = sum(1 for r in records if r.get(column) is None)
        ratio = nulls / len(records)
        add(
            CheckResult(
                name=f"nulls_{column}",
                passed=ratio <= NULL_TOLERANCE,
                # Non-blocking: the archive genuinely publishes gaps for some
                # stations, and refusing the whole load over one sparse column
                # would mean losing nine good cities to save one.
                blocking=False,
                detail=f"{nulls} nulls ({ratio:.1%}), tolerance {NULL_TOLERANCE:.0%}",
                failing_rows=nulls,
            )
        )

    # Internal consistency: a maximum below its minimum means the columns were
    # swapped somewhere upstream, which range checks alone cannot detect.
    inverted = [
        r
        for r in records
        if r.get("temperature_2m_max") is not None
        and r.get("temperature_2m_min") is not None
        and r["temperature_2m_max"] < r["temperature_2m_min"]
    ]
    add(
        CheckResult(
            name="max_not_below_min",
            passed=not inverted,
            blocking=True,
            detail=f"{len(inverted)} rows where max temperature is below min",
            failing_rows=len(inverted),
        )
    )

    if expected_cities is not None:
        seen = len({r["city_id"] for r in records})
        add(
            CheckResult(
                name="completeness_cities",
                passed=seen == expected_cities,
                blocking=False,
                detail=f"{seen} of {expected_cities} cities present",
                failing_rows=max(0, expected_cities - seen),
            )
        )

    return report


def freshness_days(latest: date | None, today: date | None = None) -> int | None:
    """How many days behind the warehouse is. None when it is empty."""
    if latest is None:
        return None
    return (( today or date.today()) - latest).days
