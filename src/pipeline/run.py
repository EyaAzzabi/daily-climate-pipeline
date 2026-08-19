"""Pipeline entrypoint: ingest -> validate -> load -> derive -> report.

Ordering is the whole design. Validation happens *before* the load, so a failing
blocking check leaves the warehouse exactly as it was rather than requiring a
rollback of partially written data.
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import quality, sources, warehouse
from .config import BRONZE_DIR, CITIES, REPORTS_DIR


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def write_bronze(records: list[dict], start: date, end: date) -> Path:
    """Persist the raw rows before anything transforms them.

    If a transformation turns out to be wrong, the raw landing zone is what makes
    it possible to reprocess without re-requesting the source -- which matters
    doubly when the source only keeps a limited history.
    """
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    path = BRONZE_DIR / f"weather_{start.isoformat()}_{end.isoformat()}.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")
    return path


def ingest(start: date, end: date) -> tuple[list[dict], list[str]]:
    """Fetch every city. One city failing must not lose the other nine."""
    records: list[dict] = []
    failures: list[str] = []
    for city in CITIES:
        try:
            payload = sources.fetch_city(city, start, end)
            records.extend(sources.to_records(city, payload))
        except sources.SourceError as exc:
            failures.append(f"{city.name}: {exc}")
            print(f"  ! {city.name} failed: {exc}", file=sys.stderr)
        else:
            print(f"  . {city.name}")
    return records, failures


def run(start: date, end: date, db_path: Path | None = None) -> int:
    run_id = uuid.uuid4().hex[:12]
    started = datetime.now(timezone.utc)
    print(f"run {run_id}: {start} to {end}")

    con = warehouse.connect(db_path)
    warehouse.upsert_cities(con, CITIES)

    records, failures = ingest(start, end)
    if records:
        print(f"  bronze -> {write_bronze(records, start, end)}")

    report = quality.validate(records, expected_cities=len(CITIES))
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "latest_quality.json").write_text(
        json.dumps(report.to_dict(), indent=2), encoding="utf-8"
    )

    for result in report.results:
        if not result.passed:
            marker = "BLOCK" if result.blocking else "warn "
            print(f"  [{marker}] {result.name}: {result.detail}")

    status, rows, message = "failed", 0, ""
    if not report.passed:
        blocking = [r.name for r in report.results if r.blocking and not r.passed]
        message = f"blocked by {', '.join(blocking)}"
        print(f"  quality gate FAILED: {message}")
    else:
        rows = warehouse.load_records(con, records)
        warehouse.build_gold(con)
        status = "succeeded"
        message = "; ".join(failures) if failures else "ok"
        print(f"  loaded {rows} rows, gold tables rebuilt")

    con.execute(
        """
        INSERT INTO run_log (run_id, started_at, finished_at, target_start, target_end,
                             rows_loaded, status, quality_passed, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            started,
            datetime.now(timezone.utc),
            start,
            end,
            rows,
            status,
            report.passed,
            message[:500],
        ],
    )

    latest = warehouse.latest_date(con)
    lag = quality.freshness_days(latest)
    print(f"  warehouse latest: {latest} (lag {lag} days)")
    con.close()

    # Non-zero exit so a scheduled run shows up red rather than quietly green.
    return 0 if report.passed else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=parse_date, help="YYYY-MM-DD, defaults to --end")
    parser.add_argument(
        "--end",
        type=parse_date,
        help="YYYY-MM-DD, defaults to the most recent settled archive date",
    )
    parser.add_argument(
        "--days", type=int, default=1, help="number of days back from --end (default 1)"
    )
    parser.add_argument("--db", type=Path, default=None)
    args = parser.parse_args(argv)

    end = args.end or sources.target_date()
    start = args.start or (end - timedelta(days=args.days - 1))
    return run(start, end, args.db)


if __name__ == "__main__":
    raise SystemExit(main())
