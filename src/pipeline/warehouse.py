"""DuckDB warehouse: schema, idempotent load, and derived tables.

The single most important property here is that **running the pipeline twice for
the same day changes nothing**. Scheduled jobs get retried, backfilled, and run
manually by someone checking whether they work; if any of those double-counts, the
warehouse is wrong in a way that is very hard to detect after the fact.

Idempotency is achieved by deleting the target partition before inserting it,
inside a transaction. DuckDB has no MERGE, and delete-then-insert on a natural key
is the standard equivalent.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from .config import FACTS_CSV, WAREHOUSE_PATH

SCHEMA = """
CREATE TABLE IF NOT EXISTS fact_daily_weather (
    city_id             VARCHAR NOT NULL,
    observed_date       DATE    NOT NULL,
    temperature_2m_max  DOUBLE,
    temperature_2m_min  DOUBLE,
    temperature_2m_mean DOUBLE,
    precipitation_sum   DOUBLE,
    windspeed_10m_max   DOUBLE,
    loaded_at           TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (city_id, observed_date)
);

CREATE TABLE IF NOT EXISTS dim_city (
    city_id   VARCHAR PRIMARY KEY,
    city_name VARCHAR NOT NULL,
    country   VARCHAR NOT NULL,
    latitude  DOUBLE  NOT NULL,
    longitude DOUBLE  NOT NULL
);

-- Every run leaves a trace, successful or not. A pipeline without a run log can
-- tell you what the data says but not whether it should be trusted.
CREATE TABLE IF NOT EXISTS run_log (
    run_id        VARCHAR PRIMARY KEY,
    started_at    TIMESTAMP NOT NULL,
    finished_at   TIMESTAMP,
    target_start  DATE,
    target_end    DATE,
    rows_loaded   INTEGER,
    status        VARCHAR NOT NULL,
    quality_passed BOOLEAN,
    message       VARCHAR
);
"""


def connect(path: Path | None = None) -> duckdb.DuckDBPyConnection:
    path = Path(path or WAREHOUSE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(SCHEMA)
    return con


def upsert_cities(con: duckdb.DuckDBPyConnection, cities) -> None:
    con.executemany(
        """
        INSERT INTO dim_city (city_id, city_name, country, latitude, longitude)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (city_id) DO UPDATE SET
            city_name = excluded.city_name,
            country   = excluded.country,
            latitude  = excluded.latitude,
            longitude = excluded.longitude
        """,
        [(c.city_id, c.name, c.country, c.latitude, c.longitude) for c in cities],
    )


def load_records(con: duckdb.DuckDBPyConnection, records: list[dict]) -> int:
    """Idempotently load daily observations.

    Returns the number of rows written. Re-running with the same input returns the
    same count and leaves the table identical.
    """
    if not records:
        return 0

    keys = sorted({(r["city_id"], r["observed_date"]) for r in records})
    rows = [
        (
            r["city_id"],
            r["observed_date"],
            r.get("temperature_2m_max"),
            r.get("temperature_2m_min"),
            r.get("temperature_2m_mean"),
            r.get("precipitation_sum"),
            r.get("windspeed_10m_max"),
        )
        for r in records
    ]

    con.execute("BEGIN TRANSACTION")
    try:
        con.executemany(
            "DELETE FROM fact_daily_weather WHERE city_id = ? AND observed_date = ?",
            keys,
        )
        con.executemany(
            """
            INSERT INTO fact_daily_weather (
                city_id, observed_date, temperature_2m_max, temperature_2m_min,
                temperature_2m_mean, precipitation_sum, windspeed_10m_max
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return len(rows)


def latest_date(con: duckdb.DuckDBPyConnection) -> date | None:
    row = con.execute("SELECT max(observed_date) FROM fact_daily_weather").fetchone()
    return row[0] if row and row[0] else None


def missing_dates(
    con: duckdb.DuckDBPyConnection, start: date, end: date, city_id: str
) -> list[date]:
    """Dates in the range with no row for this city -- the gaps a backfill targets."""
    rows = con.execute(
        """
        WITH expected AS (
            SELECT unnest(generate_series(?::DATE, ?::DATE, INTERVAL 1 DAY))::DATE AS d
        )
        SELECT e.d
        FROM expected e
        LEFT JOIN fact_daily_weather f
          ON f.observed_date = e.d AND f.city_id = ?
        WHERE f.city_id IS NULL
        ORDER BY e.d
        """,
        [start, end, city_id],
    ).fetchall()
    return [r[0] for r in rows]


def build_gold(con: duckdb.DuckDBPyConnection) -> None:
    """Derived tables the README charts and any downstream consumer read from.

    Rebuilt in full on every run. The fact table is small enough that incremental
    maintenance would add failure modes without buying meaningful time.
    """
    con.execute("DROP TABLE IF EXISTS gold_monthly_city")
    con.execute(
        """
        CREATE TABLE gold_monthly_city AS
        SELECT
            f.city_id,
            c.city_name,
            c.country,
            date_trunc('month', f.observed_date)         AS month,
            count(*)                                     AS days_observed,
            round(avg(f.temperature_2m_mean), 2)         AS avg_temp_mean,
            round(max(f.temperature_2m_max), 2)          AS max_temp,
            round(min(f.temperature_2m_min), 2)          AS min_temp,
            round(sum(f.precipitation_sum), 2)           AS total_precipitation,
            round(avg(f.windspeed_10m_max), 2)           AS avg_max_windspeed
        FROM fact_daily_weather f
        JOIN dim_city c USING (city_id)
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 4
        """
    )

    con.execute("DROP TABLE IF EXISTS gold_city_summary")
    con.execute(
        """
        CREATE TABLE gold_city_summary AS
        SELECT
            c.city_id,
            c.city_name,
            c.country,
            count(*)                              AS days_observed,
            min(f.observed_date)                  AS first_date,
            max(f.observed_date)                  AS last_date,
            round(avg(f.temperature_2m_mean), 2)  AS avg_temp_mean,
            -- Diurnal range: a climate signal that also happens to be a good
            -- canary, since swapped or misaligned columns make it go negative.
            round(avg(f.temperature_2m_max - f.temperature_2m_min), 2) AS avg_diurnal_range,
            round(sum(f.precipitation_sum), 2)    AS total_precipitation
        FROM fact_daily_weather f
        JOIN dim_city c USING (city_id)
        GROUP BY 1, 2, 3
        ORDER BY avg_temp_mean DESC
        """
    )


FACT_COLUMNS = (
    "city_id, observed_date, temperature_2m_max, temperature_2m_min, "
    "temperature_2m_mean, precipitation_sum, windspeed_10m_max"
)


def export_facts(con: duckdb.DuckDBPyConnection, path: Path | None = None) -> int:
    """Write the fact table to a deterministically ordered CSV.

    `loaded_at` is deliberately excluded. It changes on every run, so including it
    would mark every row dirty in git even when no observation changed -- turning a
    three-line diff into a full-file rewrite.
    """
    path = Path(path or FACTS_CSV)
    path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT {FACT_COLUMNS} FROM fact_daily_weather "
        f"ORDER BY city_id, observed_date) TO '{path.as_posix()}' (HEADER, DELIMITER ',')"
    )
    return con.execute("SELECT count(*) FROM fact_daily_weather").fetchone()[0]


def import_facts(con: duckdb.DuckDBPyConnection, path: Path | None = None) -> int:
    """Rehydrate the fact table from the committed CSV.

    Lets a fresh clone rebuild the warehouse without re-requesting the API, which
    matters because the archive only serves a bounded history.
    """
    path = Path(path or FACTS_CSV)
    if not path.exists():
        return 0
    con.execute(
        f"INSERT OR REPLACE INTO fact_daily_weather ({FACT_COLUMNS}) "
        f"SELECT {FACT_COLUMNS} FROM read_csv_auto('{path.as_posix()}')"
    )
    return con.execute("SELECT count(*) FROM fact_daily_weather").fetchone()[0]
