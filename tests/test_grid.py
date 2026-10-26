"""
build_target.py ciktisini kontrol eder.
Calistirma (proje kokunde):  python -m pytest tests -v
"""
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
TARGET = (ROOT / "data" / "processed" / "target.parquet").as_posix()
AGG = (ROOT / "data" / "agg" / "*.parquet").as_posix()


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{TARGET}')").fetchall()
    c.execute(f"CREATE VIEW a AS SELECT * FROM read_parquet('{AGG}')").fetchall()
    yield c
    c.close()


def one(con, sql):
    return con.execute(sql).fetchall()[0][0]


def test_grid_is_complete(con):
    zones = one(con, "SELECT count(DISTINCT zone) FROM t")
    hours = one(con, "SELECT count(DISTINCT hour) FROM t")
    rows = one(con, "SELECT count(*) FROM t")
    assert rows == zones * hours


def test_hours_are_contiguous(con):
    hours = one(con, "SELECT count(DISTINCT hour) FROM t")
    span = one(con, "SELECT date_diff('hour', min(hour), max(hour)) + 1 FROM t")
    assert hours == span


def test_no_duplicate_zone_hour(con):
    assert one(con, "SELECT count(*) FROM (SELECT zone, hour FROM t GROUP BY 1,2 HAVING count(*) > 1)") == 0


def test_excluded_zones_absent(con):
    assert one(con, "SELECT count(*) FROM t WHERE zone IN (264, 265)") == 0


def test_no_negative_and_null_only_on_dst_gap(con):
    assert one(con, "SELECT count(*) FROM t WHERE trips < 0") == 0
    assert one(con, "SELECT count(*) FROM t WHERE trips IS NULL AND NOT is_dst_gap") == 0
    assert one(con, "SELECT count(*) FROM t WHERE trips IS NOT NULL AND is_dst_gap") == 0


def test_trips_model_dst_rules(con):
    # Bosluk saati: model kolonu da NULL
    assert one(con, "SELECT count(*) FROM t WHERE is_dst_gap AND trips_model IS NOT NULL") == 0
    # Cift saat: model kolonu = ham / 2
    assert one(con, "SELECT count(*) FROM t WHERE is_dst_dup AND abs(trips_model - trips / 2.0) > 1e-9") == 0
    # Diger tum saatler: model kolonu = ham
    assert one(con, "SELECT count(*) FROM t WHERE NOT is_dst_gap AND NOT is_dst_dup AND trips_model <> trips") == 0


def test_trip_total_preserved(con):
    agg_sum = one(con, "SELECT sum(trips) FROM a")
    gap_sum = one(con, "SELECT coalesce(sum(a.trips),0) FROM a JOIN (SELECT DISTINCT hour FROM t WHERE is_dst_gap) g USING (hour)")
    assert one(con, "SELECT sum(trips) FROM t") == agg_sum - gap_sum
