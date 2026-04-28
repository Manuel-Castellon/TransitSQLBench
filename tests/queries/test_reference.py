"""Tests for spatialbench.queries.reference.

Strategy: build a small synthetic GTFS zip, run load() to get a real DuckDB
spatialbench database, then exercise each tier function against it. Distances
are asserted with tolerance because they come out of the EPSG:2039 transform.
"""

import zipfile
from collections.abc import Generator
from pathlib import Path

import duckdb
import pytest

from spatialbench.data.load import load
from spatialbench.queries.reference import (
    tier1_route_stops_on_weekdays,
    tier2_stops_within_radius,
    tier3_route_pairs_sharing_stops,
    tier4_route_consecutive_stop_gaps,
    tier5_reachable_with_one_transfer,
)

# ── fixture geometry ─────────────────────────────────────────────────────────
# All stops near (32.0°N, 34.8°E), Tel Aviv area.
#   s1 (32.000,  34.800)   anchor
#   s2 (32.000,  34.810)   ~1110 m east of s1
#   s3 (32.0001, 34.8101)  ~14 m from s2 ("across the street")
#   s4 (32.000,  34.820)   ~1110 m east of s2
#   s5 (32.020,  34.800)   ~1832 m north of s1, isolated
#   s6 (32.000,  34.830)   ~1110 m east of s4
#
# The s2/s3 pair is the discriminating geometry for Tier 5 walking transfers.

STOPS_CSV = (
    "stop_id,stop_name,stop_lat,stop_lon\n"
    "s1,Anchor,32.0000,34.8000\n"
    "s2,East-A,32.0000,34.8100\n"
    "s3,East-A-Across,32.0001,34.8101\n"
    "s4,East-B,32.0000,34.8200\n"
    "s5,North,32.0200,34.8000\n"
    "s6,East-C,32.0000,34.8300\n"
)
ROUTES_CSV = (
    "route_id,agency_id,route_short_name,route_long_name,route_type\n"
    "R1,A,1,One,3\n"
    "R2,A,2,Two,3\n"
    "R3,A,3,Three,3\n"
    "R4,A,4,Four,3\n"
    "R5,A,5,Five,3\n"
)
TRIPS_CSV = (
    "route_id,service_id,trip_id\n"
    "R1,SVC_WD,T1\n"
    "R2,SVC_WD,T2\n"
    "R3,SVC_WE,T3\n"
    "R4,SVC_WD,T4\n"
    "R5,SVC_WD,T5\n"
)
STOP_TIMES_CSV = (
    "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
    # T1: s1 → s2
    "T1,07:00:00,07:00:00,s1,1\n"
    "T1,07:05:00,07:05:00,s2,2\n"
    # T2: s3 → s4
    "T2,08:00:00,08:00:00,s3,1\n"
    "T2,08:05:00,08:05:00,s4,2\n"
    # T3: s1 → s5  (weekend)
    "T3,09:00:00,09:00:00,s1,1\n"
    "T3,09:10:00,09:10:00,s5,2\n"
    # T4: s1 → s2 → s4
    "T4,10:00:00,10:00:00,s1,1\n"
    "T4,10:05:00,10:05:00,s2,2\n"
    "T4,10:10:00,10:10:00,s4,3\n"
    # T5: s3 → s6
    "T5,11:00:00,11:00:00,s3,1\n"
    "T5,11:10:00,11:10:00,s6,2\n"
)
CALENDAR_CSV = (
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
    "SVC_WD,1,1,1,1,1,0,0,20260101,20261231\n"
    "SVC_WE,0,0,0,0,0,1,1,20260101,20261231\n"
)
CALENDAR_DATES_CSV = "service_id,date,exception_type\nSVC_WD,20260501,2\n"


def _make_zip(tmp_path: Path, files: dict[str, str]) -> Path:
    zp = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(zp, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)
    return zp


@pytest.fixture
def db(tmp_path: Path) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    files = {
        "stops.txt": STOPS_CSV,
        "routes.txt": ROUTES_CSV,
        "trips.txt": TRIPS_CSV,
        "stop_times.txt": STOP_TIMES_CSV,
        "calendar.txt": CALENDAR_CSV,
        "calendar_dates.txt": CALENDAR_DATES_CSV,
    }
    zp = _make_zip(tmp_path, files)
    db_path = tmp_path / "out.duckdb"
    load(zp, db_path)
    con = duckdb.connect(str(db_path))
    con.execute("LOAD spatial;")
    yield con
    con.close()


# ── Tier 1 ───────────────────────────────────────────────────────────────────


def test_tier1_weekday_route(db: duckdb.DuckDBPyConnection) -> None:
    r = tier1_route_stops_on_weekdays(db, "R1")
    assert r.route_id == "R1"
    assert r.n_stops == 2  # s1, s2


def test_tier1_multi_stop_weekday_route(db: duckdb.DuckDBPyConnection) -> None:
    r = tier1_route_stops_on_weekdays(db, "R4")
    assert r.n_stops == 3  # s1, s2, s4


def test_tier1_weekend_only_route_returns_zero(db: duckdb.DuckDBPyConnection) -> None:
    r = tier1_route_stops_on_weekdays(db, "R3")
    assert r.n_stops == 0


def test_tier1_unknown_route(db: duckdb.DuckDBPyConnection) -> None:
    r = tier1_route_stops_on_weekdays(db, "DOES_NOT_EXIST")
    assert r.n_stops == 0


# ── Tier 2 ───────────────────────────────────────────────────────────────────


def test_tier2_tight_radius_returns_anchor_only(db: duckdb.DuckDBPyConnection) -> None:
    r = tier2_stops_within_radius(db, lat=32.0000, lon=34.8000, radius_m=200.0)
    assert [s.stop_id for s in r.stops] == ["s1"]
    assert r.stops[0].distance_m == pytest.approx(0.0, abs=0.01)


def test_tier2_picks_up_close_neighbour(db: duckdb.DuckDBPyConnection) -> None:
    # Query at s2's coordinates, 50 m radius. s3 is ~14 m away, others > 1 km.
    r = tier2_stops_within_radius(db, lat=32.0000, lon=34.8100, radius_m=50.0)
    ids = [s.stop_id for s in r.stops]
    assert ids == ["s2", "s3"]
    assert r.stops[0].distance_m == pytest.approx(0.0, abs=0.01)
    assert r.stops[1].distance_m == pytest.approx(14.4, abs=1.0)


def test_tier2_radius_zero_returns_empty(db: duckdb.DuckDBPyConnection) -> None:
    # Query somewhere with nothing nearby.
    r = tier2_stops_within_radius(db, lat=33.0, lon=35.0, radius_m=100.0)
    assert r.stops == []


# ── Tier 3 ───────────────────────────────────────────────────────────────────


def test_tier3_pairs_ranked_by_shared_stops(db: duckdb.DuckDBPyConnection) -> None:
    r = tier3_route_pairs_sharing_stops(db, limit=10)
    pairs = [(p.route_a, p.route_b, p.shared_stops) for p in r.pairs]
    # Top pair: R1 & R4 share {s1, s2} = 2.
    assert pairs[0] == ("R1", "R4", 2)
    # Then four pairs of size 1, broken by (route_a, route_b) lexicographic.
    assert sorted(pairs[1:]) == [
        ("R1", "R3", 1),
        ("R2", "R4", 1),
        ("R2", "R5", 1),
        ("R3", "R4", 1),
    ]


def test_tier3_respects_limit(db: duckdb.DuckDBPyConnection) -> None:
    r = tier3_route_pairs_sharing_stops(db, limit=1)
    assert len(r.pairs) == 1
    assert r.pairs[0].shared_stops == 2


# ── Tier 4 ───────────────────────────────────────────────────────────────────


def test_tier4_straight_line_ranks_routes_by_avg_gap(db: duckdb.DuckDBPyConnection) -> None:
    r = tier4_route_consecutive_stop_gaps(db, use_shape_dist=False, limit=10)
    assert r.used_shape_dist is False
    routes = [g.route_id for g in r.routes]
    # R5 (~2210m) and R3 (~1832m) lead. R1 and R4 both average ~1110.6m so
    # their order is a floating-point tie — assert as a set in positions 2-3.
    # R2 (~1099.5m) is last.
    assert routes[0] == "R5"
    assert routes[1] == "R3"
    assert set(routes[2:4]) == {"R1", "R4"}
    assert routes[4] == "R2"
    gaps = {g.route_id: g.avg_gap_m for g in r.routes}
    assert gaps["R5"] == pytest.approx(2210, abs=5)
    assert gaps["R3"] == pytest.approx(1832, abs=5)
    assert gaps["R1"] == pytest.approx(1110.6, abs=1)
    assert gaps["R4"] == pytest.approx(1110.6, abs=1)
    assert gaps["R2"] == pytest.approx(1099.5, abs=1)


def test_tier4_with_shape_dist_uses_column(tmp_path: Path) -> None:
    # Minimal fixture with shape_dist_traveled populated.
    stop_times = (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled\n"
        "T1,07:00:00,07:00:00,s1,1,0.0\n"
        "T1,07:05:00,07:05:00,s2,2,1000.0\n"
        "T1,07:10:00,07:10:00,s4,3,2500.0\n"
    )
    files = {
        "stops.txt": STOPS_CSV,
        "routes.txt": (
            "route_id,agency_id,route_short_name,route_long_name,route_type\nR1,A,1,One,3\n"
        ),
        "trips.txt": "route_id,service_id,trip_id\nR1,SVC_WD,T1\n",
        "stop_times.txt": stop_times,
        "calendar.txt": CALENDAR_CSV,
    }
    zp = _make_zip(tmp_path, files)
    db_path = tmp_path / "shape.duckdb"
    load(zp, db_path)
    con = duckdb.connect(str(db_path))
    con.execute("LOAD spatial;")
    try:
        r = tier4_route_consecutive_stop_gaps(con, use_shape_dist=True, limit=5)
    finally:
        con.close()
    assert r.used_shape_dist is True
    assert len(r.routes) == 1
    # Legs of 1000 and 1500 → avg 1250 (independent of geometry).
    assert r.routes[0].route_id == "R1"
    assert r.routes[0].avg_gap_m == pytest.approx(1250.0, abs=0.01)


# ── Tier 5 ───────────────────────────────────────────────────────────────────


def test_tier5_walking_transfer_unlocks_extra_stop(db: duckdb.DuckDBPyConnection) -> None:
    # 400 m walking radius bridges s2↔s3, so a passenger on T1/T4 can walk to
    # s3 and board T2 (→ s4) or T5 (→ s6).
    r = tier5_reachable_with_one_transfer(db, origin_stop_id="s1", walking_distance_m=400.0)
    assert r.origin_stop_id == "s1"
    assert r.walking_distance_m == 400.0
    assert r.reachable_stop_ids == ["s4", "s6"]


def test_tier5_naive_zero_walking_misses_walking_leg(db: duckdb.DuckDBPyConnection) -> None:
    # Same-stop-only transfers: cannot reach s6 because no trip from s2 reaches
    # it — only from s3. This is the *whole point* of the benchmark.
    r = tier5_reachable_with_one_transfer(db, origin_stop_id="s1", walking_distance_m=1.0)
    assert r.reachable_stop_ids == ["s4"]


def test_tier5_unknown_origin_returns_empty(db: duckdb.DuckDBPyConnection) -> None:
    r = tier5_reachable_with_one_transfer(db, origin_stop_id="DOES_NOT_EXIST")
    assert r.reachable_stop_ids == []


def test_tier5_default_walking_distance(db: duckdb.DuckDBPyConnection) -> None:
    # Default 400 m matches the explicit-400 m result.
    explicit = tier5_reachable_with_one_transfer(db, origin_stop_id="s1", walking_distance_m=400.0)
    default = tier5_reachable_with_one_transfer(db, origin_stop_id="s1")
    assert default.reachable_stop_ids == explicit.reachable_stop_ids
    assert default.walking_distance_m == 400.0
