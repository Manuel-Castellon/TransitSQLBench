"""Tests for spatialbench.queries.cli."""

import zipfile
from collections.abc import Generator
from pathlib import Path

import pytest

from spatialbench.data.load import load
from spatialbench.queries.cli import (
    _print_tier2,
    _print_tier3,
    _print_tier4,
    _print_tier5,
    main,
)
from spatialbench.queries.reference import (
    RouteAvgGap,
    RoutePair,
    StopWithDistance,
    Tier2Result,
    Tier3Result,
    Tier4Result,
    Tier5Result,
)
from tests.queries.test_reference import (
    CALENDAR_CSV,
    CALENDAR_DATES_CSV,
    ROUTES_CSV,
    STOP_TIMES_CSV,
    STOPS_CSV,
    TRIPS_CSV,
)


@pytest.fixture
def db_path(tmp_path: Path) -> Generator[Path, None, None]:
    files = {
        "stops.txt": STOPS_CSV,
        "routes.txt": ROUTES_CSV,
        "trips.txt": TRIPS_CSV,
        "stop_times.txt": STOP_TIMES_CSV,
        "calendar.txt": CALENDAR_CSV,
        "calendar_dates.txt": CALENDAR_DATES_CSV,
    }
    zp = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(zp, "w") as z:
        for name, content in files.items():
            z.writestr(name, content)
    db = tmp_path / "out.duckdb"
    load(zp, db)
    yield db


# ── _connect failure path ────────────────────────────────────────────────────


def test_main_exits_when_db_missing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    missing = tmp_path / "nope.duckdb"
    with pytest.raises(SystemExit) as exc:
        main(["--db", str(missing), "tier1", "--route", "R1"])
    assert exc.value.code == 1
    assert "not found" in capsys.readouterr().err


# ── one e2e test per subcommand ──────────────────────────────────────────────


def test_main_tier1(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "tier1", "--route", "R4"])
    out = capsys.readouterr().out
    assert "Route R4" in out
    assert "3 distinct" in out


def test_main_tier2(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main([
        "--db", str(db_path),
        "tier2", "--lat", "32.0", "--lon", "34.81", "--radius", "50",
    ])  # fmt: skip
    out = capsys.readouterr().out
    assert "s2" in out and "s3" in out


def test_main_tier3(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "tier3", "--limit", "10"])
    out = capsys.readouterr().out
    assert "R1" in out and "R4" in out and "shared=2" in out


def test_main_tier4_straight_line(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "tier4", "--limit", "10"])
    out = capsys.readouterr().out
    assert "straight-line" in out
    assert "R5" in out


def test_main_tier5(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "tier5", "--stop", "s1", "--walking", "400"])
    out = capsys.readouterr().out
    assert "From stop s1" in out
    assert "s4" in out and "s6" in out


def test_main_all(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "all", "--stop", "s1", "--route", "R4"])
    out = capsys.readouterr().out
    assert "Tier 1" in out and "Tier 2" in out and "Tier 3" in out
    assert "Tier 4" in out and "Tier 5" in out
    assert "Route R4: 3" in out


# ── printer empty-branches + Tier4 shape-dist branch ─────────────────────────


def test_print_tier2_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _print_tier2(Tier2Result(stops=[]), lat=0.0, lon=0.0, radius_m=10.0)
    assert "(none)" in capsys.readouterr().out


def test_print_tier2_with_stop(capsys: pytest.CaptureFixture[str]) -> None:
    _print_tier2(
        Tier2Result(stops=[StopWithDistance(stop_id="X", stop_name="N", distance_m=12.3)]),
        lat=0.0,
        lon=0.0,
        radius_m=100.0,
    )
    out = capsys.readouterr().out
    assert "X" in out and "12.3" in out


def test_print_tier3_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _print_tier3(Tier3Result(pairs=[]))
    assert "(none)" in capsys.readouterr().out


def test_print_tier3_with_pair(capsys: pytest.CaptureFixture[str]) -> None:
    _print_tier3(Tier3Result(pairs=[RoutePair(route_a="A", route_b="B", shared_stops=7)]))
    assert "shared=7" in capsys.readouterr().out


def test_print_tier4_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _print_tier4(Tier4Result(routes=[], used_shape_dist=True))
    out = capsys.readouterr().out
    assert "shape_dist_traveled" in out
    assert "(none)" in out


def test_print_tier4_shape_dist_branch(capsys: pytest.CaptureFixture[str]) -> None:
    _print_tier4(
        Tier4Result(
            routes=[RouteAvgGap(route_id="R", avg_gap_m=999.5)],
            used_shape_dist=True,
        )
    )
    out = capsys.readouterr().out
    assert "shape_dist_traveled" in out
    assert "999.5" in out


def test_print_tier5_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _print_tier5(Tier5Result(origin_stop_id="X", walking_distance_m=400.0, reachable_stop_ids=[]))
    out = capsys.readouterr().out
    assert "From stop X" in out
    assert "(none)" in out
