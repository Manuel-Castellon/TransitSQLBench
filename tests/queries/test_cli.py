"""Tests for transitsqlbench.queries.cli."""

import zipfile
from collections.abc import Generator
from pathlib import Path

import pytest

from tests.queries.test_reference import (
    CALENDAR_CSV,
    CALENDAR_DATES_CSV,
    ROUTES_CSV,
    STOP_TIMES_CSV,
    STOPS_CSV,
    TRIPS_CSV,
)
from transitsqlbench.data.load import load
from transitsqlbench.queries.cli import (
    _print_q2,
    _print_q3,
    _print_q4,
    _print_q5,
    main,
)
from transitsqlbench.queries.reference import (
    Q2Result,
    Q3Result,
    Q4Result,
    Q5Result,
    RouteAvgGap,
    RoutePair,
    StopWithDistance,
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
        main(["--db", str(missing), "q1", "--route", "R1"])
    assert exc.value.code == 1
    assert "not found" in capsys.readouterr().err


# ── one e2e test per subcommand ──────────────────────────────────────────────


def test_main_q1(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "q1", "--route", "R4"])
    out = capsys.readouterr().out
    assert "Route R4" in out
    assert "3 distinct" in out


def test_main_q2(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main([
        "--db", str(db_path),
        "q2", "--lat", "32.0", "--lon", "34.81", "--radius", "50",
    ])  # fmt: skip
    out = capsys.readouterr().out
    assert "s2" in out and "s3" in out


def test_main_q3(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "q3", "--limit", "10"])
    out = capsys.readouterr().out
    assert "R1" in out and "R4" in out and "shared=2" in out


def test_main_q4_straight_line(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "q4", "--limit", "10"])
    out = capsys.readouterr().out
    assert "straight-line" in out
    assert "R5" in out


def test_main_q5(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "q5", "--stop", "s1", "--walking", "400"])
    out = capsys.readouterr().out
    assert "From stop s1" in out
    assert "s4" in out and "s6" in out


def test_main_all(db_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    main(["--db", str(db_path), "all", "--stop", "s1", "--route", "R4"])
    out = capsys.readouterr().out
    assert "Q1" in out and "Q2" in out and "Q3" in out
    assert "Q4" in out and "Q5" in out
    assert "Route R4: 3" in out


# ── printer empty-branches + Q4 shape-dist branch ─────────────────────────


def test_print_q2_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _print_q2(Q2Result(stops=[]), lat=0.0, lon=0.0, radius_m=10.0)
    assert "(none)" in capsys.readouterr().out


def test_print_q2_with_stop(capsys: pytest.CaptureFixture[str]) -> None:
    _print_q2(
        Q2Result(stops=[StopWithDistance(stop_id="X", stop_name="N", distance_m=12.3)]),
        lat=0.0,
        lon=0.0,
        radius_m=100.0,
    )
    out = capsys.readouterr().out
    assert "X" in out and "12.3" in out


def test_print_q3_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _print_q3(Q3Result(pairs=[]))
    assert "(none)" in capsys.readouterr().out


def test_print_q3_with_pair(capsys: pytest.CaptureFixture[str]) -> None:
    _print_q3(Q3Result(pairs=[RoutePair(route_a="A", route_b="B", shared_stops=7)]))
    assert "shared=7" in capsys.readouterr().out


def test_print_q4_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _print_q4(Q4Result(routes=[], used_shape_dist=True))
    out = capsys.readouterr().out
    assert "shape_dist_traveled" in out
    assert "(none)" in out


def test_print_q4_shape_dist_branch(capsys: pytest.CaptureFixture[str]) -> None:
    _print_q4(
        Q4Result(
            routes=[RouteAvgGap(route_id="R", avg_gap_m=999.5)],
            used_shape_dist=True,
        )
    )
    out = capsys.readouterr().out
    assert "shape_dist_traveled" in out
    assert "999.5" in out


def test_print_q5_empty(capsys: pytest.CaptureFixture[str]) -> None:
    _print_q5(Q5Result(origin_stop_id="X", walking_distance_m=400.0, reachable_stop_ids=[]))
    out = capsys.readouterr().out
    assert "From stop X" in out
    assert "(none)" in out
