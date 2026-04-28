"""Tests for spatialbench.data.load."""

import zipfile
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
from hypothesis import given
from hypothesis import strategies as st

from spatialbench.data.load import (
    LoadError,
    LoadResult,
    load,
    main,
    parse_gtfs_time,
)

# ── synthetic GTFS fixtures ──────────────────────────────────────────────────


STOPS_CSV = (
    "stop_id,stop_name,stop_lat,stop_lon\n"
    "1,Alpha,32.0000,34.8000\n"
    "2,Beta,32.0100,34.8100\n"
    "3,Gamma,32.0200,34.8200\n"
)
ROUTES_CSV = (
    "route_id,agency_id,route_short_name,route_long_name,route_type\n"
    "R1,A1,1,Line One,3\n"
    "R2,A1,2,Line Two,3\n"
)
TRIPS_CSV = "route_id,service_id,trip_id\nR1,SVC,T1\nR2,SVC,T2\n"
STOP_TIMES_CSV = (
    "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
    "T1,07:00:00,07:00:30,1,1\n"
    "T1,25:30:00,25:30:30,2,2\n"
    "T2,08:00:00,08:00:30,2,1\n"
    "T2,08:15:00,08:15:30,3,2\n"
)
STOP_TIMES_WITH_SHAPE_CSV = (
    "trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled\n"
    "T1,07:00:00,07:00:30,1,1,0.0\n"
    "T1,25:30:00,25:30:30,2,2,1234.5\n"
    "T2,08:00:00,08:00:30,2,1,0.0\n"
    "T2,08:15:00,08:15:30,3,2,2200.0\n"
)
STOP_TIMES_SHAPE_ALL_NULL_CSV = (
    "trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled\n"
    "T1,07:00:00,07:00:30,1,1,\n"
    "T1,25:30:00,25:30:30,2,2,\n"
    "T2,08:00:00,08:00:30,2,1,\n"
    "T2,08:15:00,08:15:30,3,2,\n"
)
CALENDAR_CSV = (
    "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
    "SVC,1,1,1,1,1,0,0,20260101,20261231\n"
)
CALENDAR_DATES_CSV = "service_id,date,exception_type\nSVC,20260501,2\n"


def _make_zip(tmp_path: Path, files: dict[str, str], name: str = "gtfs.zip") -> Path:
    zp = tmp_path / name
    with zipfile.ZipFile(zp, "w") as z:
        for fname, content in files.items():
            z.writestr(fname, content)
    return zp


def _full_gtfs(
    *,
    with_shape: bool = False,
    shape_all_null: bool = False,
    with_calendar_dates: bool = True,
) -> dict[str, str]:
    if shape_all_null:
        st_csv = STOP_TIMES_SHAPE_ALL_NULL_CSV
    elif with_shape:
        st_csv = STOP_TIMES_WITH_SHAPE_CSV
    else:
        st_csv = STOP_TIMES_CSV
    files = {
        "stops.txt": STOPS_CSV,
        "routes.txt": ROUTES_CSV,
        "trips.txt": TRIPS_CSV,
        "stop_times.txt": st_csv,
        "calendar.txt": CALENDAR_CSV,
    }
    if with_calendar_dates:
        files["calendar_dates.txt"] = CALENDAR_DATES_CSV
    return files


# ── parse_gtfs_time ──────────────────────────────────────────────────────────


def test_parse_gtfs_time_basic() -> None:
    assert parse_gtfs_time("00:00:00") == 0
    assert parse_gtfs_time("01:02:03") == 3723
    assert parse_gtfs_time("23:59:59") == 86399


def test_parse_gtfs_time_handles_hour_ge_24() -> None:
    assert parse_gtfs_time("25:30:00") == 25 * 3600 + 30 * 60


def test_parse_gtfs_time_rejects_wrong_segment_count() -> None:
    with pytest.raises(ValueError, match="HH:MM:SS"):
        parse_gtfs_time("12:30")


def test_parse_gtfs_time_rejects_non_integer() -> None:
    with pytest.raises(ValueError, match="non-integer"):
        parse_gtfs_time("12:ab:30")


def test_parse_gtfs_time_rejects_out_of_range_minute() -> None:
    with pytest.raises(ValueError, match="out-of-range"):
        parse_gtfs_time("12:60:00")


def test_parse_gtfs_time_rejects_out_of_range_second() -> None:
    with pytest.raises(ValueError, match="out-of-range"):
        parse_gtfs_time("12:00:60")


def test_parse_gtfs_time_rejects_negative_hour() -> None:
    with pytest.raises(ValueError, match="out-of-range"):
        parse_gtfs_time("-1:00:00")


@given(
    h=st.integers(min_value=0, max_value=47),
    m=st.integers(min_value=0, max_value=59),
    s=st.integers(min_value=0, max_value=59),
)
def test_parse_gtfs_time_property(h: int, m: int, s: int) -> None:
    formatted = f"{h:02d}:{m:02d}:{s:02d}"
    assert parse_gtfs_time(formatted) == h * 3600 + m * 60 + s


# ── load: happy path ────────────────────────────────────────────────────────


def test_load_happy_path(tmp_path: Path) -> None:
    zp = _make_zip(tmp_path, _full_gtfs())
    db = tmp_path / "out.duckdb"

    result = load(zp, db)

    assert isinstance(result, LoadResult)
    assert result.db_path == db
    assert result.row_counts["stops"] == 3
    assert result.row_counts["routes"] == 2
    assert result.row_counts["trips"] == 2
    assert result.row_counts["stop_times"] == 4
    assert result.row_counts["calendar"] == 1
    assert result.row_counts["calendar_dates"] == 1
    assert result.has_shape_dist_traveled is False
    assert db.exists()


def test_load_geometry_is_populated(tmp_path: Path) -> None:
    zp = _make_zip(tmp_path, _full_gtfs())
    db = tmp_path / "out.duckdb"
    load(zp, db)

    con = duckdb.connect(str(db))
    con.execute("LOAD spatial;")
    rows = con.execute(
        "SELECT stop_id, ST_X(geom), ST_Y(geom) FROM stops ORDER BY stop_id"
    ).fetchall()
    con.close()

    assert rows == [(1, 34.8, 32.0), (2, 34.81, 32.01), (3, 34.82, 32.02)]


def test_load_dual_time_columns(tmp_path: Path) -> None:
    zp = _make_zip(tmp_path, _full_gtfs())
    db = tmp_path / "out.duckdb"
    load(zp, db)

    con = duckdb.connect(str(db))
    rows = con.execute(
        "SELECT arrival_time, arrival_sec FROM stop_times "
        "WHERE trip_id = 'T1' ORDER BY stop_sequence"
    ).fetchall()
    con.close()

    assert rows == [("07:00:00", 25200), ("25:30:00", 91800)]


def test_load_spatial_index_supports_dwithin(tmp_path: Path) -> None:
    zp = _make_zip(tmp_path, _full_gtfs())
    db = tmp_path / "out.duckdb"
    load(zp, db)

    con = duckdb.connect(str(db))
    con.execute("LOAD spatial;")
    n = con.execute(
        "SELECT COUNT(*) FROM stops WHERE ST_DWithin(geom, ST_Point(34.8, 32.0), 0.001)"
    ).fetchone()
    con.close()
    assert n is not None and n[0] == 1


def test_load_geom_itm_supports_meter_dwithin(tmp_path: Path) -> None:
    # Stops are ~1.4 km apart in the test fixture (0.01° lat + 0.01° lon at lat 32).
    # A 500m radius around stop 1 should match only stop 1 itself.
    zp = _make_zip(tmp_path, _full_gtfs())
    db = tmp_path / "out.duckdb"
    load(zp, db)

    con = duckdb.connect(str(db))
    con.execute("LOAD spatial;")
    rows = con.execute(
        """
        SELECT stop_id FROM stops
        WHERE ST_DWithin(
            geom_itm,
            ST_Transform(ST_Point(34.8000, 32.0000), 'EPSG:4326', 'EPSG:2039'),
            500
        )
        ORDER BY stop_id
        """
    ).fetchall()
    con.close()
    assert rows == [(1,)]


# ── load: shape_dist_traveled detection ──────────────────────────────────────


def test_load_detects_shape_dist_present(tmp_path: Path) -> None:
    zp = _make_zip(tmp_path, _full_gtfs(with_shape=True))
    result = load(zp, tmp_path / "out.duckdb")
    assert result.has_shape_dist_traveled is True


def test_load_detects_shape_dist_all_null_as_absent(tmp_path: Path) -> None:
    zp = _make_zip(tmp_path, _full_gtfs(shape_all_null=True))
    result = load(zp, tmp_path / "out.duckdb")
    assert result.has_shape_dist_traveled is False


# ── load: optional file ──────────────────────────────────────────────────────


def test_load_handles_missing_calendar_dates(tmp_path: Path) -> None:
    zp = _make_zip(tmp_path, _full_gtfs(with_calendar_dates=False))
    result = load(zp, tmp_path / "out.duckdb")
    assert result.row_counts["calendar_dates"] == 0


# ── load: error path ────────────────────────────────────────────────────────


def test_load_raises_on_missing_required_file(tmp_path: Path) -> None:
    files = _full_gtfs()
    del files["routes.txt"]
    zp = _make_zip(tmp_path, files)
    with pytest.raises(LoadError, match=r"routes\.txt"):
        load(zp, tmp_path / "out.duckdb")


# ── load: idempotency + default db_path ──────────────────────────────────────


def test_load_is_idempotent_on_existing_db(tmp_path: Path) -> None:
    zp = _make_zip(tmp_path, _full_gtfs())
    db = tmp_path / "out.duckdb"
    load(zp, db)
    result = load(zp, db)
    assert result.row_counts["stops"] == 3


def test_load_uses_default_db_path_when_none(tmp_path: Path) -> None:
    zp = _make_zip(tmp_path, _full_gtfs())
    default = tmp_path / "default.duckdb"
    with patch("spatialbench.data.load.DB_PATH", default):
        result = load(zp)
    assert result.db_path == default
    assert default.exists()


# ── main ─────────────────────────────────────────────────────────────────────


def test_main_with_explicit_zip_and_db(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    zp = _make_zip(tmp_path, _full_gtfs())
    db = tmp_path / "out.duckdb"

    main(["--zip", str(zp), "--db", str(db)])

    out = capsys.readouterr().out
    assert str(db) in out
    assert "stops: 3" in out
    assert "shape_dist_traveled present: False" in out


def test_main_uses_manifest_when_no_zip_arg(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    zp = _make_zip(tmp_path, _full_gtfs(), name="feed.zip")
    assert zp.exists()
    db = tmp_path / "out.duckdb"

    from spatialbench.data.fetch import Manifest

    fake_manifest = Manifest(url="https://example.com/feed.zip", filename="feed.zip")
    with (
        patch("spatialbench.data.load.load_manifest", return_value=fake_manifest),
        patch("spatialbench.data.load.RAW_DIR", tmp_path),
    ):
        main(["--db", str(db)])

    assert "stops: 3" in capsys.readouterr().out


def test_main_exits_when_zip_missing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    missing = tmp_path / "nope.zip"
    with pytest.raises(SystemExit) as exc:
        main(["--zip", str(missing), "--db", str(tmp_path / "out.duckdb")])
    assert exc.value.code == 1
    assert "not found" in capsys.readouterr().err


def test_main_exits_on_load_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    files = _full_gtfs()
    del files["stops.txt"]
    zp = _make_zip(tmp_path, files)
    with pytest.raises(SystemExit) as exc:
        main(["--zip", str(zp), "--db", str(tmp_path / "out.duckdb")])
    assert exc.value.code == 1
    assert "stops.txt" in capsys.readouterr().err
