"""
Load a GTFS zip into a DuckDB database with spatial indexing.

Usage:
    python -m spatialbench.data.load                              # use manifest defaults
    python -m spatialbench.data.load --zip path/to.zip --db path/to.duckdb
"""

import argparse
import sys
import tempfile
import zipfile
from pathlib import Path

import duckdb
from pydantic import BaseModel

from spatialbench.data.fetch import RAW_DIR, load_manifest

DB_PATH = Path(__file__).parent.parent.parent / "data" / "spatialbench.duckdb"

REQUIRED_FILES: tuple[str, ...] = ("stops", "routes", "trips", "stop_times", "calendar")
OPTIONAL_FILES: tuple[str, ...] = ("calendar_dates",)


class LoadResult(BaseModel):
    db_path: Path
    row_counts: dict[str, int]
    has_shape_dist_traveled: bool


class LoadError(Exception):
    pass


def parse_gtfs_time(s: str) -> int:
    """Parse a GTFS time ('HH:MM:SS', possibly HH≥24) to seconds since service-day start.

    GTFS allows hours ≥ 24 to express trips that cross midnight while still belonging
    to the previous service day (e.g., '25:30:00' is 1:30 AM the next calendar day).
    """
    parts = s.split(":")
    if len(parts) != 3:
        raise ValueError(f"expected HH:MM:SS, got {s!r}")
    try:
        h, m, sec = (int(p) for p in parts)
    except ValueError as e:
        raise ValueError(f"non-integer component in {s!r}") from e
    if h < 0 or not (0 <= m < 60) or not (0 <= sec < 60):
        raise ValueError(f"out-of-range component in {s!r}")
    return h * 3600 + m * 60 + sec


def _scalar_int(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchall()[0][0])


def _install_spatial(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")


def _load_table(con: duckdb.DuckDBPyConnection, name: str, csv_path: Path) -> int:
    con.execute(f"DROP TABLE IF EXISTS {name};")
    con.execute(
        f"CREATE TABLE {name} AS "
        "SELECT * FROM read_csv(?, header=true, nullstr='', auto_detect=true);",
        [str(csv_path)],
    )
    return _scalar_int(con, f"SELECT COUNT(*) FROM {name};")


def _load_stops(con: duckdb.DuckDBPyConnection, csv_path: Path) -> int:
    # geom: WGS84 lat/lon (degrees) — useful for display + cheap bbox prefilter.
    # geom_itm: EPSG:2039 (Israeli Transverse Mercator, meters) — what every
    # meter-based ST_DWithin/ST_Distance must use. Materializing both keeps
    # spatial queries explicit about which CRS they're operating in.
    con.execute("DROP TABLE IF EXISTS stops;")
    con.execute(
        """
        CREATE TABLE stops AS
        SELECT
            *,
            ST_Point(stop_lon, stop_lat) AS geom,
            ST_Transform(ST_Point(stop_lon, stop_lat), 'EPSG:4326', 'EPSG:2039') AS geom_itm
        FROM read_csv(?, header=true, nullstr='', auto_detect=true);
        """,
        [str(csv_path)],
    )
    return _scalar_int(con, "SELECT COUNT(*) FROM stops;")


def _load_stop_times(con: duckdb.DuckDBPyConnection, csv_path: Path) -> tuple[int, bool]:
    con.execute("DROP TABLE IF EXISTS stop_times;")
    con.execute(
        """
        CREATE TABLE stop_times AS
        SELECT
            *,
            CASE WHEN arrival_time IS NULL THEN NULL ELSE
                CAST(split_part(arrival_time, ':', 1) AS INTEGER) * 3600
                + CAST(split_part(arrival_time, ':', 2) AS INTEGER) * 60
                + CAST(split_part(arrival_time, ':', 3) AS INTEGER)
            END AS arrival_sec,
            CASE WHEN departure_time IS NULL THEN NULL ELSE
                CAST(split_part(departure_time, ':', 1) AS INTEGER) * 3600
                + CAST(split_part(departure_time, ':', 2) AS INTEGER) * 60
                + CAST(split_part(departure_time, ':', 3) AS INTEGER)
            END AS departure_sec
        FROM read_csv(
            ?,
            header=true,
            nullstr='',
            auto_detect=true,
            types={'arrival_time': 'VARCHAR', 'departure_time': 'VARCHAR'}
        );
        """,
        [str(csv_path)],
    )
    n = _scalar_int(con, "SELECT COUNT(*) FROM stop_times;")
    cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'stop_times';"
        ).fetchall()
    }
    if "shape_dist_traveled" not in cols:
        return n, False
    has = _scalar_int(
        con,
        "SELECT COUNT(*) FROM stop_times WHERE shape_dist_traveled IS NOT NULL;",
    )
    return n, has > 0


def _add_indexes(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE INDEX idx_stops_geom ON stops USING RTREE (geom);")
    con.execute("CREATE INDEX idx_stops_geom_itm ON stops USING RTREE (geom_itm);")
    con.execute("CREATE INDEX idx_stop_times_trip ON stop_times (trip_id);")
    con.execute("CREATE INDEX idx_stop_times_stop ON stop_times (stop_id);")
    con.execute("CREATE INDEX idx_trips_route ON trips (route_id);")


def load(zip_path: Path, db_path: Path | None = None) -> LoadResult:
    if db_path is None:
        db_path = DB_PATH
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()

    with tempfile.TemporaryDirectory() as tmp, zipfile.ZipFile(zip_path) as z:
        names = set(z.namelist())
        missing = [f"{r}.txt" for r in REQUIRED_FILES if f"{r}.txt" not in names]
        if missing:
            raise LoadError(f"missing required GTFS files: {', '.join(missing)}")
        z.extractall(tmp)
        tmp_path = Path(tmp)

        con = duckdb.connect(str(db_path))
        try:
            _install_spatial(con)
            row_counts: dict[str, int] = {}
            row_counts["stops"] = _load_stops(con, tmp_path / "stops.txt")
            row_counts["routes"] = _load_table(con, "routes", tmp_path / "routes.txt")
            row_counts["trips"] = _load_table(con, "trips", tmp_path / "trips.txt")
            n_st, has_shape = _load_stop_times(con, tmp_path / "stop_times.txt")
            row_counts["stop_times"] = n_st
            row_counts["calendar"] = _load_table(con, "calendar", tmp_path / "calendar.txt")
            cd_path = tmp_path / "calendar_dates.txt"
            if cd_path.exists():
                row_counts["calendar_dates"] = _load_table(con, "calendar_dates", cd_path)
            else:
                row_counts["calendar_dates"] = 0
            _add_indexes(con)
        finally:
            con.close()

    return LoadResult(
        db_path=db_path,
        row_counts=row_counts,
        has_shape_dist_traveled=has_shape,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--zip", type=Path, default=None)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    args = parser.parse_args(argv)

    zip_path: Path = args.zip if args.zip is not None else RAW_DIR / load_manifest().filename
    if not zip_path.exists():
        print(f"GTFS zip not found at {zip_path}. Run `make fetch` first.", file=sys.stderr)
        sys.exit(1)

    try:
        result = load(zip_path, args.db)
    except LoadError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    print(f"Loaded into {result.db_path}")
    for table, count in result.row_counts.items():
        print(f"  {table}: {count:,}")
    print(f"  shape_dist_traveled present: {result.has_shape_dist_traveled}")


if __name__ == "__main__":  # pragma: no cover
    main()
