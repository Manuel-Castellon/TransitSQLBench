"""
Reference implementations of the five tiered benchmark queries.

These are the *correct* answers — the harness compares agent output against
results from these functions on the same DuckDB database. They are written for
clarity, not for the agent to read; spatially-naive solutions to the same
question would, for instance, omit `geom_itm` (EPSG:2039) and use the lat/lon
`geom` column for meter-based ST_DWithin, which is wrong by ~111 km/degree.
"""

from typing import Final

import duckdb
from pydantic import BaseModel

DEFAULT_WALKING_DISTANCE_M: Final[float] = 400.0


# ── Result models ────────────────────────────────────────────────────────────


class Tier1Result(BaseModel):
    """Number of distinct stops served by a route on any weekday (Mon-Fri)."""

    route_id: str
    n_stops: int


class StopWithDistance(BaseModel):
    stop_id: str
    stop_name: str
    distance_m: float


class Tier2Result(BaseModel):
    """Stops within `radius_m` of a query point."""

    stops: list[StopWithDistance]


class RoutePair(BaseModel):
    route_a: str
    route_b: str
    shared_stops: int


class Tier3Result(BaseModel):
    pairs: list[RoutePair]


class RouteAvgGap(BaseModel):
    route_id: str
    avg_gap_m: float


class Tier4Result(BaseModel):
    routes: list[RouteAvgGap]
    used_shape_dist: bool


class Tier5Result(BaseModel):
    """Stops reachable from `origin_stop_id` with exactly one transfer.

    A transfer is either remaining at the alighting stop (same-stop transfer)
    or walking to any stop within `walking_distance_m`.
    """

    origin_stop_id: str
    walking_distance_m: float
    reachable_stop_ids: list[str]


# ── Tier 1: tabular join ─────────────────────────────────────────────────────


def tier1_route_stops_on_weekdays(
    con: duckdb.DuckDBPyConnection,
    route_id: str,
) -> Tier1Result:
    n = con.execute(
        """
        SELECT COUNT(DISTINCT st.stop_id)
        FROM trips t
        JOIN stop_times st ON st.trip_id = t.trip_id
        JOIN calendar c ON c.service_id = t.service_id
        WHERE CAST(t.route_id AS VARCHAR) = ?
          AND (c.monday + c.tuesday + c.wednesday + c.thursday + c.friday) > 0
        """,
        [route_id],
    ).fetchall()[0][0]
    return Tier1Result(route_id=route_id, n_stops=int(n))


# ── Tier 2: ST_DWithin in meters ─────────────────────────────────────────────


def tier2_stops_within_radius(
    con: duckdb.DuckDBPyConnection,
    lat: float,
    lon: float,
    radius_m: float,
) -> Tier2Result:
    rows = con.execute(
        """
        WITH q AS (
            SELECT ST_Transform(ST_Point(?, ?), 'EPSG:4326', 'EPSG:2039') AS p
        )
        SELECT
            CAST(s.stop_id AS VARCHAR),
            s.stop_name,
            ST_Distance(s.geom_itm, q.p) AS distance_m
        FROM stops s, q
        WHERE ST_DWithin(s.geom_itm, q.p, ?)
        ORDER BY distance_m, s.stop_id
        """,
        [lon, lat, radius_m],
    ).fetchall()
    return Tier2Result(
        stops=[
            StopWithDistance(stop_id=str(r[0]), stop_name=str(r[1]), distance_m=float(r[2]))
            for r in rows
        ]
    )


# ── Tier 3: relational set intersection ──────────────────────────────────────


def tier3_route_pairs_sharing_stops(
    con: duckdb.DuckDBPyConnection,
    limit: int = 20,
) -> Tier3Result:
    rows = con.execute(
        """
        WITH bridge AS (
            SELECT DISTINCT
                CAST(t.route_id AS VARCHAR) AS route_id,
                st.stop_id
            FROM trips t
            JOIN stop_times st USING (trip_id)
        )
        SELECT
            a.route_id AS route_a,
            b.route_id AS route_b,
            COUNT(*) AS shared
        FROM bridge a
        JOIN bridge b ON a.stop_id = b.stop_id
        WHERE a.route_id < b.route_id
        GROUP BY a.route_id, b.route_id
        ORDER BY shared DESC, route_a, route_b
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    return Tier3Result(
        pairs=[
            RoutePair(route_a=str(r[0]), route_b=str(r[1]), shared_stops=int(r[2])) for r in rows
        ]
    )


# ── Tier 4: ordered ST_Distance over consecutive stops ───────────────────────


def tier4_route_consecutive_stop_gaps(
    con: duckdb.DuckDBPyConnection,
    use_shape_dist: bool,
    limit: int = 20,
) -> Tier4Result:
    if use_shape_dist:
        sql = """
            WITH legs AS (
                SELECT
                    CAST(t.route_id AS VARCHAR) AS route_id,
                    st.shape_dist_traveled - LAG(st.shape_dist_traveled)
                        OVER (PARTITION BY st.trip_id ORDER BY st.stop_sequence) AS gap_m
                FROM stop_times st
                JOIN trips t USING (trip_id)
            )
            SELECT route_id, AVG(gap_m) AS avg_gap_m
            FROM legs
            WHERE gap_m IS NOT NULL AND gap_m > 0
            GROUP BY route_id
            ORDER BY avg_gap_m DESC, route_id
            LIMIT ?
        """
    else:
        sql = """
            WITH legs AS (
                SELECT
                    CAST(t.route_id AS VARCHAR) AS route_id,
                    s.geom_itm AS geom_itm,
                    LAG(s.geom_itm) OVER (
                        PARTITION BY st.trip_id ORDER BY st.stop_sequence
                    ) AS prev_geom
                FROM stop_times st
                JOIN trips t USING (trip_id)
                JOIN stops s USING (stop_id)
            )
            SELECT route_id, AVG(ST_Distance(geom_itm, prev_geom)) AS avg_gap_m
            FROM legs
            WHERE prev_geom IS NOT NULL
            GROUP BY route_id
            ORDER BY avg_gap_m DESC, route_id
            LIMIT ?
        """
    rows = con.execute(sql, [limit]).fetchall()
    return Tier4Result(
        routes=[RouteAvgGap(route_id=str(r[0]), avg_gap_m=float(r[1])) for r in rows],
        used_shape_dist=use_shape_dist,
    )


# ── Tier 5: 2-hop reachability with walking transfers ────────────────────────


def tier5_reachable_with_one_transfer(
    con: duckdb.DuckDBPyConnection,
    origin_stop_id: str,
    walking_distance_m: float = DEFAULT_WALKING_DISTANCE_M,
) -> Tier5Result:
    rows = con.execute(
        """
        WITH direct_from AS (
            SELECT DISTINCT b.stop_id AS dest
            FROM stop_times a
            JOIN stop_times b
              ON a.trip_id = b.trip_id
             AND b.stop_sequence > a.stop_sequence
            WHERE CAST(a.stop_id AS VARCHAR) = ?
        ),
        transfer_pool AS (
            SELECT DISTINCT t.stop_id AS via
            FROM direct_from df
            JOIN stops sa ON sa.stop_id = df.dest
            JOIN stops t  ON ST_DWithin(t.geom_itm, sa.geom_itm, ?)
        )
        SELECT DISTINCT CAST(b.stop_id AS VARCHAR) AS reachable
        FROM transfer_pool tp
        JOIN stop_times a ON a.stop_id = tp.via
        JOIN stop_times b
          ON b.trip_id = a.trip_id
         AND b.stop_sequence > a.stop_sequence
        WHERE CAST(b.stop_id AS VARCHAR) != ?
        ORDER BY reachable
        """,
        [origin_stop_id, walking_distance_m, origin_stop_id],
    ).fetchall()
    return Tier5Result(
        origin_stop_id=origin_stop_id,
        walking_distance_m=walking_distance_m,
        reachable_stop_ids=[str(r[0]) for r in rows],
    )
