"""
CLI for poking at the reference queries against a loaded TransitSQLBench DuckDB.

Usage:
    python -m transitsqlbench.queries.cli tier1 --route R1
    python -m transitsqlbench.queries.cli tier2 --lat 32.0055 --lon 34.8854 --radius 500
    python -m transitsqlbench.queries.cli tier3 [--limit 20]
    python -m transitsqlbench.queries.cli tier4 [--shape-dist] [--limit 20]
    python -m transitsqlbench.queries.cli tier5 --stop STOP_ID [--walking 400]
    python -m transitsqlbench.queries.cli all   # demo run for Stage 1 acceptance

This is exploratory tooling, not the Stage 5 Streamlit UI. It exists so we can
get a feel for the answers (and the failure modes) before building anything
heavier.
"""

import argparse
import sys
from pathlib import Path

import duckdb

from transitsqlbench.data.load import DB_PATH
from transitsqlbench.queries.reference import (
    DEFAULT_WALKING_DISTANCE_M,
    Tier1Result,
    Tier2Result,
    Tier3Result,
    Tier4Result,
    Tier5Result,
    tier1_route_stops_on_weekdays,
    tier2_stops_within_radius,
    tier3_route_pairs_sharing_stops,
    tier4_route_consecutive_stop_gaps,
    tier5_two_hop_reachable_with_walking,
)


def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    if not db_path.exists():
        print(
            f"DuckDB file not found at {db_path}. Run `make data` first.",
            file=sys.stderr,
        )
        sys.exit(1)
    con = duckdb.connect(str(db_path), read_only=True)
    con.execute("LOAD spatial;")
    return con


# ── pretty-printers ──────────────────────────────────────────────────────────


def _print_tier1(r: Tier1Result) -> None:
    print(f"Route {r.route_id}: {r.n_stops} distinct stops on weekdays.")


def _print_tier2(r: Tier2Result, lat: float, lon: float, radius_m: float) -> None:
    print(f"Stops within {radius_m:g} m of ({lat}, {lon}):")
    if not r.stops:
        print("  (none)")
        return
    for s in r.stops:
        print(f"  {s.stop_id:>10}  {s.distance_m:>8.1f} m  {s.stop_name}")


def _print_tier3(r: Tier3Result) -> None:
    print("Route pairs by shared stops (DESC):")
    if not r.pairs:
        print("  (none)")
        return
    for p in r.pairs:
        print(f"  {p.route_a:>8}  {p.route_b:>8}   shared={p.shared_stops}")


def _print_tier4(r: Tier4Result) -> None:
    src = "shape_dist_traveled" if r.used_shape_dist else "straight-line geometry"
    print(f"Average inter-stop gap per route ({src}, DESC):")
    if not r.routes:
        print("  (none)")
        return
    for g in r.routes:
        print(f"  {g.route_id:>8}   {g.avg_gap_m:>10.1f} m")


def _print_tier5(r: Tier5Result) -> None:
    print(
        f"From stop {r.origin_stop_id}, with walking <= {r.walking_distance_m:g} m, "
        f"{len(r.reachable_stop_ids)} stops reachable by schedule-agnostic two-hop search:"
    )
    if not r.reachable_stop_ids:
        print("  (none)")
        return
    print("  " + ", ".join(r.reachable_stop_ids))


# ── per-subcommand handlers ──────────────────────────────────────────────────


def _cmd_tier1(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    _print_tier1(tier1_route_stops_on_weekdays(con, args.route))


def _cmd_tier2(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    r = tier2_stops_within_radius(con, lat=args.lat, lon=args.lon, radius_m=args.radius)
    _print_tier2(r, args.lat, args.lon, args.radius)


def _cmd_tier3(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    _print_tier3(tier3_route_pairs_sharing_stops(con, limit=args.limit))


def _cmd_tier4(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    _print_tier4(
        tier4_route_consecutive_stop_gaps(con, use_shape_dist=args.shape_dist, limit=args.limit)
    )


def _cmd_tier5(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    _print_tier5(
        tier5_two_hop_reachable_with_walking(
            con, origin_stop_id=args.stop, walking_distance_m=args.walking
        )
    )


def _cmd_all(con: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    """Stage 1 acceptance demo: one example per tier with sensible defaults."""
    print("── Tier 1: route → stops on weekdays ──")
    _print_tier1(tier1_route_stops_on_weekdays(con, args.route))
    print()
    print("── Tier 2: stops within radius of point ──")
    r2 = tier2_stops_within_radius(con, lat=args.lat, lon=args.lon, radius_m=args.radius)
    _print_tier2(r2, args.lat, args.lon, args.radius)
    print()
    print("── Tier 3: route pairs sharing stops ──")
    _print_tier3(tier3_route_pairs_sharing_stops(con, limit=args.limit))
    print()
    print("── Tier 4: average inter-stop gap per route ──")
    _print_tier4(
        tier4_route_consecutive_stop_gaps(con, use_shape_dist=args.shape_dist, limit=args.limit)
    )
    print()
    print("── Tier 5: schedule-agnostic two-hop reachability ──")
    _print_tier5(
        tier5_two_hop_reachable_with_walking(
            con, origin_stop_id=args.stop, walking_distance_m=args.walking
        )
    )


# ── argument parsing ─────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="transitsqlbench-queries")
    parser.add_argument("--db", type=Path, default=DB_PATH, help="path to transitsqlbench.duckdb")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("tier1", help="route → distinct stops on weekdays")
    p1.add_argument("--route", required=True)
    p1.set_defaults(handler=_cmd_tier1)

    p2 = sub.add_parser("tier2", help="stops within radius_m of (lat, lon)")
    p2.add_argument("--lat", type=float, required=True)
    p2.add_argument("--lon", type=float, required=True)
    p2.add_argument("--radius", type=float, default=500.0, help="meters (default 500)")
    p2.set_defaults(handler=_cmd_tier2)

    p3 = sub.add_parser("tier3", help="route pairs sharing the most stops")
    p3.add_argument("--limit", type=int, default=20)
    p3.set_defaults(handler=_cmd_tier3)

    p4 = sub.add_parser("tier4", help="routes with the largest average inter-stop gap")
    p4.add_argument(
        "--shape-dist",
        action="store_true",
        help="use shape_dist_traveled column instead of straight-line ST_Distance",
    )
    p4.add_argument("--limit", type=int, default=20)
    p4.set_defaults(handler=_cmd_tier4)

    p5 = sub.add_parser(
        "tier5",
        help="schedule-agnostic two-hop reachable stops with walking transfers",
    )
    p5.add_argument("--stop", required=True, help="origin stop_id")
    p5.add_argument(
        "--walking",
        type=float,
        default=DEFAULT_WALKING_DISTANCE_M,
        help=f"walking transfer distance in meters (default {DEFAULT_WALKING_DISTANCE_M:g})",
    )
    p5.set_defaults(handler=_cmd_tier5)

    pa = sub.add_parser("all", help="run a one-shot demo across all five tiers")
    pa.add_argument("--route", default="R1")
    pa.add_argument("--lat", type=float, default=32.0055)  # Ben Gurion airport
    pa.add_argument("--lon", type=float, default=34.8854)
    pa.add_argument("--radius", type=float, default=500.0)
    pa.add_argument("--limit", type=int, default=10)
    pa.add_argument("--shape-dist", action="store_true")
    pa.add_argument("--stop", required=True, help="origin stop_id for tier 5")
    pa.add_argument("--walking", type=float, default=DEFAULT_WALKING_DISTANCE_M)
    pa.set_defaults(handler=_cmd_all)

    return parser


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)
    con = _connect(args.db)
    try:
        args.handler(con, args)
    finally:
        con.close()


if __name__ == "__main__":  # pragma: no cover
    main()
