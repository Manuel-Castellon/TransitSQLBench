"""
Cold-LLM baseline grader for Q5 (schedule-agnostic two-hop reachability).

Operates in two modes:

  - default: load a saved baseline JSON from `baselines/` (no API spend) and
    grade it against the current DuckDB ground truth, reporting both strict
    and loose-subset name matches.
  - --live: hit the Anthropic API with a real model (Haiku 4.5 by default),
    save the response to `baselines/`, then grade.

The point of this script is the *contrast* against the spatial-aware reference
SQL: a generic LLM with no GTFS tools can name a few dozen central terminals
and rail interchanges, but is off by ~2 orders of magnitude on the true
reachable set (~12k distinct stop names). That's the headline finding the
benchmark is built to make legible.

Run:
    uv run python scripts/naive_llm_baseline.py
    uv run python scripts/naive_llm_baseline.py --baseline baselines/cold_subagent_2026-04-28.json
    uv run --with anthropic python scripts/naive_llm_baseline.py --live
"""

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import duckdb

REPO_ROOT: Final[Path] = Path(__file__).parent.parent
DEFAULT_DB: Final[Path] = REPO_ROOT / "data" / "transitsqlbench.duckdb"
DEFAULT_BASELINE: Final[Path] = REPO_ROOT / "baselines" / "cold_subagent_2026-04-28.json"
ORIGIN_STOP_ID: Final[int] = 22633  # Ben Gurion Airport Terminal 1
WALKING_M: Final[int] = 400
LIVE_MODEL: Final[str] = "claude-haiku-4-5"


@dataclass
class GradeResult:
    listed_total: int
    strict_hits: int
    loose_hits: int
    truth_name_count: int
    estimate_count: int | None
    listed_strict_misses: list[str]
    listed_loose_misses: list[str]


def get_truth_stop_names(
    con: duckdb.DuckDBPyConnection, stop_id: int, walking_m: int
) -> list[str]:
    rows = con.execute(
        """
        WITH direct_from AS (
            SELECT DISTINCT b.stop_id AS dest
            FROM stop_times a JOIN stop_times b
              ON a.trip_id = b.trip_id AND b.stop_sequence > a.stop_sequence
            WHERE a.stop_id = ?
        ),
        pool AS (
            SELECT DISTINCT t.stop_id AS via
            FROM direct_from df
            JOIN stops sa ON sa.stop_id = df.dest
            JOIN stops t  ON ST_DWithin(t.geom_itm, sa.geom_itm, ?)
        )
        SELECT DISTINCT s.stop_name
        FROM pool p
        JOIN stop_times a ON a.stop_id = p.via
        JOIN stop_times b
          ON b.trip_id = a.trip_id AND b.stop_sequence > a.stop_sequence
        JOIN stops s ON s.stop_id = b.stop_id
        WHERE b.stop_id != ?
        """,
        [stop_id, walking_m, stop_id],
    ).fetchall()
    return [str(r[0]) for r in rows]


def strict_match(listed: list[str], truth: set[str]) -> list[bool]:
    """Exact (whitespace-normalized) string equality with the truth set."""
    norm_truth = {t.strip() for t in truth}
    return [n.strip() in norm_truth for n in listed]


def loose_subset_match(listed: list[str], truth: set[str]) -> list[bool]:
    """Loose match: a listed name `n` is a hit iff some truth name contains `n`
    as a substring, or vice versa, after whitespace normalization. This forgives
    bilingual variants and partial-name shortenings that the strict matcher
    rejects, at the cost of false positives on very short listed names.
    """
    norm_truth = [t.strip() for t in truth]
    hits: list[bool] = []
    for n in listed:
        ns = n.strip()
        if not ns:
            hits.append(False)
            continue
        hit = any(ns in t or t in ns for t in norm_truth)
        hits.append(hit)
    return hits


def grade(
    listed: list[str], truth_names: list[str], estimate_count: int | None
) -> GradeResult:
    truth_set = {t.strip() for t in truth_names}
    strict = strict_match(listed, truth_set)
    loose = loose_subset_match(listed, truth_set)
    return GradeResult(
        listed_total=len(listed),
        strict_hits=sum(strict),
        loose_hits=sum(loose),
        truth_name_count=len(truth_set),
        estimate_count=estimate_count,
        listed_strict_misses=[n for n, h in zip(listed, strict, strict=True) if not h],
        listed_loose_misses=[n for n, h in zip(listed, loose, strict=True) if not h],
    )


def load_baseline(path: Path) -> tuple[list[str], int | None]:
    data = json.loads(path.read_text())
    response = data["response"]
    listed = [str(s) for s in response["reachable_stop_names"]]
    estimate = response.get("estimate_count")
    return listed, int(estimate) if estimate is not None else None


def call_live_model() -> tuple[list[str], int | None]:  # pragma: no cover
    import anthropic

    if not os.environ.get("ANTHROPIC_API_KEY"):
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    prompt = (
        f"You are a transit-routing assistant for the Israeli national bus network.\n\n"
        f'Question: A passenger is starting at "Ben Gurion Airport / Terminal 1" '
        f"(stop_id {ORIGIN_STOP_ID}).\n\n"
        f"They will:\n"
        f"  1. Ride from this stop to some downstream stop Y.\n"
        f"  2. Optionally walk up to {WALKING_M} metres to a different stop Y'.\n"
        f"  3. Ride from Y' to a final downstream stop Z.\n\n"
        f"Return ONLY this JSON: "
        f'{{"reachable_stop_names": ["..."], "estimate_count": <int>}}'
    )
    client = anthropic.Anthropic()
    msg = client.messages.create(
        model=LIVE_MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text  # type: ignore[union-attr]
    text = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    parsed = json.loads(text)
    listed = [str(s) for s in parsed.get("reachable_stop_names", [])]
    estimate = parsed.get("estimate_count")
    return listed, int(estimate) if estimate is not None else None


def print_report(result: GradeResult) -> None:
    print(f"Ground truth (SQL, walking-aware graph): {result.truth_name_count} distinct stop names")
    print(f"Model listed:   {result.listed_total} names")
    if result.estimate_count is not None:
        print(f"Model estimate: {result.estimate_count} (true total: {result.truth_name_count})")
    pct_strict = result.strict_hits / max(result.listed_total, 1) * 100
    pct_loose = result.loose_hits / max(result.listed_total, 1) * 100
    print(f"Strict matches: {result.strict_hits}/{result.listed_total} ({pct_strict:.0f}%)")
    print(f"Loose matches:  {result.loose_hits}/{result.listed_total} ({pct_loose:.0f}%)")
    print(
        f"Recall vs truth (loose, by listed names only): "
        f"{result.loose_hits}/{result.truth_name_count} "
        f"= {result.loose_hits / max(result.truth_name_count, 1) * 100:.2f}%"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--live", action="store_true", help="hit the Anthropic API")
    args = parser.parse_args(argv)

    if not args.db.exists():
        print(f"DuckDB not found at {args.db}; run `make data` first.", file=sys.stderr)
        return 1

    if args.live:
        listed, estimate = call_live_model()  # pragma: no cover
    else:
        if not args.baseline.exists():
            print(f"Baseline file not found: {args.baseline}", file=sys.stderr)
            return 1
        listed, estimate = load_baseline(args.baseline)
        print(f"Loaded baseline: {args.baseline}")

    con = duckdb.connect(str(args.db), read_only=True)
    con.execute("LOAD spatial;")
    truth = get_truth_stop_names(con, ORIGIN_STOP_ID, WALKING_M)
    con.close()

    result = grade(listed, truth, estimate)
    print_report(result)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
