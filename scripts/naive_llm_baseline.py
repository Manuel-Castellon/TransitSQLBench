"""
One-off: ask Haiku 4.5 to estimate Tier 5 reachability with no tools.

Run with:
    uv run --with anthropic python scripts/naive_llm_baseline.py

Compares the model's free-form list against the SQL ground truth.
This is exploratory; if/when we promote LLM agents to a permanent module
(Stage 3), the patterns here move into spatialbench/agents/.
"""

import json
import os
import sys
from pathlib import Path

import anthropic
import duckdb

DB = Path(__file__).parent.parent / "data" / "spatialbench.duckdb"
ORIGIN_STOP_ID = 22633  # Ben Gurion Airport Terminal 1
WALKING_M = 400
MODEL = "claude-haiku-4-5"


def get_stop_name(con: duckdb.DuckDBPyConnection, stop_id: int) -> str:
    row = con.execute("SELECT stop_name FROM stops WHERE stop_id = ?", [stop_id]).fetchone()
    assert row is not None
    return str(row[0])


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


PROMPT_TEMPLATE = """You are a transit-routing assistant for the Israeli national bus network.

Question: A passenger is starting at the bus stop "{origin_name}" (stop_id {origin_id}) at Ben Gurion International Airport.

They will:
  1. Take exactly one bus from this stop to some other stop Y.
  2. Optionally walk up to {walking_m} metres to a different stop Y'.
  3. Take exactly one more bus from Y' to a final stop Z.

List the stops Z (by name, in Hebrew or English) that you believe are reachable this way.

Return a JSON object with this exact shape — nothing else, no prose:
{{"reachable_stop_names": ["stop name 1", "stop name 2", ...], "estimate_count": <integer>}}

Be honest about uncertainty: if you don't actually know which routes serve this stop, say so by listing only what you're confident about. Do not invent specific stop names you have no basis for. The estimate_count is your best guess at the *true* number of distinct reachable stops, which may be larger than the list you can name."""


def ask_model(client: anthropic.Anthropic, origin_name: str) -> dict[str, object]:
    msg = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": PROMPT_TEMPLATE.format(
                    origin_name=origin_name,
                    origin_id=ORIGIN_STOP_ID,
                    walking_m=WALKING_M,
                ),
            }
        ],
    )
    raw = msg.content[0].text  # type: ignore[union-attr]
    # Strip markdown fences if present.
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return dict(json.loads(text))


def main() -> None:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set", file=sys.stderr)
        sys.exit(1)
    if not DB.exists():
        print(f"DuckDB not found at {DB}; run `make data` first.", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(str(DB), read_only=True)
    con.execute("LOAD spatial;")

    origin_name = get_stop_name(con, ORIGIN_STOP_ID)
    truth_names = get_truth_stop_names(con, ORIGIN_STOP_ID, WALKING_M)
    truth_set = {n.strip() for n in truth_names}
    print(f"Origin: {origin_name} (stop_id {ORIGIN_STOP_ID})")
    print(f"Ground truth (SQL, spatial-aware): {len(truth_set)} distinct stop names\n")

    client = anthropic.Anthropic()
    print(f"Asking {MODEL} (no tools, no GTFS context)…\n")
    answer = ask_model(client, origin_name)

    listed = [str(s).strip() for s in answer.get("reachable_stop_names", [])]  # type: ignore[arg-type]
    estimate = answer.get("estimate_count", "?")

    overlap = [n for n in listed if n in truth_set]

    print(f"Model estimate of total count: {estimate}")
    print(f"Model listed:                 {len(listed)} names")
    print(f"Of those, present in truth:   {len(overlap)} ({len(overlap) / max(len(listed), 1):.0%})")
    print(f"True total:                   {len(truth_set)}")
    print(f"\nFirst 10 names the model listed:")
    for n in listed[:10]:
        mark = "✓" if n in truth_set else "✗"
        print(f"  {mark}  {n}")


if __name__ == "__main__":
    main()
