# TransitSQLBench

**A regression-evaluation harness for SQL agents on public-transit analytics, built around the spatial-SQL failure modes that ordinary text-to-SQL benchmarks can't expose.**

Status: early-stage. Stage 1 data foundation is complete; Stage 2 benchmark curation is next.

TransitSQLBench is deliberately narrower than a general GeoAI benchmark. It focuses on one
question: when an LLM analytics agent writes SQL over production-scale public transit data, can
we tell whether a change made it better or worse — especially on the spatial-SQL failure modes
(projection awareness, ST_DWithin in meters, walking-distance transfers) that text-to-SQL
benchmarks miss because their source datasets contain no spatial data?

---

## The Problem

Teams are shipping LLM-powered analytics agents that answer business questions by generating SQL,
calling tools, and summarizing results. When they fail, they often fail silently: a plausible table,
number, or explanation appears, but the query encoded the wrong semantics.

Transit analytics is a useful stress test because even simple-sounding questions combine:

- GTFS joins across `routes`, `trips`, `stop_times`, calendars, and stops.
- Spatial SQL in meters, where coordinate reference systems matter.
- Temporal filters and GTFS service-day quirks such as times beyond 24:00:00.
- Accessibility and transfer questions that require set reasoning, not just lookup.

The project is the benchmark plus harness. The reference agent exists only to validate that the
benchmark exposes real regressions and improvements.

## What This Is Not

This project sits near several existing efforts, so the boundary is explicit:

- It is **not** a multimodal spatial cognition benchmark like SpatialBench or SpatialEval. There are
  no image, path-tracing, or mental-rotation tasks.
- It is **not** a database-engine performance benchmark like Apache Sedona SpatialBench. The goal is
  agent answer quality, not query-engine throughput.
- It is **not** a broad GIS workflow/code-generation benchmark like GeoAnalystBench or GeoAgentBench.
  The target output is SQL over a fixed analytical schema, not arbitrary GIS scripts or tool chains.
- It is **not** TransitGPT. TransitGPT shows that LLMs can answer GTFS questions by generating Python
  against GTFS feeds. TransitSQLBench instead emphasizes SQL agents, regression evaluation, spatial
  SQL semantics, and run-to-run diffing.

## The Approach

Three pieces, built in order:

1. **Benchmark questions**: curated natural-language transit analytics questions with reference SQL,
   reference answers, difficulty tiers, and capability tags.
2. **Reference agent**: a minimal SQL-generating agent that runs against the benchmark database and
   produces answers plus execution traces.
3. **Evaluation harness**: graders, run storage, and diff tooling to answer: which questions were
   gained, regressed, or unchanged between two agent versions?

## Data

The core dataset is the Israeli Ministry of Transport GTFS feed: a national feed with buses, rail,
and light rail. Stage 1 loads a pinned snapshot into DuckDB with:

- raw GTFS tables for stops, routes, trips, stop times, and calendars
- WGS84 stop geometry for display
- EPSG:2039 projected geometry for meter-based spatial queries
- parsed GTFS time columns for service-day arithmetic
- five reference seed queries (`q1`..`q5`) that exercise the join, set-reasoning, and spatial
  shapes Stage 2 benchmark questions will be drawn from

The current checked-in manifest pins the feed by URL, size, date, and SHA-256. The raw zip and
DuckDB database are generated locally and intentionally not committed.

## Scope

### v1

- One dataset: Israeli GTFS snapshot.
- 50 benchmark questions across transit SQL and spatial SQL difficulty tiers.
- One reference SQL agent.
- Evaluation harness with exact, numeric-tolerance, and semantic graders.
- SQLite run storage and a lightweight diff UI.

### Later

- OSM and demographic joins.
- More regions or agencies.
- Query-router experiments.
- Multi-agent or multi-model comparisons.

## Current Status

- [x] Stage 0: Project scaffold and business case
- [x] Stage 1: Data foundation (GTFS ingest to DuckDB)
- [ ] Stage 2: Benchmark curation v1 (~50 questions)
- [ ] Stage 3: Reference SQL agent
- [ ] Stage 4: Evaluation harness
- [ ] Stage 5: Diff UI
- [ ] Stage 6: Writeup and findings

See [ROADMAP.md](./ROADMAP.md) for detail.

## Quickstart

```bash
make data
make queries STOP=22633
make check
```

`make data` downloads/verifies the pinned GTFS feed and builds the local DuckDB database.
