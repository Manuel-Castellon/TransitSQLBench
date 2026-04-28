# SpatialBench

**A benchmark and evaluation harness for geospatial analytics agents, grounded in Israeli open transit data.**

Status: early-stage. Stage 1 data foundation is complete; Stage 2 benchmark curation is next.

---

## The problem

Teams are shipping LLM-powered analytics agents ("ask your data a question in English") into products at a remarkable pace. The agents answer business questions by generating SQL, calling tools, and summarizing results. When they work, they feel magical. When they fail, they fail *silently and confidently* — returning a number, a chart, and a paragraph of prose that looks right and isn't.

Two specific pain points sit underneath that:

### 1. Evaluation blindness
Classic software has pytest. LLM agents have vibes. When a team changes a prompt, upgrades a model, or adds a new tool, they ship it because "it looked better on five examples the PM tried." Regressions are detected by users, in production, often weeks later. There is no pytest-equivalent that says *"this prompt change fixed 12 questions but broke 4, and here are the 4."*

Building that harness is not hard in principle. It is tedious, opinionated, and nobody's 20% project. So most teams don't, and keep shipping blind.

### 2. Geospatial reasoning is the LLM blind spot
Off-the-shelf text-to-SQL agents handle *"top 10 customers by revenue"* fine. They fall apart on:

> *"Which bus stops in Tel Aviv are within 500m of a school but more than 1km from any light rail station, and how does their morning ridership compare to the city average?"*

Spatial SQL (PostGIS, H3, geohashes, projections) is underrepresented in the training data that teaches LLMs to write SQL. Units and coordinate reference systems get silently ignored. Spatial joins are computationally and semantically different from relational joins. The existing text-to-SQL benchmarks (Spider, BIRD, WikiSQL) are overwhelmingly relational — a spatially-capable agent and a spatially-naive one score the same on them.

### The gap this fills

There is no widely-used benchmark for *"how good is your analytics agent at spatial questions?"* and no widely-used harness for detecting regressions between agent versions on such questions. SpatialBench is both: a curated question set with ground-truth answers, plus the evaluation infrastructure to run any agent against it and diff two runs.

## Who feels this pain

- **Mobility companies** (Waze, Moovit, Via, Uber, Lyft) — internal data-science teams run spatial queries constantly, and some ship user-facing analytics agents built on top.
- **Logistics and delivery** — routing, warehousing, service-area analysis.
- **Urban planning and transit agencies** — accessibility studies, coverage gaps, equity analysis.
- **GIS platforms** (ESRI, Mapbox, CARTO) — their customers increasingly ask "can your AI handle spatial?"
- **Real estate analytics** — every query is implicitly spatial.

## The approach

Three pieces, built in this order:

1. **The benchmark**: ~50 questions (v1) over a single curated dataset, each with a reference SQL query, a reference answer, and difficulty + capability tags. This is the core contribution and most of the real work.
2. **A reference agent**: a geospatial-native analytics agent built on the benchmark's dataset. Not the point of the project, but you need one to validate the benchmark.
3. **The eval harness**: runs any agent over the benchmark, grades with pluggable graders (exact match, numeric tolerance, LLM-judge), stores runs with agent + prompt version, and surfaces regressions between runs.

## Why Israeli transit data

The core dataset is the [Israeli Ministry of Transport GTFS feed](https://www.gov.il/he/departments/general/gtfs_general_transit_feed_specifications) — one of the most complete national transit feeds in the world. Buses, rail, light rail, updated weekly.

Joinable with:
- **OpenStreetMap** — roads, POIs, amenities.
- **Israeli CBS** — demographic data by statistical area, geocoded.
- **Tel Aviv open data portal** — parking, bike share history, traffic counters.
- **Israel Meteorological Service** — weather.

This choice gives us real multi-source joins, genuine spatial complexity (not toy polygons), and a domain with authentic business questions. It is also a concrete, single-region dataset — deliberately narrower than "world-scale" to keep the benchmark tractable.

## Scope

### v1 (current)
- One dataset: Israeli GTFS snapshot (pinned by date).
- 50 benchmark questions across 5 difficulty tiers.
- One reference agent (Claude + local Ollama fallback).
- Eval harness with 3 graders, SQLite run storage.
- Streamlit diff UI for comparing two runs.
- Running locally in one command.

### v2 (only if v1 sings)
- Scale benchmark to 200 questions, add OSM + CBS joins.
- Train a small query-router classifier (the MLOps slice).
- Publish benchmark + write up findings.
- Infrastructure as code + containerized deploy to a cloud provider.
- Multi-agent comparison (Claude vs GPT vs local models).

## Cost model

Designed to run on ~$0–100 total API spend across the full v1 build.

| Tier | When | Rough cost |
|---|---|---|
| Local Ollama (Qwen / Llama) | Dev loop, fast iteration | Free |
| Gemini Flash free tier | Eval runs during development | Free |
| Claude Haiku 4.5 (with prompt caching) | "Real" eval runs, judge | Cents per run |
| Claude Sonnet 4.6 | Final comparison runs only | Dollars per run |
| Claude Opus 4.7 | Avoided unless experiment demands | — |

## Status

- [x] Stage 0: Project scaffold and business case
- [x] Stage 1: Data foundation (GTFS ingest → DuckDB)
- [ ] Stage 2: Benchmark curation v1 (~50 questions) ← *you are here*
- [ ] Stage 3: Reference agent (baseline loop)
- [ ] Stage 4: Evaluation harness
- [ ] Stage 5: Diff UI
- [ ] Stage 6: Writeup + first published finding

See [ROADMAP.md](./ROADMAP.md) for detail.

## Quickstart

```bash
make data
make queries STOP=22633
make check
```
