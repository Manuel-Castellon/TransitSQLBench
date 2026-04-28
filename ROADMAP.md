# TransitSQLBench Roadmap

A staged build. Each stage has a clear goal, an acceptance criterion that proves it's done, the key decisions that stage forces, and what gets deliberately deferred. Stages are sequential — later stages depend on earlier ones.

Effort estimates assume part-time work (evenings / weekends). They are planning fiction; reality will vary.

---

## Stage 0 — Scaffold and business case
**Goal**: Enough of a skeleton that the project has a spine, without writing code that will need to be thrown away.

**Acceptance**: `README.md` and `ROADMAP.md` exist, and the business case in the README would convince a skeptical reader that this project addresses a real gap.

**Key decisions made here**:
- Benchmark-first framing (the eval harness is the hero, not the agent).
- Israeli GTFS as the anchor dataset.
- v1 / v2 split to bound scope.

**Deferred**: all code, all dependencies, all infrastructure.

**Effort**: one session.

---

## Stage 1 — Data foundation
**Goal**: A reproducible, read-only snapshot of the Israeli GTFS feed, loaded into DuckDB, with the schema documented and a handful of hand-written SQL queries that exercise the interesting shapes.

**Acceptance**:
- One command (`make data` or equivalent) takes a fresh clone to a queryable DuckDB file.
- The GTFS snapshot is pinned by date and checksum so the benchmark is reproducible.
- A notebook or script demonstrates 5–10 reference queries of varying spatial complexity, *written and understood by you*. These become the seed for Stage 2.

**Key decisions**:
- Package manager: `uv` (fast, modern) vs `poetry` vs `pip-tools`.
- DuckDB spatial extension vs PostGIS — DuckDB is simpler and fast enough at this scale; PostGIS is a Stage 7+ decision if we need it.
- How to pin the feed: commit the raw zip to Git LFS, or store a hash and re-fetch? Probably hash + re-fetch, with a cached copy for offline dev.

**Deferred**: OSM, CBS, weather. Stage 1 is *one* dataset, known cold.

**The learning goal**: you understand the GTFS schema well enough to look at any question and sketch the query shape without opening docs.

**Effort**: 1–2 weeks part-time.

---

## Stage 2 — Benchmark curation v1
**Goal**: 50 questions with reference SQL, reference answers, and tags. This is the *hardest* and *most valuable* stage. Do not rush it.

**Acceptance**:
- `benchmark/v1/questions.yaml` (or similar) with 50 entries.
- Each entry has: question text (English), reference SQL, reference answer, difficulty tier, capability tags.

**Two orthogonal axes** — keep them separate:

- **Difficulty tier** (one of five buckets, single-valued):
  1. **Lookup**: "How many bus stops are in Haifa?"
  2. **Aggregate**: "What is the median service frequency on line 5 between 7–9am?"
  3. **Relational join**: "Which operators run the most trips that end at rail stations?"
  4. **Spatial**: "Which neighborhoods have no bus stop within 300m?"
  5. **Multi-step reasoning**: "If line 142 were cancelled, how many trips per day would lose their only direct connection to a light-rail station?"
- **Capability tags** (multi-valued, what the question *probes*):
  `spatial_join`, `projection_aware`, `temporal_filter`, `set_reasoning`, `walking_transfer`,
  `null_handling`, `ambiguity_resolution`. A single Spatial-tier question can carry several tags.

The `q1`..`q5` *seed query shapes* in `transitsqlbench/queries/reference.py` are not the same
thing as difficulty tiers — they are five concrete query templates the benchmark questions are
drawn from. Do not conflate the two vocabularies in the question file.

**Scope boundary**:
- Focus on SQL answer generation over a fixed transit analytics schema.
- Do not drift into general GIS workflow/code generation; GeoAnalystBench and GeoAgentBench already cover that neighborhood.
- Do not drift into generic visual/spatial cognition; SpatialEval and SpatialBench already cover that neighborhood.
- Do not duplicate TransitGPT's core claim that LLMs can answer GTFS questions by generating Python. Our benchmark should isolate SQL-agent behavior, spatial SQL semantics, and regression detection.

**Key decisions**:
- Schema for the question file (versioning matters — benchmark will evolve).
- How to encode "reference answer" when the answer is a list or a chart, not a scalar.
- How to handle ambiguous questions — do we have a "multiple acceptable answers" field?
- Hebrew variants? Probably defer to v2.

**Deferred**: grading logic (Stage 4); question-writing tools; crowd sourcing.

**The learning goal**: you can defend every single question — *why is this interesting, what capability does it probe, what's the failure mode it exposes?*

**Effort**: 2–3 weeks part-time. This is the stage most likely to expand.

---

## Stage 3 — Reference agent (baseline)
**Goal**: A minimum-viable agent that, given a question and the GTFS schema, attempts to produce an answer. It will be bad at many questions. That is fine and in fact necessary — we need failure modes to validate the benchmark.

**Acceptance**:
- `transitsqlbench agent run "question"` returns an answer + the SQL it ran + a trace of tool calls.
- Agent loop: inspect-schema → generate-SQL → execute → summarize, with at most one retry on SQL failure.
- Works against both a local Ollama model and the Anthropic API, toggled by config.
- Trace is structured JSON so Stage 4 can grade it.

**Key decisions**:
- Tool-use framing: raw Anthropic SDK tool use, or a lightweight framework? Raw SDK keeps the mechanics visible; frameworks hide behavior that matters when debugging. Lean raw.
- How much schema to inject per-call (all of it vs retrieval-augmented).
- Prompt caching strategy — the system prompt and schema block should be cached.

**Deferred**: the query router classifier (v2), multi-turn clarification, chart generation.

**The learning goal**: you can draw the agent control flow on a whiteboard and explain every arrow.

**Effort**: 1–2 weeks part-time.

---

## Stage 4 — Evaluation harness
**Goal**: Run the agent over the full benchmark, grade each result, and store the run. This is the *actual* contribution of the project.

**Acceptance**:
- `transitsqlbench eval run --agent <version>` runs the full benchmark and writes a row per question to SQLite.
- Three graders, each pluggable:
  1. **Exact match** — for scalar answers.
  2. **Numeric tolerance** — for aggregates where the agent's grouping may differ slightly.
  3. **LLM-judge** — semantic match against reference answer, with a structured rubric and a cheap judge model.
- Run record includes: agent version, prompt hash, model used, timestamps, per-question pass/fail, grader rationale, cost in tokens and dollars.
- A one-line summary: `47/50 passed on v1.2 (was 44/50 on v1.1, +3 gained, 0 regressed)`.

**Key decisions**:
- Run storage schema (SQLite). It should be easy to query for regressions later.
- How to handle flaky questions (LLM nondeterminism) — N runs and majority vote? Defer probably.
- Grader prompt design for the LLM judge — this itself needs to be evaluated for bias.

**Deferred**: distributed execution, queue-based runs, cost-optimization beyond caching.

**The learning goal**: you can explain why evaluating LLM systems is genuinely hard and what your three graders buy you.

**Effort**: 1–2 weeks part-time.

---

## Stage 5 — Diff UI
**Goal**: A human can look at two runs and see, at a glance, what got better and what got worse.

**Acceptance**:
- Streamlit app: select two runs, see a table of per-question outcomes, filter to "gained" / "regressed" / "unchanged".
- Click a regression to see: the question, both agents' SQL, both answers, the grader's rationale.
- Runs are loaded from the SQLite store, no other setup.

**Key decisions**:
- Streamlit vs a tiny FastAPI + HTMX thing vs Gradio. Streamlit wins on effort-to-value.
- How to visualize the SQL diff — side-by-side text diff, or syntax-highlighted AST diff? Side-by-side is fine.

**Deferred**: authentication, multi-user runs, dashboards.

**Effort**: 3–5 days part-time.

---

## Stage 6 — Writeup and first finding
**Goal**: You have something concrete to show and talk about.

**Acceptance**:
- `README.md` upgraded from "early-stage" to "v1 shipped."
- A `FINDINGS.md` or blog-post-style document explaining 3 concrete things the benchmark revealed — e.g. "GPT-4o fails projection-aware questions 60% of the time; Claude Haiku 4.5 gets them 80% right because it defaults to WGS84 awareness." Real numbers.
- A design-doc-quality write-up of the eval harness architecture.

**Deferred**: all of v2.

**Effort**: 1 week part-time.

---

## v2 and beyond (not committed)

Only if v1 is working end-to-end and you still want to push. In rough order:
- Add OSM and CBS joins, scale benchmark to 200 questions.
- Train and deploy the query-router classifier (the MLOps slice).
- Publish the benchmark publicly, solicit agent submissions.
- IaC (Terraform) + containerized deploy to GCP (the infra narrative).
- Multi-agent comparison harness, leaderboard-style.
- Hebrew-language question variants.

---

## Rules of the road

1. **Do not start a stage before the prior stage's acceptance criteria are met.** Half-built stages rot.
2. **Every stage ends with a write-up of what was decided and why.** Decisions not written down are decisions that have to be re-made.
3. **When a stage expands past its effort estimate by 2x, stop and reassess scope** — probably you are building v2 early.
4. **If the benchmark questions in Stage 2 start to feel synthetic, stop and go find real questions** — ask someone on a transit-ops team, look at Stack Overflow, read urban planning papers. Synthetic benchmarks are why text-to-SQL evaluation is in the state it's in.
