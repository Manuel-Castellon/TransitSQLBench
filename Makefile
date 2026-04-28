.PHONY: data fetch fetch-update load queries test lint typecheck check

fetch:
	uv run python -m transitsqlbench.data.fetch

fetch-update:
	uv run python -m transitsqlbench.data.fetch --update

load:
	uv run python -m transitsqlbench.data.load

data: fetch load

# Stage 1 demo: needs an origin stop_id from the loaded feed; override with STOP=…
STOP ?=
queries:
	@if [ -z "$(STOP)" ]; then echo "usage: make queries STOP=<stop_id>"; exit 1; fi
	uv run python -m transitsqlbench.queries.cli all --stop $(STOP)

test:
	uv run pytest

lint:
	uv run ruff check transitsqlbench tests
	uv run ruff format --check transitsqlbench tests

typecheck:
	uv run mypy transitsqlbench tests

check: lint typecheck test
