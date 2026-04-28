.PHONY: data fetch fetch-update load queries test lint typecheck check

fetch:
	uv run python -m spatialbench.data.fetch

fetch-update:
	uv run python -m spatialbench.data.fetch --update

load:
	uv run python -m spatialbench.data.load

data: fetch load

# Stage 1 demo: needs an origin stop_id from the loaded feed; override with STOP=…
STOP ?=
queries:
	@if [ -z "$(STOP)" ]; then echo "usage: make queries STOP=<stop_id>"; exit 1; fi
	uv run python -m spatialbench.queries.cli all --stop $(STOP)

test:
	uv run pytest

lint:
	uv run ruff check spatialbench tests
	uv run ruff format --check spatialbench tests

typecheck:
	uv run mypy spatialbench tests

check: lint typecheck test
