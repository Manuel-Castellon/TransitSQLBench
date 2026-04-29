"""Pydantic models and validation for the benchmark question file."""

import argparse
import sys
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator
from pydantic import ValidationError as PydanticValidationError

KNOWN_TAGS: frozenset[str] = frozenset(
    {
        "spatial_join",
        "projection_aware",
        "temporal_filter",
        "set_reasoning",
        "walking_transfer",
        "null_handling",
        "ambiguity_resolution",
    }
)


class BenchmarkLoadError(Exception):
    pass


class ReferenceAnswer(BaseModel):
    value: int | float | str | list[int | float | str] | None = None
    count: int | None = None
    sample: list[int | float | str] | None = None

    @model_validator(mode="after")
    def _check_value_or_summary(self) -> "ReferenceAnswer":
        has_value = self.value is not None
        has_summary = self.count is not None or self.sample is not None
        if has_value and has_summary:
            msg = "must have 'value' or 'count'+'sample', not both"
            raise ValueError(msg)
        if not has_value and not has_summary:
            msg = "must have either 'value' or 'count'+'sample'"
            raise ValueError(msg)
        if has_summary and (self.count is None or self.sample is None):
            msg = "summary requires both 'count' and 'sample'"
            raise ValueError(msg)
        return self


class Question(BaseModel):
    id: str = Field(min_length=1)
    text: str = Field(min_length=1)
    difficulty: int = Field(ge=1, le=5)
    tags: list[str]
    seed_shape: Literal["q1", "q2", "q3", "q4", "q5"] | None = None
    answer_type: Literal["scalar", "list", "set"]
    reference_sql: str = Field(min_length=1)
    reference_answer: ReferenceAnswer
    notes: str | None = None


class BenchmarkFile(BaseModel):
    schema_version: Literal[1]
    questions: list[Question]


class ValidationResult(BaseModel):
    ok: bool
    errors: list[str]
    warnings: list[str]
    question_count: int


def load_benchmark(path: Path) -> BenchmarkFile:
    try:
        raw = yaml.safe_load(path.read_text())
    except FileNotFoundError:
        raise BenchmarkLoadError(f"File not found: {path}") from None
    except yaml.YAMLError as exc:
        raise BenchmarkLoadError(f"YAML parse error: {exc}") from None
    if not isinstance(raw, dict):
        raise BenchmarkLoadError(f"Expected YAML mapping at top level, got {type(raw).__name__}")
    try:
        return BenchmarkFile.model_validate(raw)
    except PydanticValidationError as exc:
        raise BenchmarkLoadError(str(exc)) from None


def validate_benchmark(path: Path) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    try:
        bench = load_benchmark(path)
    except BenchmarkLoadError as exc:
        return ValidationResult(ok=False, errors=[str(exc)], warnings=[], question_count=0)

    seen_ids: set[str] = set()
    for q in bench.questions:
        if q.id in seen_ids:
            errors.append(f"Duplicate question id: {q.id}")
        seen_ids.add(q.id)

        for tag in q.tags:
            if tag not in KNOWN_TAGS:
                warnings.append(f"Question {q.id}: unknown tag '{tag}'")

        ans = q.reference_answer
        if q.answer_type == "scalar":
            if isinstance(ans.value, list):
                errors.append(f"Question {q.id}: answer_type 'scalar' but value is a list")
            if ans.value is None:
                errors.append(f"Question {q.id}: answer_type 'scalar' requires 'value'")
        elif ans.value is not None and not isinstance(ans.value, list):
            errors.append(f"Question {q.id}: answer_type '{q.answer_type}' but value is not a list")

    if not bench.questions:
        warnings.append("No questions in benchmark file")

    return ValidationResult(
        ok=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        question_count=len(bench.questions),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate benchmark question file")
    parser.add_argument("path", type=Path)
    args = parser.parse_args(argv)
    result = validate_benchmark(args.path)
    for err in result.errors:
        print(f"ERROR: {err}")
    for warn in result.warnings:
        print(f"WARNING: {warn}")
    if result.ok:
        print(f"OK: {result.question_count} questions validated")
    return 0 if result.ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
