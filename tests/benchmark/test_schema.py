"""Tests for transitsqlbench.benchmark.schema."""

from pathlib import Path

import pytest
import yaml

from transitsqlbench.benchmark.schema import (
    BenchmarkFile,
    BenchmarkLoadError,
    ReferenceAnswer,
    load_benchmark,
    main,
    validate_benchmark,
)


def _minimal_q(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "id": "q001",
        "text": "How many stops?",
        "difficulty": 1,
        "tags": [],
        "answer_type": "scalar",
        "reference_sql": "SELECT COUNT(*) FROM stops",
        "reference_answer": {"value": 42},
    }
    base.update(overrides)
    return base


def _write_yaml(tmp_path: Path, data: object) -> Path:
    p = tmp_path / "questions.yaml"
    p.write_text(yaml.dump(data, allow_unicode=True))
    return p


# ── ReferenceAnswer model ──────────────────────────────────────────────


class TestReferenceAnswer:
    def test_value_only(self) -> None:
        a = ReferenceAnswer(value=42)
        assert a.value == 42
        assert a.count is None

    def test_list_value(self) -> None:
        a = ReferenceAnswer(value=["a", "b"])
        assert a.value == ["a", "b"]

    def test_summary_only(self) -> None:
        a = ReferenceAnswer(count=100, sample=["x", "y"])
        assert a.count == 100
        assert a.value is None

    def test_both_value_and_summary_rejected(self) -> None:
        with pytest.raises(ValueError, match="not both"):
            ReferenceAnswer(value=42, count=10, sample=["x"])

    def test_neither_value_nor_summary_rejected(self) -> None:
        with pytest.raises(ValueError, match="either"):
            ReferenceAnswer()

    def test_count_without_sample_rejected(self) -> None:
        with pytest.raises(ValueError, match="summary requires both"):
            ReferenceAnswer(count=10)

    def test_sample_without_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="summary requires both"):
            ReferenceAnswer(sample=["x"])


# ── load_benchmark ─────────────────────────────────────────────────────


class TestLoadBenchmark:
    def test_valid_file(self, tmp_path: Path) -> None:
        data = {"schema_version": 1, "questions": [_minimal_q()]}
        p = _write_yaml(tmp_path, data)
        bench = load_benchmark(p)
        assert isinstance(bench, BenchmarkFile)
        assert len(bench.questions) == 1

    def test_empty_questions(self, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": []})
        bench = load_benchmark(p)
        assert bench.questions == []

    def test_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(BenchmarkLoadError, match="File not found"):
            load_benchmark(tmp_path / "missing.yaml")

    def test_malformed_yaml(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.yaml"
        p.write_text(":\n  :\n    - [invalid")
        with pytest.raises(BenchmarkLoadError, match="YAML parse error"):
            load_benchmark(p)

    def test_non_dict_yaml(self, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, [1, 2, 3])
        with pytest.raises(BenchmarkLoadError, match="Expected YAML mapping"):
            load_benchmark(p)

    def test_invalid_schema_version(self, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"schema_version": 99, "questions": []})
        with pytest.raises(BenchmarkLoadError):
            load_benchmark(p)

    def test_invalid_difficulty(self, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [_minimal_q(difficulty=0)]})
        with pytest.raises(BenchmarkLoadError):
            load_benchmark(p)

    def test_invalid_seed_shape(self, tmp_path: Path) -> None:
        p = _write_yaml(
            tmp_path, {"schema_version": 1, "questions": [_minimal_q(seed_shape="q99")]}
        )
        with pytest.raises(BenchmarkLoadError):
            load_benchmark(p)

    def test_valid_seed_shape(self, tmp_path: Path) -> None:
        data = {"schema_version": 1, "questions": [_minimal_q(seed_shape="q3")]}
        p = _write_yaml(tmp_path, data)
        bench = load_benchmark(p)
        assert bench.questions[0].seed_shape == "q3"

    def test_optional_notes(self, tmp_path: Path) -> None:
        data = {"schema_version": 1, "questions": [_minimal_q(notes="some clarification")]}
        p = _write_yaml(tmp_path, data)
        bench = load_benchmark(p)
        assert bench.questions[0].notes == "some clarification"


# ── validate_benchmark ─────────────────────────────────────────────────


class TestValidateBenchmark:
    def test_valid_scalar(self, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [_minimal_q()]})
        r = validate_benchmark(p)
        assert r.ok is True
        assert r.question_count == 1
        assert r.errors == []
        assert r.warnings == []

    def test_valid_list_answer(self, tmp_path: Path) -> None:
        q = _minimal_q(answer_type="list", reference_answer={"value": ["R1", "R2"]})
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q]})
        r = validate_benchmark(p)
        assert r.ok is True

    def test_valid_set_summary(self, tmp_path: Path) -> None:
        q = _minimal_q(answer_type="set", reference_answer={"count": 500, "sample": ["s1", "s2"]})
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q]})
        r = validate_benchmark(p)
        assert r.ok is True

    def test_empty_questions_warns(self, tmp_path: Path) -> None:
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": []})
        r = validate_benchmark(p)
        assert r.ok is True
        assert "No questions" in r.warnings[0]

    def test_load_error_returns_failure(self, tmp_path: Path) -> None:
        r = validate_benchmark(tmp_path / "missing.yaml")
        assert r.ok is False
        assert r.question_count == 0

    def test_duplicate_ids(self, tmp_path: Path) -> None:
        q1 = _minimal_q()
        q2 = _minimal_q(text="Different question")
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q1, q2]})
        r = validate_benchmark(p)
        assert r.ok is False
        assert any("Duplicate" in e for e in r.errors)

    def test_unknown_tag_warns(self, tmp_path: Path) -> None:
        q = _minimal_q(tags=["spatial_join", "made_up_tag"])
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q]})
        r = validate_benchmark(p)
        assert r.ok is True
        assert any("unknown tag" in w for w in r.warnings)

    def test_scalar_with_list_value_errors(self, tmp_path: Path) -> None:
        q = _minimal_q(answer_type="scalar", reference_answer={"value": [1, 2, 3]})
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q]})
        r = validate_benchmark(p)
        assert r.ok is False
        assert any("scalar" in e and "list" in e for e in r.errors)

    def test_scalar_with_summary_errors(self, tmp_path: Path) -> None:
        q = _minimal_q(answer_type="scalar", reference_answer={"count": 10, "sample": ["a"]})
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q]})
        r = validate_benchmark(p)
        assert r.ok is False
        assert any("requires 'value'" in e for e in r.errors)

    def test_list_with_non_list_value_errors(self, tmp_path: Path) -> None:
        q = _minimal_q(answer_type="list", reference_answer={"value": 42})
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q]})
        r = validate_benchmark(p)
        assert r.ok is False
        assert any("not a list" in e for e in r.errors)

    def test_set_with_non_list_value_errors(self, tmp_path: Path) -> None:
        q = _minimal_q(answer_type="set", reference_answer={"value": "oops"})
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q]})
        r = validate_benchmark(p)
        assert r.ok is False
        assert any("not a list" in e for e in r.errors)

    def test_set_with_summary_ok(self, tmp_path: Path) -> None:
        q = _minimal_q(answer_type="set", reference_answer={"count": 14433, "sample": ["10000"]})
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q]})
        r = validate_benchmark(p)
        assert r.ok is True

    def test_list_with_summary_ok(self, tmp_path: Path) -> None:
        q = _minimal_q(answer_type="list", reference_answer={"count": 20, "sample": ["R1", "R2"]})
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [q]})
        r = validate_benchmark(p)
        assert r.ok is True


# ── main CLI ───────────────────────────────────────────────────────────


class TestMain:
    def test_valid_file_returns_zero(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": [_minimal_q()]})
        rc = main([str(p)])
        assert rc == 0
        assert "OK: 1 questions" in capsys.readouterr().out

    def test_invalid_file_returns_one(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = main([str(tmp_path / "missing.yaml")])
        assert rc == 1
        assert "ERROR" in capsys.readouterr().out

    def test_warnings_printed(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        p = _write_yaml(tmp_path, {"schema_version": 1, "questions": []})
        rc = main([str(p)])
        assert rc == 0
        out = capsys.readouterr().out
        assert "WARNING" in out
        assert "OK: 0 questions" in out
