"""Tests for transitsqlbench.data.fetch."""

import hashlib
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
import yaml
from pydantic import ValidationError

from transitsqlbench.data.fetch import (
    HashMismatchError,
    Manifest,
    _download,
    _sha256,
    fetch,
    load_manifest,
    main,
    save_manifest,
)

# ── helpers ───────────────────────────────────────────────────────────────────


def _write_manifest(
    path: Path,
    url: str = "https://example.com/feed.zip",
    sha256: str | None = None,
    size_bytes: int | None = None,
    snapshot_date: str | None = None,
) -> None:
    data: dict[str, object] = {"url": url, "filename": "feed.zip"}
    if sha256 is not None:
        data["sha256"] = sha256
    if size_bytes is not None:
        data["size_bytes"] = size_bytes
    if snapshot_date is not None:
        data["snapshot_date"] = snapshot_date
    with open(path, "w") as f:
        yaml.dump(data, f)


def _mock_cm(chunks: list[bytes], raise_http_error: bool = False) -> MagicMock:
    """Context manager mock for requests.get(...) used with `with ... as r:`."""
    cm = MagicMock()
    cm.__enter__.return_value = cm
    cm.__exit__.return_value = False  # don't suppress exceptions
    cm.iter_content.return_value = iter(chunks)
    if raise_http_error:
        cm.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
    return cm


# ── Manifest ──────────────────────────────────────────────────────────────────


def test_manifest_defaults() -> None:
    m = Manifest(url="https://example.com/feed.zip", filename="feed.zip")
    assert m.sha256 is None
    assert m.size_bytes is None
    assert m.snapshot_date is None


def test_manifest_accepts_http() -> None:
    m = Manifest(url="http://example.com/feed.zip", filename="feed.zip")
    assert m.url.startswith("http://")


def test_manifest_accepts_https() -> None:
    m = Manifest(url="https://example.com/feed.zip", filename="feed.zip")
    assert m.url.startswith("https://")


def test_manifest_rejects_non_http_url() -> None:
    with pytest.raises(ValidationError):
        Manifest(url="ftp://example.com/feed.zip", filename="feed.zip")


# ── HashMismatchError ─────────────────────────────────────────────────────────


def test_hash_mismatch_error_fields(tmp_path: Path) -> None:
    p = tmp_path / "feed.zip"
    err = HashMismatchError(p, "aaa", "bbb")
    assert err.path == p
    assert err.expected == "aaa"
    assert err.actual == "bbb"
    assert "feed.zip" in str(err)
    assert "aaa" in str(err)
    assert "bbb" in str(err)
    assert isinstance(err, ValueError)


# ── _sha256 ───────────────────────────────────────────────────────────────────


def test_sha256_known_value(tmp_path: Path) -> None:
    data = b"hello world"
    f = tmp_path / "test.bin"
    f.write_bytes(data)
    assert _sha256(f) == hashlib.sha256(data).hexdigest()


def test_sha256_empty_file(tmp_path: Path) -> None:
    f = tmp_path / "empty.bin"
    f.write_bytes(b"")
    assert _sha256(f) == hashlib.sha256(b"").hexdigest()


# ── load_manifest / save_manifest ─────────────────────────────────────────────


def test_manifest_roundtrip(tmp_path: Path) -> None:
    p = tmp_path / "manifest.yaml"
    original = Manifest(
        url="https://example.com/feed.zip",
        filename="feed.zip",
        sha256="abc123def456",
        size_bytes=12345,
        snapshot_date=date(2026, 4, 24),
    )
    save_manifest(original, p)
    assert load_manifest(p) == original


def test_load_manifest_nullable_fields(tmp_path: Path) -> None:
    p = tmp_path / "manifest.yaml"
    _write_manifest(p)
    m = load_manifest(p)
    assert m.sha256 is None
    assert m.size_bytes is None
    assert m.snapshot_date is None


# ── _download ─────────────────────────────────────────────────────────────────


def test_download_creates_dirs_and_writes(tmp_path: Path) -> None:
    dest = tmp_path / "sub" / "feed.zip"
    content = b"fake zip content"
    with patch("transitsqlbench.data.fetch.requests.get", return_value=_mock_cm([content])):
        _download("https://example.com/feed.zip", dest)
    assert dest.read_bytes() == content


def test_download_handles_empty_body(tmp_path: Path) -> None:
    dest = tmp_path / "feed.zip"
    with patch("transitsqlbench.data.fetch.requests.get", return_value=_mock_cm([])):
        _download("https://example.com/feed.zip", dest)
    assert dest.read_bytes() == b""


def test_download_raises_on_http_error(tmp_path: Path) -> None:
    dest = tmp_path / "feed.zip"
    with (
        patch(
            "transitsqlbench.data.fetch.requests.get",
            return_value=_mock_cm([], raise_http_error=True),
        ),
        pytest.raises(requests.HTTPError),
    ):
        _download("https://example.com/feed.zip", dest)


# ── fetch ─────────────────────────────────────────────────────────────────────


def test_fetch_downloads_missing_file(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.yaml"
    _write_manifest(manifest_path)
    content = b"feed data"

    def fake_download(url: str, dest: Path) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)

    with patch("transitsqlbench.data.fetch._download", side_effect=fake_download):
        result = fetch(manifest_path=manifest_path, raw_dir=tmp_path)

    assert result == tmp_path / "feed.zip"
    updated = load_manifest(manifest_path)
    assert updated.sha256 == hashlib.sha256(content).hexdigest()
    assert updated.size_bytes == len(content)
    assert updated.snapshot_date is not None


def test_fetch_re_downloads_on_update(tmp_path: Path) -> None:
    content_before = b"old data"
    content_after = b"new data"
    feed = tmp_path / "feed.zip"
    feed.write_bytes(content_before)
    manifest_path = tmp_path / "manifest.yaml"
    _write_manifest(manifest_path, sha256=hashlib.sha256(content_before).hexdigest())

    def fake_download(url: str, dest: Path) -> None:
        dest.write_bytes(content_after)

    with patch("transitsqlbench.data.fetch._download", side_effect=fake_download):
        fetch(update=True, manifest_path=manifest_path, raw_dir=tmp_path)

    assert load_manifest(manifest_path).sha256 == hashlib.sha256(content_after).hexdigest()


def test_fetch_warns_when_no_hash(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    feed = tmp_path / "feed.zip"
    feed.write_bytes(b"some data")
    manifest_path = tmp_path / "manifest.yaml"
    _write_manifest(manifest_path)

    result = fetch(manifest_path=manifest_path, raw_dir=tmp_path)

    assert result == feed
    assert "Warning" in capsys.readouterr().out


def test_fetch_verifies_matching_hash(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    content = b"real feed data"
    feed = tmp_path / "feed.zip"
    feed.write_bytes(content)
    manifest_path = tmp_path / "manifest.yaml"
    _write_manifest(
        manifest_path,
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
    )

    result = fetch(manifest_path=manifest_path, raw_dir=tmp_path)

    assert result == feed
    assert "Verified" in capsys.readouterr().out


def test_fetch_raises_on_hash_mismatch(tmp_path: Path) -> None:
    feed = tmp_path / "feed.zip"
    feed.write_bytes(b"corrupted")
    manifest_path = tmp_path / "manifest.yaml"
    _write_manifest(manifest_path, sha256="a" * 64)

    with pytest.raises(HashMismatchError) as exc_info:
        fetch(manifest_path=manifest_path, raw_dir=tmp_path)

    assert exc_info.value.expected == "a" * 64
    assert exc_info.value.path == feed


# ── main ──────────────────────────────────────────────────────────────────────


def test_main_calls_fetch_without_update() -> None:
    with patch("transitsqlbench.data.fetch.fetch") as mock_fetch:
        main([])
    mock_fetch.assert_called_once_with(update=False)


def test_main_calls_fetch_with_update() -> None:
    with patch("transitsqlbench.data.fetch.fetch") as mock_fetch:
        main(["--update"])
    mock_fetch.assert_called_once_with(update=True)


def test_main_exits_on_hash_mismatch(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    err = HashMismatchError(tmp_path / "feed.zip", "aaa", "bbb")
    with (
        patch("transitsqlbench.data.fetch.fetch", side_effect=err),
        pytest.raises(SystemExit) as exc_info,
    ):
        main([])
    assert exc_info.value.code == 1
    assert "Hash mismatch" in capsys.readouterr().err
