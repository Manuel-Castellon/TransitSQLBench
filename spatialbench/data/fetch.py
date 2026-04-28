"""
Download and verify the GTFS feed.

Usage:
    python -m spatialbench.data.fetch          # verify existing file against manifest
    python -m spatialbench.data.fetch --update # re-fetch, recompute hash, rewrite manifest
"""

import argparse
import hashlib
import sys
from datetime import date
from pathlib import Path

import requests
import yaml
from pydantic import BaseModel, field_validator

MANIFEST_PATH = Path(__file__).parent / "manifest.yaml"
RAW_DIR = Path(__file__).parent.parent.parent / "data" / "raw"
CHUNK_SIZE = 1 << 20  # 1 MB


class Manifest(BaseModel):
    url: str
    filename: str
    sha256: str | None = None
    size_bytes: int | None = None
    snapshot_date: date | None = None

    @field_validator("url")
    @classmethod
    def _require_http(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError(f"url must be http(s), got: {v!r}")
        return v


class HashMismatchError(ValueError):
    def __init__(self, path: Path, expected: str, actual: str) -> None:
        super().__init__(
            f"Hash mismatch for {path.name}\n  expected: {expected}\n  actual:   {actual}"
        )
        self.path = path
        self.expected = expected
        self.actual = actual


def load_manifest(path: Path = MANIFEST_PATH) -> Manifest:
    with open(path) as f:
        return Manifest.model_validate(yaml.safe_load(f))


def save_manifest(manifest: Manifest, path: Path = MANIFEST_PATH) -> None:
    with open(path, "w") as f:
        yaml.dump(
            manifest.model_dump(mode="json"),
            f,
            default_flow_style=False,
            allow_unicode=True,
        )


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
            h.update(chunk)
    return h.hexdigest()


def _download(url: str, dest: Path) -> None:
    print(f"Fetching {url} → {dest}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)


def fetch(
    update: bool = False,
    manifest_path: Path = MANIFEST_PATH,
    raw_dir: Path = RAW_DIR,
) -> Path:
    manifest = load_manifest(manifest_path)
    dest = raw_dir / manifest.filename

    if update or not dest.exists():
        _download(manifest.url, dest)
        digest = _sha256(dest)
        save_manifest(
            manifest.model_copy(
                update={
                    "sha256": digest,
                    "size_bytes": dest.stat().st_size,
                    "snapshot_date": date.today(),
                }
            ),
            manifest_path,
        )
        print(f"Manifest updated — sha256={digest[:12]}…")
        return dest

    if manifest.sha256 is None:
        print("Warning: manifest has no sha256 — run with --update to populate.")
        return dest

    actual = _sha256(dest)
    if actual != manifest.sha256:
        raise HashMismatchError(dest, manifest.sha256, actual)
    print(f"Verified {dest.name} ({(manifest.size_bytes or 0) // 1_000_000} MB)")
    return dest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--update", action="store_true")
    args = parser.parse_args(argv)
    update: bool = bool(args.update)
    try:
        fetch(update=update)
    except HashMismatchError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
