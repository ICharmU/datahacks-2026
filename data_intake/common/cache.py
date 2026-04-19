from __future__ import annotations

from pathlib import Path
import json

from .utils import ensure_dir


def get_source_cache_dir(cache_root: Path, source_name: str) -> Path:
    return ensure_dir(cache_root / source_name)


def cache_metadata_path(local_path: Path) -> Path:
    return local_path.with_suffix(local_path.suffix + ".meta.json")


def load_cache_metadata(local_path: Path) -> dict | None:
    meta_path = cache_metadata_path(local_path)
    if not meta_path.exists():
        return None
    return json.loads(meta_path.read_text(encoding="utf-8"))


def save_cache_metadata(local_path: Path, metadata: dict) -> None:
    meta_path = cache_metadata_path(local_path)
    meta_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")


def should_redownload(local_path: Path, *, force_refresh: bool) -> bool:
    if force_refresh:
        return True
    return not local_path.exists()