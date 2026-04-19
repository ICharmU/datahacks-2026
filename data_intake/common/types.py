from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any


@dataclass
class IngestionContext:
    bucket_name: str
    region_name: str = "us-west-2"
    profile_name: str | None = None
    cache_root: Path = Path(".cache/data_intake")
    force_refresh: bool = False
    dry_run: bool = False
    run_id: str = ""
    run_date: str = ""
    request_timeout_sec: int = 120
    user_agent: str = "toxictide-data-intake/0.1"
    max_workers: int = 4
    max_files_per_group: int | None = None


@dataclass
class ValidationResult:
    ok: bool
    checks: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SourceFileRecord:
    source_name: str
    source_group: str | None
    source_url: str | None
    local_cache_path: str
    s3_key: str
    summary_s3_key: str
    filename: str
    size_bytes: int
    content_type: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    validation: dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)