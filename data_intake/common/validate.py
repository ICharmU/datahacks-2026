from __future__ import annotations

from pathlib import Path

from .aws import s3_head_object
from .types import ValidationResult
from .utils import suffix_lower


def validate_local_file(path: Path, expected_suffixes: set[str] | None = None) -> ValidationResult:
    result = ValidationResult(ok=True)

    exists = path.exists()
    result.checks["exists"] = exists
    if not exists:
        result.ok = False
        result.errors.append(f"Local file missing: {path}")
        return result

    size_bytes = path.stat().st_size
    result.checks["size_bytes"] = size_bytes
    result.checks["nonzero_size"] = size_bytes > 0
    if size_bytes <= 0:
        result.ok = False
        result.errors.append(f"File has zero bytes: {path}")

    suffix = suffix_lower(path)
    result.checks["suffix"] = suffix
    if expected_suffixes and suffix not in expected_suffixes:
        result.warnings.append(
            f"Unexpected suffix {suffix}; expected one of {sorted(expected_suffixes)}"
        )

    return result


def validate_s3_upload(s3, *, bucket_name: str, key: str, expected_size: int) -> ValidationResult:
    result = ValidationResult(ok=True)

    head = s3_head_object(s3, bucket_name, key)
    result.checks["upload_exists"] = head is not None
    if head is None:
        result.ok = False
        result.errors.append(f"S3 object missing after upload: s3://{bucket_name}/{key}")
        return result

    remote_size = int(head["ContentLength"])
    result.checks["remote_size_bytes"] = remote_size
    result.checks["size_matches"] = remote_size == expected_size
    if remote_size != expected_size:
        result.ok = False
        result.errors.append(
            f"S3 size mismatch for s3://{bucket_name}/{key}: local={expected_size}, remote={remote_size}"
        )

    return result