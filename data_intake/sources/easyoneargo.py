from __future__ import annotations

from pathlib import Path
import json
import re

import requests

from common.aws import make_s3_client
from common.cache import get_source_cache_dir, save_cache_metadata
from common.inspect import summarize_file, write_summary_json
from common.manifest import SourceManifest
from common.parallel import run_parallel
from common.progress import TransferSettings, upload_path_with_progress
from common.download_stream import stream_download_to_path
from common.types import IngestionContext, SourceFileRecord
from common.utils import ensure_dir, normalize_filename, utc_now_iso
from common.validate import validate_local_file, validate_s3_upload

SOURCE_NAME = "easyoneargo"
FILE_KEYS = ["127233", "127234", "126470", "126471", "125529"]
BASE_URL = "https://www.seanoe.org/data/00961/107233/data/"
#https://www.seanoe.org/data/00961/107233/data/127233.tar.gz

from common.aws import s3_head_object


def should_skip_remote_upload(s3, bucket_name: str, key: str, expected_size: int) -> bool:
    head = s3_head_object(s3, bucket_name, key)
    if head is None:
        return False
    return int(head["ContentLength"]) == expected_size


def _resolve_filename(content_disposition: str | None, key: str) -> str:
    filename = f"argo_{key}.nc"
    if content_disposition and "filename=" in content_disposition:
        matches = re.findall(r'filename="?([^";]+)"?', content_disposition)
        if matches:
            filename = matches[0]
    return normalize_filename(filename)


def _find_existing_cached_file(cache_dir: Path, key: str) -> Path | None:
    candidates = list(cache_dir.glob(f"*{key}*.nc")) + list(cache_dir.glob(f"argo_{key}.nc"))
    return candidates[0] if candidates else None


def _remote_expected_size(url: str, *, timeout: int, user_agent: str) -> int | None:
    headers = {"User-Agent": user_agent}
    with requests.get(url, stream=True, timeout=timeout, headers=headers, allow_redirects=True) as response:
        response.raise_for_status()
        content_type = (response.headers.get("Content-Type") or "").lower()
        if "text/html" in content_type:
            raise ValueError(f"Expected netCDF but got HTML from redirected URL: {response.url}")
        content_length = response.headers.get("Content-Length")
        if content_length and content_length.isdigit():
            return int(content_length)
    return None


def _cache_is_usable(path: Path, expected_size: int | None) -> bool:
    if not path.exists():
        return False
    size = path.stat().st_size
    if size <= 0:
        return False
    if expected_size is not None and size != expected_size:
        return False
    return True


def _enrich_summary_for_netcdf(local_path: Path, summary: dict) -> dict:
    enriched = dict(summary)

    try:
        import xarray as xr

        with xr.open_dataset(local_path, decode_times=False) as ds:
            enriched["netcdf_overview"] = {
                "dims": {str(name): int(size) for name, size in ds.sizes.items()},
                "n_dims": int(len(ds.sizes)),
                "data_vars": [str(name) for name in ds.data_vars],
                "n_data_vars": int(len(ds.data_vars)),
                "coords": [str(name) for name in ds.coords],
                "n_coords": int(len(ds.coords)),
                "attrs": sorted(str(name) for name in ds.attrs.keys()),
            }
    except Exception as exc:
        enriched["netcdf_overview_error"] = str(exc)

    return enriched


def emit_record_log(record: dict) -> None:
    summary = record.get("summary", {}) or {}
    validation = record.get("validation", {}) or {}
    local_v = validation.get("local", {}) or {}
    s3_v = validation.get("s3", {}) or {}

    print(
        json.dumps(
            {
                "file": record.get("filename"),
                "status": record.get("status"),
                "size_bytes": record.get("size_bytes"),
                "source_url": record.get("source_url"),
                "s3_key": record.get("s3_key"),
                "local_validation": local_v,
                "s3_validation": s3_v,
                "summary": summary,
                "error": record.get("error"),
            },
            indent=2,
            default=str,
        )
    )


def run(ctx: IngestionContext) -> dict:
    s3 = make_s3_client(profile_name=ctx.profile_name, region_name=ctx.region_name)
    manifest = SourceManifest(source_name=SOURCE_NAME, run_id=ctx.run_id, run_date=ctx.run_date)

    cache_dir = get_source_cache_dir(ctx.cache_root, SOURCE_NAME)
    summary_dir = ensure_dir(cache_dir / "summaries" / ctx.run_id)

    transfer_settings = TransferSettings(
        download_chunk_size=4 * 1024 * 1024,
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=8,
        use_threads=True,
    )

    items = [{"key": key, "url": BASE_URL + key + ".tar.gz"} for key in FILE_KEYS]

    def worker(item: dict, position: int) -> dict:
        key = item["key"]
        url = item["url"]

        try:
            expected_size = _remote_expected_size(
                url,
                timeout=ctx.request_timeout_sec,
                user_agent=ctx.user_agent,
            )

            existing = None if ctx.force_refresh else _find_existing_cached_file(cache_dir, key)
            if existing is not None and not _cache_is_usable(existing, expected_size):
                existing.unlink(missing_ok=True)
                existing = None

            if existing is not None:
                local_path = existing
                content_type = None
            else:
                temp_path = cache_dir / f"argo_{key}.download"
                content_type, content_disposition, _ = stream_download_to_path(
                    url,
                    temp_path,
                    timeout=ctx.request_timeout_sec,
                    user_agent=ctx.user_agent,
                    settings=transfer_settings,
                    desc=f"argo:{key}",
                    position=position,
                )

                downloaded_size = temp_path.stat().st_size
                if expected_size is not None and downloaded_size != expected_size:
                    temp_path.unlink(missing_ok=True)
                    raise ValueError(
                        f"Incomplete download for {key}: expected {expected_size} bytes, got {downloaded_size}"
                    )

                filename = _resolve_filename(content_disposition, key)
                final_path = cache_dir / filename

                if final_path.exists():
                    final_path.unlink()
                temp_path.replace(final_path)

                local_path = final_path
                save_cache_metadata(
                    local_path,
                    {
                        "source_name": SOURCE_NAME,
                        "source_group": None,
                        "source_url": url,
                        "downloaded_at_utc": utc_now_iso(),
                        "content_type": content_type,
                    },
                )

            local_validation = validate_local_file(local_path, expected_suffixes={".nc", ".netcdf"})
            local_validation_dict = local_validation.to_dict()
            local_validation_dict.setdefault("checks", {})["remote_expected_size_bytes"] = expected_size

            try:
                summary = summarize_file(local_path)
            except Exception as exc:
                summary = {
                    "file_type": "unparsed",
                    "size_bytes": local_path.stat().st_size,
                    "summary_error": str(exc),
                    "note": "Fell back to metadata-only summary after parse failure.",
                }

            summary["source_name"] = SOURCE_NAME
            summary["source_group"] = None
            summary["source_url"] = url
            summary["filename"] = local_path.name
            summary["remote_expected_size_bytes"] = expected_size
            summary = _enrich_summary_for_netcdf(local_path, summary)

            local_summary_path = summary_dir / f"{local_path.name}.summary.json"
            write_summary_json(local_summary_path, summary)

            raw_key = f"sources/{SOURCE_NAME}/raw/run_date={ctx.run_date}/{local_path.name}"
            summary_key = f"sources/{SOURCE_NAME}/summaries/{ctx.run_id}/{local_path.name}.summary.json"

            record = SourceFileRecord(
                source_name=SOURCE_NAME,
                source_group=None,
                source_url=url,
                local_cache_path=str(local_path),
                s3_key=raw_key,
                summary_s3_key=summary_key,
                filename=local_path.name,
                size_bytes=local_path.stat().st_size,
                content_type=content_type,
                summary=summary,
                validation={"local": local_validation_dict},
            )

            if (
                not ctx.dry_run
                and expected_size is not None
                and should_skip_remote_upload(
                    s3,
                    bucket_name=ctx.bucket_name,
                    key=raw_key,
                    expected_size=expected_size,
                )
            ):
                record.status = "skipped_remote_exists"
                record.validation["s3"] = {
                    "ok": True,
                    "checks": {
                        "remote_exists_same_size": True,
                        "remote_expected_size_bytes": expected_size,
                    },
                }
                record_dict = record.to_dict()
                emit_record_log(record_dict)
                return record_dict

            if not ctx.dry_run:
                upload_path_with_progress(
                    s3,
                    bucket_name=ctx.bucket_name,
                    key=raw_key,
                    local_path=local_path,
                    settings=transfer_settings,
                    position=position,
                )
                upload_path_with_progress(
                    s3,
                    bucket_name=ctx.bucket_name,
                    key=summary_key,
                    local_path=local_summary_path,
                    content_type="application/json",
                    settings=transfer_settings,
                    position=position,
                )

                s3_validation = validate_s3_upload(
                    s3,
                    bucket_name=ctx.bucket_name,
                    key=raw_key,
                    expected_size=local_path.stat().st_size,
                )
                record.validation["s3"] = s3_validation.to_dict()
            else:
                record.validation["s3"] = {"ok": True, "checks": {"dry_run": True}}

            record.status = "uploaded"
            record_dict = record.to_dict()
            emit_record_log(record_dict)
            return record_dict
        except Exception as exc:
            failure_record = {
                "source_name": SOURCE_NAME,
                "source_group": None,
                "source_url": url,
                "filename": f"argo_{key}.nc",
                "status": "failed",
                "size_bytes": None,
                "s3_key": None,
                "summary": {},
                "validation": {},
                "error": str(exc),
            }
            emit_record_log(failure_record)
            return failure_record

    records = run_parallel(
        items,
        worker,
        max_workers=min(ctx.max_workers, max(1, len(items))),
        desc="argo-files",
    )

    for rec in records:
        manifest.add_record(rec)

    manifest.finish(status="completed", ended_at_utc=utc_now_iso())

    manifest_local_path = cache_dir / f"{ctx.run_id}.manifest.json"
    manifest.write_local(manifest_local_path)

    if not ctx.dry_run:
        upload_path_with_progress(
            s3,
            bucket_name=ctx.bucket_name,
            key=f"sources/{SOURCE_NAME}/manifests/{ctx.run_id}.json",
            local_path=manifest_local_path,
            content_type="application/json",
            settings=transfer_settings,
            position=0,
        )

    return manifest.to_dict()