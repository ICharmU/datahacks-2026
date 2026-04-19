from __future__ import annotations

from pathlib import Path
import re

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

    items = [{"key": key, "url": BASE_URL + key} for key in FILE_KEYS]

    def worker(item: dict, position: int) -> dict:
        key = item["key"]
        url = item["url"]

        existing = None if ctx.force_refresh else _find_existing_cached_file(cache_dir, key)

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
            filename = _resolve_filename(content_disposition, key)
            final_path = cache_dir / filename

            if temp_path != final_path:
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

        try:
            summary = summarize_file(local_path)
        except Exception as e:
            summary = {
                "file_type": "unparsed",
                "size_bytes": local_path.stat().st_size,
                "summary_error": str(e),
                "note": "Fell back to metadata-only summary after parse failure.",
            }

        summary["source_name"] = SOURCE_NAME
        summary["source_group"] = None
        summary["source_url"] = url
        summary["filename"] = local_path.name

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
            validation={"local": local_validation.to_dict()},
        )

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
        return record.to_dict()

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