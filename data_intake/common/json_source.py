from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import requests

from common.aws import make_s3_client
from common.cache import get_source_cache_dir, save_cache_metadata, should_redownload
from common.manifest import SourceManifest
from common.progress import TransferSettings, upload_path_with_progress
from common.types import IngestionContext, SourceFileRecord
from common.utils import ensure_dir, normalize_filename, utc_now_iso
from common.validate import validate_local_file, validate_s3_upload


def ingest_json_or_html_artifacts(
    *,
    ctx: IngestionContext,
    source_name: str,
    items: list[dict],
) -> dict:
    """
    Generic runner for:
      - JSON API responses
      - HTML discovery pages
      - text artifacts
    Each item should have:
      group, url, filename, kind
    """
    s3 = make_s3_client(profile_name=ctx.profile_name, region_name=ctx.region_name)
    manifest = SourceManifest(source_name=source_name, run_id=ctx.run_id, run_date=ctx.run_date)

    cache_dir = get_source_cache_dir(ctx.cache_root, source_name)
    summary_dir = ensure_dir(cache_dir / "summaries" / ctx.run_id)

    transfer_settings = TransferSettings(
        download_chunk_size=2 * 1024 * 1024,
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=8 * 1024 * 1024,
        max_concurrency=8,
        use_threads=True,
    )

    headers = {"User-Agent": ctx.user_agent}

    for item in items:
        group = item["group"]
        url = item["url"]
        filename = normalize_filename(item["filename"])
        kind = item.get("kind", "json")

        local_path = cache_dir / filename

        if should_redownload(local_path, force_refresh=ctx.force_refresh):
            with requests.get(url, timeout=ctx.request_timeout_sec, headers=headers) as response:
                response.raise_for_status()

                if kind == "json":
                    payload = response.json()
                    local_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                    content_type = "application/json"
                else:
                    local_path.write_text(response.text, encoding="utf-8", errors="replace")
                    content_type = response.headers.get("Content-Type")

            save_cache_metadata(
                local_path,
                {
                    "source_name": source_name,
                    "source_group": group,
                    "source_url": url,
                    "kind": kind,
                    "downloaded_at_utc": utc_now_iso(),
                    "content_type": content_type,
                },
            )

        local_validation = validate_local_file(
            local_path,
            expected_suffixes={".json", ".html", ".txt"},
        )

        summary = {
            "file_type": kind,
            "size_bytes": local_path.stat().st_size,
            "source_name": source_name,
            "source_group": group,
            "source_url": url,
            "filename": local_path.name,
        }

        local_summary_path = summary_dir / f"{local_path.name}.summary.json"
        local_summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

        raw_root = "raw" if kind == "json" else "discovery"
        raw_key = f"sources/{source_name}/{raw_root}/run_date={ctx.run_date}/{group}/{local_path.name}"
        summary_key = f"sources/{source_name}/summaries/{ctx.run_id}/{group}/{local_path.name}.summary.json"

        record = SourceFileRecord(
            source_name=source_name,
            source_group=group,
            source_url=url,
            local_cache_path=str(local_path),
            s3_key=raw_key,
            summary_s3_key=summary_key,
            filename=local_path.name,
            size_bytes=local_path.stat().st_size,
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
                position=1,
            )
            upload_path_with_progress(
                s3,
                bucket_name=ctx.bucket_name,
                key=summary_key,
                local_path=local_summary_path,
                content_type="application/json",
                settings=transfer_settings,
                position=1,
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
        manifest.add_record(record.to_dict())

    manifest.finish(status="completed", ended_at_utc=utc_now_iso())

    manifest_local_path = cache_dir / f"{ctx.run_id}.manifest.json"
    manifest.write_local(manifest_local_path)

    if not ctx.dry_run:
        upload_path_with_progress(
            s3,
            bucket_name=ctx.bucket_name,
            key=f"sources/{source_name}/manifests/{ctx.run_id}.json",
            local_path=manifest_local_path,
            content_type="application/json",
            settings=transfer_settings,
            position=0,
        )

    return manifest.to_dict()