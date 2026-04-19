from __future__ import annotations

import json

import requests

from common.aws import make_s3_client
from common.cache import get_source_cache_dir, save_cache_metadata, should_redownload
from common.inspect import summarize_file, write_summary_json
from common.manifest import SourceManifest
from common.progress import TransferSettings, upload_path_with_progress
from common.types import IngestionContext, SourceFileRecord
from common.utils import ensure_dir, normalize_filename, utc_now_iso
from common.validate import validate_local_file, validate_s3_upload


def _enrich_summary_for_tabular(local_path, summary: dict) -> dict:
    enriched = dict(summary)

    try:
        import pandas as pd

        suffix = local_path.suffix.lower()
        read_kwargs = {"low_memory": False}
        if suffix in {".tsv", ".tab"}:
            read_kwargs["sep"] = "\t"

        df = pd.read_csv(local_path, **read_kwargs)
        enriched["table_overview"] = {
            "shape": [int(df.shape[0]), int(df.shape[1])],
            "columns": [str(col) for col in df.columns.tolist()],
            "dtypes": {str(col): str(dtype) for col, dtype in df.dtypes.astype(str).to_dict().items()},
        }
    except Exception as exc:
        enriched["table_overview_error"] = str(exc)

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


def ingest_tabular_files(
    *,
    ctx: IngestionContext,
    source_name: str,
    items: list[dict],
    expected_suffixes: set[str],
) -> dict:
    """
    Generic runner for sources that are basically:
      URL -> local cached file -> summary -> S3 raw -> manifest
    Each item should have:
      group, url, filename
    """
    s3 = make_s3_client(profile_name=ctx.profile_name, region_name=ctx.region_name)
    manifest = SourceManifest(source_name=source_name, run_id=ctx.run_id, run_date=ctx.run_date)

    cache_dir = get_source_cache_dir(ctx.cache_root, source_name)
    summary_dir = ensure_dir(cache_dir / "summaries" / ctx.run_id)

    transfer_settings = TransferSettings(
        download_chunk_size=4 * 1024 * 1024,
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=8,
        use_threads=True,
    )

    for item in items:
        group = item["group"]
        url = item["url"]
        filename = normalize_filename(item["filename"])
        local_path = cache_dir / filename

        try:
            if should_redownload(local_path, force_refresh=ctx.force_refresh):
                headers = {"User-Agent": ctx.user_agent}
                with requests.get(url, stream=True, timeout=ctx.request_timeout_sec, headers=headers) as response:
                    response.raise_for_status()
                    with local_path.open("wb") as out:
                        for chunk in response.iter_content(chunk_size=transfer_settings.download_chunk_size):
                            if chunk:
                                out.write(chunk)

                save_cache_metadata(
                    local_path,
                    {
                        "source_name": source_name,
                        "source_group": group,
                        "source_url": url,
                        "downloaded_at_utc": utc_now_iso(),
                        "content_type": response.headers.get("Content-Type"),
                    },
                )

            local_validation = validate_local_file(local_path, expected_suffixes=expected_suffixes)

            try:
                summary = summarize_file(local_path)
            except Exception as exc:
                summary = {
                    "file_type": "unparsed",
                    "size_bytes": local_path.stat().st_size,
                    "summary_error": str(exc),
                    "note": "Fell back to metadata-only summary after parse failure.",
                }

            summary["source_name"] = source_name
            summary["source_group"] = group
            summary["source_url"] = url
            summary["filename"] = local_path.name
            summary = _enrich_summary_for_tabular(local_path, summary)

            local_summary_path = summary_dir / f"{local_path.name}.summary.json"
            write_summary_json(local_summary_path, summary)

            raw_key = f"sources/{source_name}/raw/run_date={ctx.run_date}/{group}/{local_path.name}"
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
            record_dict = record.to_dict()
            emit_record_log(record_dict)
            manifest.add_record(record_dict)
        except Exception as exc:
            failure_record = {
                "source_name": source_name,
                "source_group": group,
                "source_url": url,
                "filename": filename,
                "status": "failed",
                "size_bytes": None,
                "s3_key": None,
                "summary": {},
                "validation": {},
                "error": str(exc),
            }
            emit_record_log(failure_record)
            manifest.add_record(failure_record)

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