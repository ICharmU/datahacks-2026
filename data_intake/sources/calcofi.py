from __future__ import annotations

from pathlib import Path
import json
import zipfile

from common.aws import make_s3_client
from common.cache import get_source_cache_dir, save_cache_metadata, should_redownload
from common.inspect import summarize_file, write_summary_json
from common.manifest import SourceManifest
from common.parallel import run_parallel
from common.progress import TransferSettings, upload_path_with_progress
from common.download_stream import stream_download_to_path
from common.types import IngestionContext, SourceFileRecord
from common.utils import ensure_dir, normalize_filename, utc_now_iso
from common.validate import validate_local_file, validate_s3_upload

from .calcofi_manifest import CALCOFI_FILES

SOURCE_NAME = "calcofi"

from common.aws import s3_head_object


def should_skip_remote_upload(s3, bucket_name: str, key: str, expected_size: int) -> bool:
    head = s3_head_object(s3, bucket_name, key)
    if head is None:
        return False
    return int(head["ContentLength"]) == expected_size


def _resolve_final_path(cache_dir: Path, filename: str) -> Path:
    return cache_dir / normalize_filename(filename)


def _enrich_summary_for_tabular(local_path: Path, summary: dict) -> dict:
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
            "dtypes": {
                str(col): str(dtype)
                for col, dtype in df.dtypes.astype(str).to_dict().items()
            },
        }
    except Exception as exc:
        enriched["table_overview_error"] = str(exc)

    return enriched


def _enrich_summary_for_zip(local_path: Path, summary: dict) -> dict:
    enriched = dict(summary)

    try:
        with zipfile.ZipFile(local_path, "r") as zf:
            members = zf.infolist()
            enriched["zip_overview"] = {
                "member_count": int(len(members)),
                "member_names": [member.filename for member in members[:100]],
                "total_uncompressed_size_bytes": int(sum(member.file_size for member in members)),
                "total_compressed_size_bytes": int(sum(member.compress_size for member in members)),
            }
    except Exception as exc:
        enriched["zip_overview_error"] = str(exc)

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

    def worker(item: dict, position: int) -> dict:
        url = item["url"]
        group = item["group"]
        filename = item["filename"]
        kind = item.get("kind", "file")

        try:
            local_path = _resolve_final_path(cache_dir, filename)

            if kind == "page":
                if should_redownload(local_path, force_refresh=ctx.force_refresh):
                    stream_download_to_path(
                        url,
                        local_path,
                        timeout=ctx.request_timeout_sec,
                        user_agent=ctx.user_agent,
                        settings=transfer_settings,
                        desc=f"calcofi-page:{local_path.name}",
                        position=position,
                    )
                    save_cache_metadata(
                        local_path,
                        {
                            "source_name": SOURCE_NAME,
                            "source_group": group,
                            "source_url": url,
                            "kind": kind,
                            "downloaded_at_utc": utc_now_iso(),
                        },
                    )

                summary = summarize_file(local_path)
                summary["source_name"] = SOURCE_NAME
                summary["source_group"] = group
                summary["source_url"] = url
                summary["kind"] = kind
                summary["filename"] = local_path.name

                local_summary_path = summary_dir / f"{local_path.name}.summary.json"
                write_summary_json(local_summary_path, summary)

                raw_key = f"sources/{SOURCE_NAME}/discovery/run_date={ctx.run_date}/{group}/{local_path.name}"
                summary_key = f"sources/{SOURCE_NAME}/summaries/{ctx.run_id}/{group}/{local_path.name}.summary.json"

                record = SourceFileRecord(
                    source_name=SOURCE_NAME,
                    source_group=group,
                    source_url=url,
                    local_cache_path=str(local_path),
                    s3_key=raw_key,
                    summary_s3_key=summary_key,
                    filename=local_path.name,
                    size_bytes=local_path.stat().st_size,
                    summary=summary,
                    validation={
                        "local": {
                            "ok": summary.get("file_type") == "html",
                            "checks": {
                                "kind": kind,
                                "detected_file_type": summary.get("file_type"),
                                "size_bytes": local_path.stat().st_size,
                            },
                            "warnings": [
                                "Page URL saved as discovery artifact, not treated as raw data."
                            ],
                            "errors": [],
                        }
                    },
                )

                if not ctx.dry_run and should_skip_remote_upload(
                    s3,
                    bucket_name=ctx.bucket_name,
                    key=raw_key,
                    expected_size=local_path.stat().st_size,
                ):
                    record.status = "skipped_remote_exists"
                    record.validation["s3"] = {
                        "ok": True,
                        "checks": {"remote_exists_same_size": True},
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

                record.status = "uploaded"
                record_dict = record.to_dict()
                emit_record_log(record_dict)
                return record_dict

            if should_redownload(local_path, force_refresh=ctx.force_refresh):
                stream_download_to_path(
                    url,
                    local_path,
                    timeout=ctx.request_timeout_sec,
                    user_agent=ctx.user_agent,
                    settings=transfer_settings,
                    desc=f"calcofi:{local_path.name}",
                    position=position,
                )
                save_cache_metadata(
                    local_path,
                    {
                        "source_name": SOURCE_NAME,
                        "source_group": group,
                        "source_url": url,
                        "kind": kind,
                        "downloaded_at_utc": utc_now_iso(),
                    },
                )

            local_validation = validate_local_file(
                local_path,
                expected_suffixes={".csv", ".zip", ".txt", ".tsv", ".tab"},
            )

            try:
                if local_path.suffix.lower() == ".zip":
                    summary = {
                        "file_type": "zip",
                        "size_bytes": local_path.stat().st_size,
                        "filename": local_path.name,
                        "source_name": SOURCE_NAME,
                        "source_group": group,
                        "source_url": url,
                    }
                else:
                    summary = summarize_file(local_path)
            except Exception as exc:
                summary = {
                    "file_type": "unparsed",
                    "size_bytes": local_path.stat().st_size,
                    "filename": local_path.name,
                    "source_name": SOURCE_NAME,
                    "source_group": group,
                    "source_url": url,
                    "summary_error": str(exc),
                    "note": "Fell back to metadata-only summary after parse failure.",
                }

            summary["source_name"] = SOURCE_NAME
            summary["source_group"] = group
            summary["source_url"] = url
            summary["kind"] = kind
            summary["filename"] = local_path.name

            suffix = local_path.suffix.lower()
            if suffix == ".zip":
                summary = _enrich_summary_for_zip(local_path, summary)
            elif suffix in {".csv", ".txt", ".tsv", ".tab"}:
                summary = _enrich_summary_for_tabular(local_path, summary)

            local_summary_path = summary_dir / f"{local_path.name}.summary.json"
            write_summary_json(local_summary_path, summary)

            raw_key = f"sources/{SOURCE_NAME}/raw/run_date={ctx.run_date}/{group}/{local_path.name}"
            summary_key = f"sources/{SOURCE_NAME}/summaries/{ctx.run_id}/{group}/{local_path.name}.summary.json"

            record = SourceFileRecord(
                source_name=SOURCE_NAME,
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
            return failure_record

    records = run_parallel(
        CALCOFI_FILES,
        worker,
        max_workers=min(4, max(1, len(CALCOFI_FILES))),
        desc="calcofi-files",
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