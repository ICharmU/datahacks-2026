from __future__ import annotations

from pathlib import Path

from common.aws import make_s3_client
from common.cache import get_source_cache_dir, save_cache_metadata, should_redownload
from common.inspect import summarize_file, write_summary_json
from common.manifest import SourceManifest
from common.parallel import run_parallel
from common.progress import TransferSettings, upload_path_with_progress
from common.download_stream import stream_download_to_path
from common.types import IngestionContext, SourceFileRecord
from common.utils import ensure_dir, utc_now_iso
from common.validate import validate_local_file, validate_s3_upload

from .nasa_ocean_color_manifest import NASA_OCEAN_COLOR_FILES

SOURCE_NAME = "nasa_ocean_color"


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
        group = item["group"]
        url = item["url"]
        filename = item["filename"]

        local_path = cache_dir / filename

        if should_redownload(local_path, force_refresh=ctx.force_refresh):
            stream_download_to_path(
                url,
                local_path,
                timeout=ctx.request_timeout_sec,
                user_agent=ctx.user_agent,
                settings=transfer_settings,
                desc=f"nasa:{filename}",
                position=position,
            )
            save_cache_metadata(
                local_path,
                {
                    "source_name": SOURCE_NAME,
                    "source_group": group,
                    "source_url": url,
                    "downloaded_at_utc": utc_now_iso(),
                },
            )

        local_validation = validate_local_file(local_path, expected_suffixes={".nc", ".netcdf", ".hdf", ".h5"})

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
        summary["source_group"] = group
        summary["source_url"] = url
        summary["filename"] = local_path.name

        local_summary_path = summary_dir / f"{group}__{local_path.name}.summary.json"
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
        return record.to_dict()

    records = run_parallel(
        NASA_OCEAN_COLOR_FILES,
        worker,
        max_workers=min(ctx.max_workers, max(1, len(NASA_OCEAN_COLOR_FILES))),
        desc="nasa-ocean-color",
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