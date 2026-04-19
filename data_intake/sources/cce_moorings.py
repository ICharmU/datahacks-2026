from __future__ import annotations

from pathlib import Path
import json
import urllib.request

from siphon.catalog import TDSCatalog

from common.aws import make_s3_client
from common.cache import get_source_cache_dir, save_cache_metadata, should_redownload
from common.inspect import summarize_file, write_summary_json
from common.manifest import SourceManifest
from common.parallel import run_parallel
from common.progress import TransferSettings, upload_path_with_progress
from common.types import IngestionContext, SourceFileRecord
from common.utils import ensure_dir, normalize_filename, utc_now_iso
from common.validate import validate_local_file, validate_s3_upload

SOURCE_NAME = "cce_moorings"

CATALOGS = {
    "cce1": "https://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/CCE1/catalog.xml",
    "cce2": "https://dods.ndbc.noaa.gov/thredds/catalog/oceansites/DATA/CCE2/catalog.xml",
}

from common.aws import s3_head_object


def should_skip_remote_upload(s3, bucket_name: str, key: str, expected_size: int) -> bool:
    head = s3_head_object(s3, bucket_name, key)
    if head is None:
        return False
    return int(head["ContentLength"]) == expected_size


def _download_to_cache(download_url: str, dest_path: Path) -> int:
    with urllib.request.urlopen(download_url) as response:
        with dest_path.open("wb") as out:
            total = 0
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                total += len(chunk)
    return total


def _discover_catalog_items(ctx: IngestionContext) -> list[dict]:
    discovered: list[dict] = []

    for group_name, catalog_url in CATALOGS.items():
        cat = TDSCatalog(catalog_url)

        count_for_group = 0
        for dataset_name, dataset_obj in cat.datasets.items():
            if "HTTPServer" not in dataset_obj.access_urls:
                continue

            if ctx.max_files_per_group is not None and count_for_group >= ctx.max_files_per_group:
                break

            count_for_group += 1
            discovered.append(
                {
                    "group": group_name,
                    "catalog_url": catalog_url,
                    "dataset_name": dataset_name,
                    "download_url": dataset_obj.access_urls["HTTPServer"],
                }
            )

    return discovered


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

    discovered = _discover_catalog_items(ctx)
    manifest.add_note(f"discovered_downloadable_datasets={len(discovered)}")

    def worker(item: dict, position: int) -> dict:
        group = item["group"]
        dataset_name = item["dataset_name"]
        download_url = item["download_url"]
        catalog_url = item["catalog_url"]

        try:
            group_cache = ensure_dir(cache_dir / group)
            filename = normalize_filename(dataset_name)
            local_path = group_cache / filename

            if should_redownload(local_path, force_refresh=ctx.force_refresh):
                _download_to_cache(download_url, local_path)
                save_cache_metadata(
                    local_path,
                    {
                        "source_name": SOURCE_NAME,
                        "source_group": group,
                        "source_url": download_url,
                        "catalog_url": catalog_url,
                        "downloaded_at_utc": utc_now_iso(),
                    },
                )

            local_validation = validate_local_file(
                local_path,
                expected_suffixes={".nc", ".cdf", ".txt", ".csv"},
            )

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
            summary["source_group"] = group
            summary["catalog_url"] = catalog_url
            summary["source_url"] = download_url
            summary["filename"] = local_path.name

            suffix = local_path.suffix.lower()
            if suffix in {".nc", ".cdf"}:
                summary = _enrich_summary_for_netcdf(local_path, summary)
            elif suffix in {".csv", ".txt", ".tsv", ".tab"}:
                summary = _enrich_summary_for_tabular(local_path, summary)

            local_summary_path = summary_dir / f"{group}__{local_path.name}.summary.json"
            write_summary_json(local_summary_path, summary)

            raw_key = f"sources/{SOURCE_NAME}/raw/run_date={ctx.run_date}/{group}/{local_path.name}"
            summary_key = f"sources/{SOURCE_NAME}/summaries/{ctx.run_id}/{group}/{local_path.name}.summary.json"

            record = SourceFileRecord(
                source_name=SOURCE_NAME,
                source_group=group,
                source_url=download_url,
                local_cache_path=str(local_path),
                s3_key=raw_key,
                summary_s3_key=summary_key,
                filename=local_path.name,
                size_bytes=local_path.stat().st_size,
                summary=summary,
                validation={"local": local_validation.to_dict()},
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
                "source_url": download_url,
                "filename": dataset_name,
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
        discovered,
        worker,
        max_workers=min(ctx.max_workers, max(1, len(discovered))),
        desc="cce-files",
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