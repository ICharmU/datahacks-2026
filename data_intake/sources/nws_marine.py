from __future__ import annotations

import json

import requests

from common.aws import make_s3_client
from common.cache import get_source_cache_dir, save_cache_metadata, should_redownload
from common.inspect import summarize_file, write_summary_json
from common.manifest import SourceManifest
from common.parallel import run_parallel
from common.progress import TransferSettings, upload_path_with_progress
from common.types import IngestionContext, SourceFileRecord
from common.utils import ensure_dir, normalize_filename, utc_now_iso
from common.validate import validate_local_file, validate_s3_upload

from .nws_marine_manifest import NWS_MARINE_POINTS

SOURCE_NAME = "nws_marine"

from common.aws import s3_head_object


def should_skip_remote_upload(s3, bucket_name: str, key: str, expected_size: int) -> bool:
    head = s3_head_object(s3, bucket_name, key)
    if head is None:
        return False
    return int(head["ContentLength"]) == expected_size


def _build_items() -> list[dict]:
    items = []

    for p in NWS_MARINE_POINTS:
        lat = p["lat"]
        lon = p["lon"]
        group = p["group"]

        items.append(
            {
                "group": group,
                "url": f"https://api.weather.gov/points/{lat},{lon}",
                "filename": f"{group}_points.json",
                "kind": "json",
            }
        )

    return items


def _enrich_summary_for_json(local_path, summary: dict) -> dict:
    enriched = dict(summary)

    try:
        with local_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        enriched["json_overview"] = {
            "top_level_type": type(payload).__name__,
        }

        if isinstance(payload, dict):
            enriched["json_overview"]["top_level_keys"] = sorted(str(k) for k in payload.keys())

            props = payload.get("properties", {})
            if isinstance(props, dict):
                enriched["nws_point_overview"] = {
                    "forecast": props.get("forecast"),
                    "forecastHourly": props.get("forecastHourly"),
                    "forecastGridData": props.get("forecastGridData"),
                    "observationStations": props.get("observationStations"),
                    "forecastOffice": props.get("forecastOffice"),
                    "gridId": props.get("gridId"),
                    "gridX": props.get("gridX"),
                    "gridY": props.get("gridY"),
                    "county": props.get("county"),
                    "fireWeatherZone": props.get("fireWeatherZone"),
                    "timeZone": props.get("timeZone"),
                    "radarStation": props.get("radarStation"),
                }
    except Exception as exc:
        enriched["json_overview_error"] = str(exc)

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

    items = _build_items()

    def worker(item: dict, position: int) -> dict:
        group = item["group"]
        url = item["url"]
        filename = normalize_filename(item["filename"])

        try:
            local_path = cache_dir / filename
            response_content_type = None

            if should_redownload(local_path, force_refresh=ctx.force_refresh):
                headers = {"User-Agent": ctx.user_agent, "Accept": "application/geo+json, application/json"}
                with requests.get(url, stream=True, timeout=ctx.request_timeout_sec, headers=headers) as response:
                    response.raise_for_status()
                    response_content_type = response.headers.get("Content-Type")
                    if response_content_type and "html" in response_content_type.lower():
                        raise ValueError(f"Expected JSON but got HTML from {response.url}")

                    with local_path.open("wb") as out:
                        for chunk in response.iter_content(chunk_size=transfer_settings.download_chunk_size):
                            if chunk:
                                out.write(chunk)

                save_cache_metadata(
                    local_path,
                    {
                        "source_name": SOURCE_NAME,
                        "source_group": group,
                        "source_url": url,
                        "downloaded_at_utc": utc_now_iso(),
                        "content_type": response_content_type,
                    },
                )

            local_validation = validate_local_file(local_path, expected_suffixes={".json"})
            local_validation_dict = local_validation.to_dict()
            local_validation_dict.setdefault("checks", {})["expected_content_type"] = "application/json or application/geo+json"

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
            summary["source_url"] = url
            summary["filename"] = local_path.name
            if response_content_type is not None:
                summary["response_content_type"] = response_content_type

            summary = _enrich_summary_for_json(local_path, summary)

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
                validation={"local": local_validation_dict},
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
        items,
        worker,
        max_workers=min(ctx.max_workers, max(1, len(items))),
        desc="nws-marine-files",
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