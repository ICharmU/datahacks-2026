from __future__ import annotations

from dotenv import load_dotenv
from pathlib import Path
import argparse
import json
import os

from common.types import IngestionContext
from common.utils import utc_now_iso, today_utc
from sources import RUNNERS


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Toxic Tide raw data ingestion")
    p.add_argument(
        "--sources",
        nargs="+",
        default=["calcofi", "cce_moorings", "easyoneargo"],
        choices=sorted(RUNNERS.keys()),
        help="Which sources to run.",
    )
    p.add_argument("--bucket-name", default=os.getenv("RAW_BUCKET_NAME"))
    p.add_argument("--region-name", default=os.getenv("AWS_REGION", "us-west-2"))
    p.add_argument("--profile-name", default=os.getenv("AWS_PROFILE"))
    p.add_argument("--cache-root", default=os.getenv("DATA_INTAKE_CACHE_ROOT", ".cache/data_intake"))
    p.add_argument("--force-refresh", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--max-workers", type=int, default=int(os.getenv("DATA_INTAKE_MAX_WORKERS", "4")))
    p.add_argument("--max-files-per-group", type=int, default=None)
    return p


def build_context(args: argparse.Namespace) -> IngestionContext:
    run_id = utc_now_iso().replace(":", "-")
    return IngestionContext(
        bucket_name=args.bucket_name,
        region_name=args.region_name,
        profile_name=args.profile_name,
        cache_root=Path(args.cache_root),
        force_refresh=args.force_refresh,
        dry_run=args.dry_run,
        run_id=run_id,
        run_date=today_utc(),
        max_workers=args.max_workers,
        max_files_per_group=args.max_files_per_group,
    )


def main() -> None:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args()

    if not args.bucket_name:
        raise ValueError("Missing bucket name. Pass --bucket-name or set RAW_BUCKET_NAME.")

    ctx = build_context(args)

    print("=== Toxic Tide Raw Ingestion ===")
    print(json.dumps(
        {
            "bucket_name": ctx.bucket_name,
            "region_name": ctx.region_name,
            "profile_name": ctx.profile_name,
            "cache_root": str(ctx.cache_root),
            "run_id": ctx.run_id,
            "run_date": ctx.run_date,
            "force_refresh": ctx.force_refresh,
            "dry_run": ctx.dry_run,
            "max_workers": ctx.max_workers,
            "max_files_per_group": ctx.max_files_per_group,
            "sources": args.sources,
        },
        indent=2,
    ))

    all_results = {}
    for source_name in args.sources:
        print(f"\n--- Running source: {source_name} ---")
        result = RUNNERS[source_name](ctx)
        all_results[source_name] = result
        print(
            json.dumps(
                {
                    "source_name": source_name,
                    "status": result["status"],
                    "records": len(result["records"]),
                    "notes": result["notes"],
                },
                indent=2,
            )
        )

    print("\n=== Final Summary ===")
    print(
        json.dumps(
            {
                name: {
                    "status": result["status"],
                    "records": len(result["records"]),
                }
                for name, result in all_results.items()
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()