from rest_framework.decorators import api_view
from common.responses import api_ok
from .services import ServingDataService


def _status_from_active(row):
    if row.get("is_active"):
        return "ready"
    return "pending"


@api_view(["GET"])
def ingestion_status(request):
    manifest = ServingDataService.fetch_json("manifest")
    coverage = ServingDataService.fetch_json("source_coverage_summary")

    return api_ok(
        {
            "manifest": manifest,
            "sources": [
                {
                    "name": row.get("source_name"),
                    "status": _status_from_active(row),
                    "avg_available": row.get("avg_available"),
                    "avg_freshness_days": row.get("avg_freshness_days"),
                }
                for row in coverage
            ],
        }
    )


@api_view(["GET"])
def pipeline_status(request):
    manifest = ServingDataService.fetch_json("manifest")
    coverage = ServingDataService.fetch_json("source_coverage_summary")
    generated_at = manifest.get("generated_at_utc")

    rows = [
        {
            "source": row.get("source_name"),
            "status": "ready" if row.get("is_active") else "pending",
            "last_updated": generated_at,
            "note": f"avg_available={row.get('avg_available')} avg_freshness_days={row.get('avg_freshness_days')}",
        }
        for row in coverage
    ]

    return api_ok(
        {
            "generated_at_utc": generated_at,
            "bronze": rows,
            "silver": rows,
            "gold": rows,
            "serving": rows,
        }
    )


@api_view(["POST"])
def ingestion_trigger(request):
    force = bool(request.data.get("force", False))
    counts = ServingDataService.sync_all(force=force)
    return api_ok(
        {
            "triggered": True,
            "force": force,
            "counts": counts,
            "note": "Serving artifacts refreshed from S3.",
        }
    )