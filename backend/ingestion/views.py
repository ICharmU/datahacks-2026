from rest_framework.decorators import api_view
from common.responses import api_ok


@api_view(["GET"])
def ingestion_status(request):
    return api_ok(
        {
            "sources": [
                {"name": "calcofi", "status": "pending"},
                {"name": "cce_mooring", "status": "pending"},
                {"name": "easyoneargo", "status": "pending"},
                {"name": "cdip", "status": "pending"},
                {"name": "beach_advisories", "status": "pending"},
                {"name": "biotoxin", "status": "pending"},
            ]
        }
    )


@api_view(["POST"])
def ingestion_trigger(request):
    return api_ok(
        {
            "triggered": True,
            "source": request.data.get("source", "unknown"),
            "note": "Stub only for now; later this can call AWS/n8n/Databricks job orchestration.",
        }
    )