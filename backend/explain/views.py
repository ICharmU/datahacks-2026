from rest_framework.decorators import api_view
from common.responses import api_ok
from ingestion.services import ServingDataService


@api_view(["GET"])
def beach_explanation(request, slug):
    rows = [
        r for r in ServingDataService.fetch_json("explanations")
        if r.get("product_shell") == "beach" and r.get("site_id") == slug
    ]
    row = rows[0] if rows else {}
    return api_ok(
        {
            "wrapper": "beach",
            "location_id": slug,
            "horizon": request.GET.get("horizon", "24h"),
            "summary": row.get("explanation_text", "No explanation available."),
            "bullets": [row.get("headline", "")],
            "citations": [],
        }
    )


@api_view(["GET"])
def grower_explanation(request, site_id):
    rows = [
        r for r in ServingDataService.fetch_json("explanations")
        if r.get("product_shell") == "aquaculture" and r.get("site_id") == site_id
    ]
    row = rows[0] if rows else {}
    return api_ok(
        {
            "wrapper": "aquaculture",
            "location_id": site_id,
            "horizon": request.GET.get("horizon", "24h"),
            "summary": row.get("explanation_text", "No explanation available."),
            "bullets": [row.get("headline", "")],
            "citations": [],
        }
    )