from rest_framework.decorators import api_view
from common.responses import api_ok


@api_view(["GET"])
def beach_explanation(request, slug):
    horizon = request.GET.get("horizon", "24h")
    return api_ok(
        {
            "wrapper": "beach",
            "location_id": slug,
            "horizon": horizon,
            "summary": "Risk is elevated because biological productivity proxies and recent coastal transport conditions are both above baseline.",
            "bullets": [
                "Chlorophyll-related proxy increased versus recent baseline.",
                "Recent transport conditions are consistent with persistence of nearshore risk.",
                "Historical analog periods show similar risk behavior at this beach.",
            ],
            "citations": [],
        }
    )


@api_view(["GET"])
def fishing_explanation(request, segment_id):
    horizon = request.GET.get("horizon", "24h")
    return api_ok(
        {
            "wrapper": "fishing",
            "location_id": segment_id,
            "horizon": horizon,
            "summary": "Modeled toxin propensity is elevated relative to recent background conditions.",
            "bullets": [
                "Environmental signatures resemble prior moderate-risk periods.",
                "Caution is warranted until public-health guidance is checked.",
            ],
            "citations": [],
        }
    )