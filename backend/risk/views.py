from rest_framework.decorators import api_view
from rest_framework import status
from common.responses import api_ok, api_error
from .serializers import RiskQueryRequestSerializer
from .services import RiskService


@api_view(["GET"])
def risk_map(request):
    wrapper = request.GET.get("wrapper", "beach")
    horizon = request.GET.get("horizon", "24h")
    payload = RiskService.get_map(wrapper=wrapper, horizon=horizon)
    return api_ok(payload)


@api_view(["GET"])
def beach_risk_detail(request, slug):
    horizon = request.GET.get("horizon", "24h")
    payload = RiskService.get_beach_detail(slug=slug, horizon=horizon)
    if not payload:
        return api_error("Beach detail not found.", status_code=status.HTTP_404_NOT_FOUND)
    return api_ok(payload)


@api_view(["GET"])
def fishing_risk_detail(request, segment_id):
    horizon = request.GET.get("horizon", "24h")
    payload = RiskService.get_fishing_detail(segment_id=segment_id, horizon=horizon)
    if not payload:
        return api_error("Fishing detail not found.", status_code=status.HTTP_404_NOT_FOUND)
    return api_ok(payload)


@api_view(["POST"])
def risk_query(request):
    serializer = RiskQueryRequestSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    data = serializer.validated_data

    if data["wrapper"] == "beach":
        payload = RiskService.get_beach_detail(slug=data["location_id"], horizon=data["horizon"])
    elif data["wrapper"] == "fishing":
        payload = RiskService.get_fishing_detail(segment_id=data["location_id"], horizon=data["horizon"])
    else:
        payload = {
            "location_id": data["location_id"],
            "wrapper": data["wrapper"],
            "forecast": [],
            "risk_bucket": "unknown",
            "top_factors": [],
            "recommended_action": "No mock configured yet.",
            "evidence": [],
        }

    return api_ok(payload)