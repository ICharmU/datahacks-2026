from rest_framework.decorators import api_view
from rest_framework import status
from common.responses import api_ok, api_error
from .serializers import RiskQueryRequestSerializer
from .services import RiskService


@api_view(["GET"])
def grower_dashboard(request):
    region = request.GET.get("region", "Southern California")
    return api_ok(RiskService.get_grower_dashboard(region=region))


@api_view(["GET"])
def grower_site_detail(request, site_id):
    payload = RiskService.get_grower_site_detail(site_id=site_id)
    if not payload:
        return api_error("Grower site not found.", status_code=status.HTTP_404_NOT_FOUND)
    return api_ok(payload)


@api_view(["GET"])
def fleet_dashboard(request):
    port = request.GET.get("port", "San Diego")
    species = request.GET.get("species", "Market squid")
    return api_ok(RiskService.get_fleet_dashboard(port_name=port, target_species=species))


@api_view(["GET"])
def fleet_zone_detail(request, zone_id):
    payload = RiskService.get_fleet_zone_detail(zone_id=zone_id)
    if not payload:
        return api_error("Fleet zone not found.", status_code=status.HTTP_404_NOT_FOUND)
    return api_ok(payload)


@api_view(["GET"])
def risk_map(request):
    shell = request.GET.get("shell", "grower")
    wrapper = request.GET.get("wrapper", "aquaculture")
    horizon = request.GET.get("horizon", "24h")
    payload = RiskService.get_map(shell=shell, wrapper=wrapper, horizon=horizon)
    return api_ok(payload)


@api_view(["GET"])
def beach_risk_detail(request, slug):
    horizon = request.GET.get("horizon", "24h")
    payload = RiskService.get_beach_detail(site_id=slug, horizon=horizon)
    if not payload:
        return api_error("Beach detail not found.", status_code=status.HTTP_404_NOT_FOUND)
    return api_ok(payload)


@api_view(["POST"])
def risk_query(request):
    serializer = RiskQueryRequestSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    data = serializer.validated_data

    if data["wrapper"] == "beach":
        payload = RiskService.get_beach_detail(site_id=data["location_id"], horizon=data["horizon"])
    else:
        payload = RiskService.get_grower_site_detail(site_id=data["location_id"])

    return api_ok(payload or {})