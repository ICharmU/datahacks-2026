from rest_framework.decorators import api_view
from rest_framework.response import Response
from .mock_data import MOCK_MAP, MOCK_BEACH_DETAIL

@api_view(["GET"])
def demo_map(request):
    return Response(MOCK_MAP)

@api_view(["GET"])
def demo_beach_detail(request, slug):
    payload = dict(MOCK_BEACH_DETAIL)
    payload["location_id"] = slug
    return Response(payload)