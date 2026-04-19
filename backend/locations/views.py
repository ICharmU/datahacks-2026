from django.shortcuts import get_object_or_404
from rest_framework.decorators import api_view
from common.responses import api_ok
from .models import Beach, CoastalSegment
from .serializers import BeachSerializer, CoastalSegmentSerializer


@api_view(["GET"])
def beach_list(request):
    qs = Beach.objects.filter(is_active=True).order_by("name")
    serializer = BeachSerializer(qs, many=True)
    return api_ok(serializer.data)


@api_view(["GET"])
def beach_detail(request, slug):
    beach = get_object_or_404(Beach, slug=slug, is_active=True)
    serializer = BeachSerializer(beach)
    return api_ok(serializer.data)


@api_view(["GET"])
def coastal_segment_list(request):
    qs = CoastalSegment.objects.all().order_by("name")
    serializer = CoastalSegmentSerializer(qs, many=True)
    return api_ok(serializer.data)


@api_view(["GET"])
def coastal_segment_detail(request, segment_id):
    segment = get_object_or_404(CoastalSegment, segment_id=segment_id)
    serializer = CoastalSegmentSerializer(segment)
    return api_ok(serializer.data)