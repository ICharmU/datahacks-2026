from rest_framework import serializers
from .models import Beach, CoastalSegment


class CoastalSegmentSerializer(serializers.ModelSerializer):
    class Meta:
        model = CoastalSegment
        fields = [
            "segment_id",
            "name",
            "lat",
            "lon",
            "geojson",
        ]


class BeachSerializer(serializers.ModelSerializer):
    coastal_segment_id = serializers.CharField(
        source="coastal_segment.segment_id",
        read_only=True,
        allow_null=True,
    )

    class Meta:
        model = Beach
        fields = [
            "slug",
            "name",
            "county",
            "lat",
            "lon",
            "coastal_segment_id",
            "is_active",
        ]