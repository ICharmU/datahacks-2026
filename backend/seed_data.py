from locations.models import CoastalSegment, Beach

seg, _ = CoastalSegment.objects.get_or_create(
    segment_id="san-diego-nearshore-01",
    defaults={
        "name": "San Diego Nearshore 01",
        "lat": 32.8,
        "lon": -117.2,
        "geojson": {},
    },
)

Beach.objects.get_or_create(
    slug="la-jolla-shores",
    defaults={
        "name": "La Jolla Shores",
        "county": "San Diego",
        "lat": 32.8507,
        "lon": -117.2726,
        "coastal_segment": seg,
        "is_active": True,
    },
)

Beach.objects.get_or_create(
    slug="imperial-beach",
    defaults={
        "name": "Imperial Beach",
        "county": "San Diego",
        "lat": 32.5798,
        "lon": -117.1326,
        "coastal_segment": seg,
        "is_active": True,
    },
)