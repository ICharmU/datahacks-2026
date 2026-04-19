from django.conf import settings
from rest_framework.decorators import api_view
from common.responses import api_ok


@api_view(["GET"])
def health(request):
    return api_ok(
        {
            "ok": True,
            "service": "toxic-tide-backend",
            "env": settings.TOXIC_TIDE_ENV,
        }
    )


@api_view(["GET"])
def config_view(request):
    return api_ok(
        {
            "env": settings.TOXIC_TIDE_ENV,
            "use_mock_data": settings.TOXIC_TIDE_USE_MOCK_DATA,
            "wrappers": ["beach", "fishing", "surf", "ecosystem"],
        }
    )


@api_view(["GET"])
def wrappers(request):
    return api_ok(
        [
            {
                "key": "beach",
                "label": "Beach Safety",
                "description": "Swim and shore-entry safety risk.",
            },
            {
                "key": "fishing",
                "label": "Fishing Toxicity",
                "description": "Biotoxin and catch-consumption risk.",
            },
            {
                "key": "surf",
                "label": "Surf Conditions + Bio Risk",
                "description": "Good surf but possible hidden water-quality or bloom risk.",
            },
            {
                "key": "ecosystem",
                "label": "Ecosystem Stress",
                "description": "Broader biodiversity and anomaly monitoring.",
            },
        ]
    )