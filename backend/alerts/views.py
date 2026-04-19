from rest_framework.decorators import api_view
from rest_framework import status
from common.responses import api_ok, api_error

# stub-only for now; you can replace with DB-backed logic after auth is wired
MOCK_ALERTS = [
    {
        "id": 1,
        "user": 1,
        "wrapper": "beach",
        "location_type": "beach",
        "location_id": "la-jolla-shores",
        "min_risk_score": 0.7,
        "is_active": True,
        "created_at": "2026-04-18T18:00:00Z",
    }
]


@api_view(["GET"])
def alert_list(request):
    return api_ok(MOCK_ALERTS)


@api_view(["POST"])
def alert_create(request):
    payload = dict(request.data)
    payload["id"] = len(MOCK_ALERTS) + 1
    MOCK_ALERTS.append(payload)
    return api_ok(payload, status_code=status.HTTP_201_CREATED)


@api_view(["PATCH"])
def alert_update(request, alert_id):
    for alert in MOCK_ALERTS:
        if alert["id"] == alert_id:
            alert.update(request.data)
            return api_ok(alert)
    return api_error("Alert not found.", status_code=status.HTTP_404_NOT_FOUND)


@api_view(["DELETE"])
def alert_delete(request, alert_id):
    for i, alert in enumerate(MOCK_ALERTS):
        if alert["id"] == alert_id:
            deleted = MOCK_ALERTS.pop(i)
            return api_ok(deleted)
    return api_error("Alert not found.", status_code=status.HTTP_404_NOT_FOUND)