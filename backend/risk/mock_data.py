from datetime import datetime, timedelta, timezone

NOW = datetime.now(timezone.utc)

MOCK_MAP = {
    "generated_at_utc": NOW.isoformat(),
    "wrapper": "beach",
    "horizon": "24h",
    "locations": [
        {
            "location_id": "la-jolla-shores",
            "name": "La Jolla Shores",
            "lat": 32.8507,
            "lon": -117.2726,
            "risk_score": 0.74,
            "risk_bucket": "high",
            "uncertainty_score": 0.18,
        },
        {
            "location_id": "pacific-beach",
            "name": "Pacific Beach",
            "lat": 32.7940,
            "lon": -117.2550,
            "risk_score": 0.41,
            "risk_bucket": "moderate",
            "uncertainty_score": 0.12,
        },
        {
            "location_id": "imperial-beach",
            "name": "Imperial Beach",
            "lat": 32.5798,
            "lon": -117.1326,
            "risk_score": 0.88,
            "risk_bucket": "very_high",
            "uncertainty_score": 0.10,
        },
    ],
}