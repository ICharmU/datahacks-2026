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

MOCK_DETAILS = {
    "la-jolla-shores": {
        "location_id": "la-jolla-shores",
        "name": "La Jolla Shores",
        "wrapper": "beach",
        "forecast": [
            {"t": (NOW + timedelta(hours=0)).isoformat(), "risk_score": 0.61},
            {"t": (NOW + timedelta(hours=12)).isoformat(), "risk_score": 0.68},
            {"t": (NOW + timedelta(hours=24)).isoformat(), "risk_score": 0.74},
            {"t": (NOW + timedelta(hours=48)).isoformat(), "risk_score": 0.58},
        ],
        "risk_bucket": "high",
        "uncertainty_score": 0.18,
        "top_factors": [
            {"name": "chlorophyll_anomaly", "direction": "up", "magnitude": 0.86},
            {"name": "recent_wave_transport", "direction": "up", "magnitude": 0.57},
            {"name": "nearby_advisory_history", "direction": "up", "magnitude": 0.42},
        ],
        "recommended_action": "Avoid swimming tomorrow afternoon. Re-check official advisory before entering the water.",
        "evidence": [
            "Elevated biological productivity proxy",
            "Recent coastal transport conditions consistent with elevated nearshore risk",
            "Historical analogs show similar risk patterns",
        ],
    },
    "imperial-beach": {
        "location_id": "imperial-beach",
        "name": "Imperial Beach",
        "wrapper": "beach",
        "forecast": [
            {"t": (NOW + timedelta(hours=0)).isoformat(), "risk_score": 0.82},
            {"t": (NOW + timedelta(hours=12)).isoformat(), "risk_score": 0.88},
            {"t": (NOW + timedelta(hours=24)).isoformat(), "risk_score": 0.91},
            {"t": (NOW + timedelta(hours=48)).isoformat(), "risk_score": 0.79},
        ],
        "risk_bucket": "very_high",
        "uncertainty_score": 0.10,
        "top_factors": [
            {"name": "historical_advisory_density", "direction": "up", "magnitude": 0.91},
            {"name": "recent_transport_conditions", "direction": "up", "magnitude": 0.71},
            {"name": "elevated_bloom_proxy", "direction": "up", "magnitude": 0.48},
        ],
        "recommended_action": "Avoid entering the water and avoid catch consumption from adjacent nearshore zones until conditions improve.",
        "evidence": [
            "Risk remains elevated over the next 24 hours",
            "Recent coastal history and transport conditions suggest persistence",
        ],
    },
}

MOCK_FISHING = {
    "san-diego-nearshore-01": {
        "location_id": "san-diego-nearshore-01",
        "name": "San Diego Nearshore Zone 01",
        "wrapper": "fishing",
        "forecast": [
            {"t": (NOW + timedelta(hours=0)).isoformat(), "risk_score": 0.55},
            {"t": (NOW + timedelta(hours=12)).isoformat(), "risk_score": 0.62},
            {"t": (NOW + timedelta(hours=24)).isoformat(), "risk_score": 0.69},
            {"t": (NOW + timedelta(hours=48)).isoformat(), "risk_score": 0.60},
        ],
        "risk_bucket": "elevated",
        "uncertainty_score": 0.22,
        "top_factors": [
            {"name": "biotoxin_history_proxy", "direction": "up", "magnitude": 0.66},
            {"name": "chlorophyll_shift", "direction": "up", "magnitude": 0.54},
        ],
        "recommended_action": "Use caution for catch consumption from this zone and verify current public-health guidance.",
        "evidence": [
            "Modeled toxin propensity is elevated versus recent baseline",
            "Environmental conditions resemble prior moderate-risk periods",
        ],
    }
}