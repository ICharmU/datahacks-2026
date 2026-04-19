import json
from datetime import datetime
from django.conf import settings
from ingestion.services import ServingDataService
from .mock_data import MOCK_MAP, MOCK_DETAILS, MOCK_FISHING


def _to_iso_date(value):
    if not value:
        return datetime.utcnow().isoformat() + "Z"
    if "T" in str(value):
        return str(value)
    return f"{value}T00:00:00Z"


def _severity_from_level(level: str):
    if level in {"severe", "high"}:
        return "warning"
    if level == "moderate":
        return "watch"
    return "info"


def _recommendation_from_alert_status(status: str):
    mapping = {
        "act_now": "warn_buyers",
        "urgent_watch": "sample",
        "watch": "delay",
        "stable": "harvest",
        "avoid_water": "avoid",
        "caution": "caution",
        "open": "go",
    }
    return mapping.get(status, "caution")


def _parse_top_drivers(top_drivers_json: str):
    try:
        raw = json.loads(top_drivers_json or "[]")
    except Exception:
        raw = []
    out = []
    for item in raw[:5]:
        out.append(
            {
                "name": str(item.get("driver", "signal")).replace("_", " ").title(),
                "direction": "up",
                "magnitude": float(item.get("score", 0.0)),
                "description": f"Modeled contribution from {str(item.get('driver', 'signal')).replace('_', ' ')}",
            }
        )
    return out


class RiskService:
    @staticmethod
    def use_mock_data() -> bool:
        return settings.TOXIC_TIDE_USE_MOCK_DATA

    @staticmethod
    def get_aquaculture_watchlist():
        return ServingDataService.fetch_json("aquaculture_watchlist")

    @staticmethod
    def get_aquaculture_timeseries():
        return ServingDataService.fetch_json("aquaculture_timeseries")

    @staticmethod
    def get_beach_scores():
        return ServingDataService.fetch_json("beach_daily_scores")

    @staticmethod
    def get_beach_timeseries():
        return ServingDataService.fetch_json("beach_timeseries")

    @staticmethod
    def get_explanations():
        return ServingDataService.fetch_json("explanations")

    @staticmethod
    def get_map(shell: str, wrapper: str, horizon: str):
        if wrapper == "beach":
            rows = RiskService.get_beach_scores()
            return {
                "generated_at_utc": datetime.utcnow().isoformat() + "Z",
                "shell": shell,
                "wrapper": wrapper,
                "horizon": horizon,
                "locations": [
                    {
                        "location_id": r["site_id"],
                        "name": r["site_name"],
                        "lat": r["lat_dec"],
                        "lon": r["lon_dec"],
                        "risk_score": r["risk_score"],
                        "risk_bucket": r["risk_level"],
                        "uncertainty_score": None if r.get("confidence") is None else (1 - float(r["confidence"])),
                        "recommendation": _recommendation_from_alert_status(r.get("alert_status", "watch")),
                    }
                    for r in rows
                ],
            }

        if shell == "grower" or wrapper == "aquaculture":
            rows = RiskService.get_aquaculture_watchlist()
            return {
                "generated_at_utc": datetime.utcnow().isoformat() + "Z",
                "shell": "grower",
                "wrapper": "aquaculture",
                "horizon": horizon,
                "locations": [
                    {
                        "location_id": r["site_id"],
                        "name": r["site_name"],
                        "lat": r["lat_dec"],
                        "lon": r["lon_dec"],
                        "risk_score": r["risk_score"],
                        "risk_bucket": r["risk_level"],
                        "uncertainty_score": None if r.get("confidence") is None else (1 - float(r["confidence"])),
                        "recommendation": _recommendation_from_alert_status(r.get("alert_status", "watch")),
                    }
                    for r in rows
                ],
            }

        if settings.TOXIC_TIDE_ALLOW_MOCK_FLEET:
            payload = dict(MOCK_MAP)
            payload["wrapper"] = wrapper
            payload["horizon"] = horizon
            payload["shell"] = shell
            return payload

        return {"generated_at_utc": None, "shell": shell, "wrapper": wrapper, "horizon": horizon, "locations": []}

    @staticmethod
    def get_grower_dashboard(region: str):
        rows = RiskService.get_aquaculture_watchlist()
        rows = [r for r in rows if region.lower() in (r.get("region_name") or "").lower()] or rows

        alerts = []
        for r in rows:
            if r.get("risk_level") in {"high", "severe"}:
                alerts.append(
                    {
                        "id": r["site_id"],
                        "title": f"{r['site_name']} {r['risk_level']} risk",
                        "severity": _severity_from_level(r["risk_level"]),
                        "source": "AquaYield Risk Engine",
                        "updated_at": _to_iso_date(r.get("calendar_date")),
                        "summary": f"{r.get('alert_status', 'watch').replace('_', ' ').title()} based on current coastal risk signals.",
                    }
                )

        summary_cards = [
            {
                "title": "Sites Tracked",
                "value": str(len(rows)),
                "tone": "neutral",
            },
            {
                "title": "High / Severe",
                "value": str(sum(1 for r in rows if r.get('risk_level') in {'high', 'severe'})),
                "tone": "warn",
            },
            {
                "title": "Avg Confidence",
                "value": f"{round(100 * (sum(float(r.get('confidence') or 0) for r in rows) / max(len(rows),1)))}%",
                "tone": "good",
            },
        ]

        sites = [
            {
                "site_id": r["site_id"],
                "site_name": r["site_name"],
                "lat": r["lat_dec"],
                "lon": r["lon_dec"],
                "recommendation": _recommendation_from_alert_status(r.get("alert_status", "watch")),
                "risk_score": r["risk_score"],
                "harvest_window_label": f"{str(r.get('risk_level', 'unknown')).title()} risk · {r.get('calendar_date')}",
                "confidence_score": float(r.get("confidence") or 0.0),
                "latest_signal_summary": f"Biogeochemistry={round(float(r.get('component_biogeochemistry') or 0), 2)} | Contamination={round(float(r.get('component_contamination_proxy') or 0), 2)} | Hydrodynamics={round(float(r.get('component_hydrodynamics') or 0), 2)}",
            }
            for r in rows
        ]

        return {
            "generated_at_utc": datetime.utcnow().isoformat() + "Z",
            "region_name": region,
            "shell": "grower",
            "summary_cards": summary_cards,
            "sites": sites,
            "alerts": alerts,
        }

    @staticmethod
    def get_grower_site_detail(site_id: str):
        latest_rows = [r for r in RiskService.get_aquaculture_watchlist() if r.get("site_id") == site_id]
        ts_rows = [r for r in RiskService.get_aquaculture_timeseries() if r.get("site_id") == site_id]
        expl_rows = [r for r in RiskService.get_explanations() if r.get("product_shell") == "aquaculture" and r.get("site_id") == site_id]

        if not latest_rows:
            return None

        latest = latest_rows[0]
        explanation = expl_rows[0] if expl_rows else {}

        forecast = [
            {"t": _to_iso_date(r.get("calendar_date")), "value": float(r.get("risk_score") or 0.0)}
            for r in ts_rows
        ]

        top_factors = _parse_top_drivers(latest.get("top_drivers_json", "[]"))
        advisories = [
            {
                "id": f"{site_id}-advisory",
                "title": explanation.get("headline", f"{latest['site_name']} status"),
                "severity": _severity_from_level(latest.get("risk_level", "moderate")),
                "source": "AquaYield",
                "updated_at": _to_iso_date(latest.get("calendar_date")),
                "summary": explanation.get("explanation_text", "Elevated nearshore risk signals detected."),
            }
        ]

        sampling_priorities = [
            {
                "label": factor["name"],
                "priority": "high" if factor["magnitude"] >= 0.66 else ("medium" if factor["magnitude"] >= 0.4 else "low"),
                "reason": factor["description"],
            }
            for factor in top_factors[:3]
        ]

        return {
            "site_id": latest["site_id"],
            "site_name": latest["site_name"],
            "recommendation": _recommendation_from_alert_status(latest.get("alert_status", "watch")),
            "confidence_score": float(latest.get("confidence") or 0.0),
            "harvest_window_label": f"{str(latest.get('risk_level', 'unknown')).title()} risk · {latest.get('calendar_date')}",
            "forecast": forecast,
            "top_factors": top_factors,
            "advisories": advisories,
            "sampling_priorities": sampling_priorities,
            "buyer_warning_recommended": latest.get("risk_level") in {"high", "severe"},
            "recommended_action_text": explanation.get("explanation_text", "Review current site conditions before harvest or sale decisions."),
            "evidence": [
                f"Risk score: {round(float(latest.get('risk_score') or 0), 3)}",
                f"Confidence: {round(float(latest.get('confidence') or 0), 3)}",
                f"Top drivers: {latest.get('top_drivers_json', '[]')}",
                explanation.get("headline", ""),
            ],
        }

    @staticmethod
    def get_beach_detail(site_id: str, horizon: str):
        rows = [r for r in RiskService.get_beach_scores() if r.get("site_id") == site_id]
        ts_rows = [r for r in RiskService.get_beach_timeseries() if r.get("site_id") == site_id]
        expl_rows = [r for r in RiskService.get_explanations() if r.get("product_shell") == "beach" and r.get("site_id") == site_id]

        if rows:
            latest = rows[0]
            explanation = expl_rows[0] if expl_rows else {}
            return {
                "location_id": latest["site_id"],
                "name": latest["site_name"],
                "wrapper": "beach",
                "forecast": [{"t": _to_iso_date(r.get("calendar_date")), "risk_score": float(r.get("risk_score") or 0)} for r in ts_rows],
                "risk_bucket": latest["risk_level"],
                "uncertainty_score": None if latest.get("confidence") is None else (1 - float(latest["confidence"])),
                "top_factors": _parse_top_drivers(latest.get("top_drivers_json", "[]")),
                "recommended_action": _recommendation_from_alert_status(latest.get("alert_status", "watch")),
                "evidence": [explanation.get("headline", ""), explanation.get("explanation_text", "")],
            }

        if settings.TOXIC_TIDE_USE_MOCK_DATA:
            return MOCK_DETAILS.get(site_id, MOCK_DETAILS["la-jolla-shores"])
        return None

    @staticmethod
    def get_fleet_dashboard(port_name: str, target_species: str):
        # Keep fleet alive as a secondary/mocked shell during the hackathon.
        payload = dict(MOCK_MAP)
        payload["generated_at_utc"] = datetime.utcnow().isoformat() + "Z"
        return {
            "generated_at_utc": payload["generated_at_utc"],
            "port_name": port_name,
            "target_species": target_species,
            "weather_summary": "Mock fleet shell kept online for demo completeness.",
            "shell": "fleet",
            "summary_cards": [
                {"title": "Mode", "value": "Mock", "tone": "neutral"},
                {"title": "Target Species", "value": target_species, "tone": "good"},
            ],
            "recommendations": [],
            "map_points": payload["locations"],
            "alerts": [],
        }

    @staticmethod
    def get_fleet_zone_detail(zone_id: str):
        if settings.TOXIC_TIDE_USE_MOCK_DATA:
            return MOCK_FISHING.get(zone_id, MOCK_FISHING["san-diego-nearshore-01"])
        return None