from django.conf import settings
from rest_framework.decorators import api_view
from common.responses import api_ok
from ingestion.services import ServingDataService


@api_view(["GET"])
def app_config(request):
    return api_ok(
        {
            "env": settings.TOXIC_TIDE_ENV,
            "use_mock_data": settings.TOXIC_TIDE_USE_MOCK_DATA,
            "wrappers": ["aquaculture", "beach", "ecosystem", "fishing"],
        }
    )


@api_view(["GET"])
def science_summary(request):
    overview_rows = ServingDataService.fetch_json("system_overview")
    coverage = ServingDataService.fetch_json("source_coverage_summary")
    model_runs = ServingDataService.fetch_json("model_run_summary")

    overview = overview_rows[0] if isinstance(overview_rows, list) and overview_rows else {}

    datasets = [
        {
            "key": row.get("source_name"),
            "name": row.get("source_name", "").replace("_", " ").title(),
            "role": "Active inference source" if row.get("is_active") else "Planned / inactive source",
            "status": "connected" if row.get("is_active") else "planned",
            "granularity": "site-daily",
        }
        for row in coverage
    ]

    runs = [
        {
            "run_id": row.get("mlflow_run_id") or row.get("model_name"),
            "name": row.get("model_name"),
            "status": "success",
            "target": row.get("model_type"),
            "score_label": row.get("primary_metric_name"),
            "score_value": str(row.get("primary_metric_value")),
            "notes": f"rows={row.get('n_rows')}",
        }
        for row in model_runs
    ]

    architecture_cards = [
        {
            "title": "Aquaculture Sites",
            "value": str(overview.get("n_aquaculture_sites", 0)),
            "tone": "good",
        },
        {
            "title": "High / Severe Aquaculture Alerts",
            "value": str(overview.get("n_aquaculture_high_or_severe", 0)),
            "tone": "warn",
        },
        {
            "title": "Beach Sites",
            "value": str(overview.get("n_beach_sites", 0)),
            "tone": "neutral",
        },
        {
            "title": "Model Runs",
            "value": str(overview.get("n_model_runs", 0)),
            "tone": "neutral",
        },
    ]

    return api_ok(
        {
            "datasets": datasets,
            "model_runs": runs,
            "architecture_cards": architecture_cards,
        }
    )