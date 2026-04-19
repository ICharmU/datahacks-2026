from __future__ import annotations

from common.json_source import ingest_json_or_html_artifacts
from common.types import IngestionContext
from .nws_marine_manifest import NWS_MARINE_POINTS

SOURCE_NAME = "nws_marine"


def _build_items() -> list[dict]:
    items = []

    for p in NWS_MARINE_POINTS:
        lat = p["lat"]
        lon = p["lon"]
        group = p["group"]

        # points lookup
        items.append(
            {
                "group": group,
                "url": f"https://api.weather.gov/points/{lat},{lon}",
                "filename": f"{group}_points.json",
                "kind": "json",
            }
        )

        # We don't know office/grid ahead of time, so also save the point lookup;
        # later Bronze/Databricks can resolve forecastGridData endpoint from it.
        # For now, keep ingestion simple and stable.

    return items


def run(ctx: IngestionContext) -> dict:
    items = _build_items()
    return ingest_json_or_html_artifacts(
        ctx=ctx,
        source_name=SOURCE_NAME,
        items=items,
    )