from __future__ import annotations

from datetime import datetime, timezone

from common.tabular_source import ingest_tabular_files
from common.types import IngestionContext
from .noaa_tides_manifest import NOAA_TIDE_STATIONS, NOAA_TIDE_PRODUCTS

SOURCE_NAME = "noaa_tides"


def _build_items() -> list[dict]:
    items = []
    current_year = datetime.now(timezone.utc).year
    current_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    for station in NOAA_TIDE_STATIONS:
        station_id = station["station_id"]

        for product in NOAA_TIDE_PRODUCTS:
            for year in range(2020, current_year + 1):
                begin_date = f"{year}0101"
                end_date = current_date if year == current_year else f"{year}1231"

                url = (
                    "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
                    f"?begin_date={begin_date}"
                    f"&end_date={end_date}"
                    f"&station={station_id}"
                    f"&product={product}"
                    "&datum=MLLW"
                    "&time_zone=gmt"
                    "&units=metric"
                    "&application=ToxicTide"
                    "&format=csv"
                )

                items.append(
                    {
                        "group": f"station_{station_id}/{product}",
                        "url": url,
                        "filename": f"{product}_{year}.csv",
                    }
                )

    return items


def run(ctx: IngestionContext) -> dict:
    items = _build_items()
    return ingest_tabular_files(
        ctx=ctx,
        source_name=SOURCE_NAME,
        items=items,
        expected_suffixes={".csv"},
    )