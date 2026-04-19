from __future__ import annotations

from common.tabular_source import ingest_tabular_files
from common.types import IngestionContext
from .sd_beach_water_quality_manifest import SD_BEACH_WATER_QUALITY_FILES

SOURCE_NAME = "sd_beach_water_quality"


def run(ctx: IngestionContext) -> dict:
    return ingest_tabular_files(
        ctx=ctx,
        source_name=SOURCE_NAME,
        items=SD_BEACH_WATER_QUALITY_FILES,
        expected_suffixes={".csv"},
    )