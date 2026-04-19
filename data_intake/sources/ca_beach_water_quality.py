from __future__ import annotations

from common.tabular_source import ingest_tabular_files
from common.types import IngestionContext
from .ca_beach_water_quality_manifest import CA_BEACH_WATER_QUALITY_FILES

SOURCE_NAME = "ca_beach_water_quality"


def run(ctx: IngestionContext) -> dict:
    return ingest_tabular_files(
        ctx=ctx,
        source_name=SOURCE_NAME,
        items=CA_BEACH_WATER_QUALITY_FILES,
        expected_suffixes={".csv"},
    )