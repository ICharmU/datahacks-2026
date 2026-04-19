from __future__ import annotations

from common.json_source import ingest_json_or_html_artifacts
from common.types import IngestionContext
from .cdph_biotoxin_manifest import CDPH_BIOTOXIN_ARTIFACTS

SOURCE_NAME = "cdph_biotoxin"


def run(ctx: IngestionContext) -> dict:
    return ingest_json_or_html_artifacts(
        ctx=ctx,
        source_name=SOURCE_NAME,
        items=CDPH_BIOTOXIN_ARTIFACTS,
    )