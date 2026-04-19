from __future__ import annotations

from common.json_source import ingest_json_or_html_artifacts
from common.types import IngestionContext
from .nccos_hab_manifest import NCCOS_HAB_ARTIFACTS

SOURCE_NAME = "nccos_hab"


def run(ctx: IngestionContext) -> dict:
    return ingest_json_or_html_artifacts(
        ctx=ctx,
        source_name=SOURCE_NAME,
        items=NCCOS_HAB_ARTIFACTS,
    )