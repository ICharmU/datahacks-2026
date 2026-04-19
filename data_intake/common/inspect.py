from __future__ import annotations

from pathlib import Path
from typing import Any
import csv
import json

import pandas as pd
import xarray as xr

from .utils import suffix_lower


TEXT_ENCODINGS_TO_TRY = [
    "utf-8",
    "utf-8-sig",
    "cp1252",
    "latin1",
]


def sniff_text_header(path: Path, n_bytes: int = 4096) -> str:
    with path.open("rb") as f:
        raw = f.read(n_bytes)
    return raw.decode("utf-8", errors="replace").lstrip()


def looks_like_html(path: Path) -> bool:
    head = sniff_text_header(path).lower()
    return (
        head.startswith("<!doctype html")
        or head.startswith("<html")
        or "<html" in head[:500]
        or "<head" in head[:500]
        or "<body" in head[:500]
    )


def looks_like_xml(path: Path) -> bool:
    head = sniff_text_header(path).lower()
    return head.startswith("<?xml") or "<xml" in head[:500]


def detect_delimiter(path: Path, encoding: str) -> str:
    with path.open("r", encoding=encoding, errors="replace", newline="") as f:
        sample = f.read(4096)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        return dialect.delimiter
    except Exception:
        return ","


def _read_tabular_preview(path: Path, sample_rows: int = 5) -> tuple[pd.DataFrame, str, str]:
    last_error: Exception | None = None

    for encoding in TEXT_ENCODINGS_TO_TRY:
        try:
            delimiter = detect_delimiter(path, encoding)
            preview = pd.read_csv(
                path,
                nrows=sample_rows,
                encoding=encoding,
                sep=delimiter,
                engine="python",   # more forgiving than C engine
                on_bad_lines="warn",
            )
            return preview, encoding, delimiter
        except Exception as e:
            last_error = e

    raise RuntimeError(f"Unable to parse tabular file {path} with tried encodings {TEXT_ENCODINGS_TO_TRY}") from last_error


def summarize_csv(path: Path, sample_rows: int = 5) -> dict[str, Any]:
    preview, encoding, delimiter = _read_tabular_preview(path, sample_rows=sample_rows)
    return {
        "file_type": "csv",
        "size_bytes": path.stat().st_size,
        "encoding_used": encoding,
        "delimiter_used": delimiter,
        "columns": list(preview.columns),
        "dtypes_preview": {k: str(v) for k, v in preview.dtypes.to_dict().items()},
        "sample_rows": sample_rows,
        "preview": preview.to_dict(orient="records"),
    }


def summarize_tsv(path: Path, sample_rows: int = 5) -> dict[str, Any]:
    preview, encoding, delimiter = _read_tabular_preview(path, sample_rows=sample_rows)
    return {
        "file_type": "tsv",
        "size_bytes": path.stat().st_size,
        "encoding_used": encoding,
        "delimiter_used": delimiter,
        "columns": list(preview.columns),
        "dtypes_preview": {k: str(v) for k, v in preview.dtypes.to_dict().items()},
        "sample_rows": sample_rows,
        "preview": preview.to_dict(orient="records"),
    }


def summarize_netcdf(path: Path) -> dict[str, Any]:
    try:
        with xr.open_dataset(path, decode_times=False) as ds:
            dims = {k: int(v) for k, v in ds.sizes.items()}

            vars_summary = {}
            for name, var in ds.variables.items():
                vars_summary[name] = {
                    "dims": list(var.dims),
                    "dtype": str(var.dtype),
                    "shape": [int(ds.sizes[d]) for d in var.dims if d in ds.sizes],
                    "attrs": {k: str(v) for k, v in list(var.attrs.items())[:10]},
                }

            coord_names = list(ds.coords.keys())
            data_var_names = list(ds.data_vars.keys())

            return {
                "file_type": "netcdf",
                "size_bytes": path.stat().st_size,
                "dimensions": dims,
                "coordinates": coord_names,
                "data_variables": data_var_names,
                "variables": vars_summary,
                "global_attrs": {k: str(v) for k, v in list(ds.attrs.items())[:20]},
            }
    except Exception as e:
        return {
            "file_type": "netcdf_unparsed",
            "size_bytes": path.stat().st_size,
            "summary_error": str(e),
            "note": "Fell back to metadata-only NetCDF summary after parse failure.",
        }


def summarize_textlike(path: Path, sample_lines: int = 20) -> dict[str, Any]:
    lines = []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if i >= sample_lines:
                break
            lines.append(line.rstrip("\n"))
    return {
        "file_type": "text",
        "size_bytes": path.stat().st_size,
        "sample_lines": sample_lines,
        "preview_lines": lines,
    }


def summarize_html(path: Path, sample_lines: int = 20) -> dict[str, Any]:
    base = summarize_textlike(path, sample_lines=sample_lines)
    base["file_type"] = "html"
    return base


def summarize_xml(path: Path, sample_lines: int = 20) -> dict[str, Any]:
    base = summarize_textlike(path, sample_lines=sample_lines)
    base["file_type"] = "xml"
    return base


def summarize_file(path: Path) -> dict[str, Any]:
    suffix = suffix_lower(path)

    if looks_like_html(path):
        return summarize_html(path)
    if looks_like_xml(path):
        return summarize_xml(path)

    if suffix == ".csv":
        return summarize_csv(path)
    if suffix in {".tsv", ".tab"}:
        return summarize_tsv(path)
    if suffix in {".nc", ".netcdf"}:
        return summarize_netcdf(path)
    return summarize_textlike(path)


def write_summary_json(path: Path, summary: dict[str, Any]) -> None:
    path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")