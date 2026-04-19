from __future__ import annotations

from pathlib import Path
import json

from .utils import ensure_dir


class SourceManifest:
    def __init__(self, *, source_name: str, run_id: str, run_date: str):
        self.source_name = source_name
        self.run_id = run_id
        self.run_date = run_date
        self.records: list[dict] = []
        self.status = "running"
        self.started_at_utc = run_id
        self.ended_at_utc: str | None = None
        self.notes: list[str] = []

    def add_record(self, record: dict) -> None:
        self.records.append(record)

    def add_note(self, note: str) -> None:
        self.notes.append(note)

    def finish(self, *, status: str, ended_at_utc: str) -> None:
        self.status = status
        self.ended_at_utc = ended_at_utc

    def to_dict(self) -> dict:
        return {
            "source_name": self.source_name,
            "run_id": self.run_id,
            "run_date": self.run_date,
            "status": self.status,
            "started_at_utc": self.started_at_utc,
            "ended_at_utc": self.ended_at_utc,
            "notes": self.notes,
            "records": self.records,
        }

    def write_local(self, path: Path) -> None:
        ensure_dir(path.parent)
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True), encoding="utf-8")