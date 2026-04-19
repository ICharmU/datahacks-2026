from __future__ import annotations

from pathlib import Path
import requests


def stream_download(url: str, dest_path: Path, *, timeout: int = 120, user_agent: str = "toxictide-data-intake/0.1") -> tuple[str | None, str | None]:
    headers = {"User-Agent": user_agent}
    with requests.get(url, stream=True, timeout=timeout, headers=headers) as response:
        response.raise_for_status()
        content_type = response.headers.get("Content-Type")
        content_disposition = response.headers.get("Content-Disposition")
        with dest_path.open("wb") as out:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    out.write(chunk)
        return content_type, content_disposition