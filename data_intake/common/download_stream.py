from __future__ import annotations

from pathlib import Path
import requests
from tqdm import tqdm

from .progress import TransferSettings


def stream_download_to_path(
    url: str,
    dest_path: Path,
    *,
    timeout: int = 120,
    user_agent: str = "toxictide-data-intake/0.1",
    settings: TransferSettings | None = None,
    desc: str | None = None,
    position: int = 0,
) -> tuple[str | None, str | None, int]:
    settings = settings or TransferSettings()
    headers = {"User-Agent": user_agent}

    with requests.get(url, stream=True, timeout=timeout, headers=headers) as response:
        response.raise_for_status()

        total = int(response.headers.get("Content-Length", 0))
        content_type = response.headers.get("Content-Type")
        content_disposition = response.headers.get("Content-Disposition")

        with dest_path.open("wb") as out, tqdm(
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc=desc or f"download:{dest_path.name}",
            position=position,
            leave=True,
        ) as pbar:
            for chunk in response.iter_content(chunk_size=settings.download_chunk_size):
                if not chunk:
                    continue
                out.write(chunk)
                pbar.update(len(chunk))

    size_bytes = dest_path.stat().st_size
    return content_type, content_disposition, size_bytes