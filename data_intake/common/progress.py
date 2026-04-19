from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
import os
import threading

from boto3.s3.transfer import TransferConfig
from tqdm import tqdm

from .aws import upload_path


@dataclass
class TransferSettings:
    download_chunk_size: int = 1024 * 1024          # 1 MB
    multipart_threshold: int = 8 * 1024 * 1024      # 8 MB
    multipart_chunksize: int = 16 * 1024 * 1024     # 16 MB
    max_concurrency: int = 8
    use_threads: bool = True


class TqdmCallback:
    def __init__(self, pbar: tqdm):
        self.pbar = pbar
        self._lock = threading.Lock()

    def __call__(self, bytes_amount: int) -> None:
        with self._lock:
            self.pbar.update(bytes_amount)


def s3_transfer_config(settings: TransferSettings) -> TransferConfig:
    return TransferConfig(
        multipart_threshold=settings.multipart_threshold,
        multipart_chunksize=settings.multipart_chunksize,
        max_concurrency=settings.max_concurrency,
        use_threads=settings.use_threads,
    )


def upload_path_with_progress(
    s3,
    *,
    bucket_name: str,
    key: str,
    local_path: Path,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
    settings: TransferSettings | None = None,
    position: int = 0,
) -> None:
    settings = settings or TransferSettings()
    total = local_path.stat().st_size

    with tqdm(
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc=f"upload:{local_path.name}",
        position=position,
        leave=True,
    ) as pbar:
        callback = TqdmCallback(pbar)
        config = s3_transfer_config(settings)

        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        if metadata:
            extra_args["Metadata"] = metadata

        if extra_args:
            s3.upload_file(
                str(local_path),
                bucket_name,
                key,
                ExtraArgs=extra_args,
                Callback=callback,
                Config=config,
            )
        else:
            s3.upload_file(
                str(local_path),
                bucket_name,
                key,
                Callback=callback,
                Config=config,
            )