from __future__ import annotations

from pathlib import Path
from typing import BinaryIO, Any
import boto3
from botocore.exceptions import ClientError


def make_s3_client(profile_name: str | None = None, region_name: str = "us-west-2"):
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    return session.client("s3")


def s3_key_exists(s3, bucket_name: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def s3_head_object(s3, bucket_name: str, key: str) -> dict[str, Any] | None:
    try:
        return s3.head_object(Bucket=bucket_name, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def upload_path(
    s3,
    *,
    bucket_name: str,
    key: str,
    local_path: Path,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
) -> None:
    extra_args: dict[str, Any] = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if metadata:
        extra_args["Metadata"] = metadata

    if extra_args:
        s3.upload_file(str(local_path), bucket_name, key, ExtraArgs=extra_args)
    else:
        s3.upload_file(str(local_path), bucket_name, key)


def upload_fileobj(
    s3,
    *,
    bucket_name: str,
    key: str,
    fileobj: BinaryIO,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
) -> None:
    extra_args: dict[str, Any] = {}
    if content_type:
        extra_args["ContentType"] = content_type
    if metadata:
        extra_args["Metadata"] = metadata

    if extra_args:
        s3.upload_fileobj(fileobj, bucket_name, key, ExtraArgs=extra_args)
    else:
        s3.upload_fileobj(fileobj, bucket_name, key)