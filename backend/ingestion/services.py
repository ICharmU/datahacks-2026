import json
import boto3
from botocore.exceptions import ClientError
from django.conf import settings
from django.core.cache import cache
import logging

logging.basicConfig(level=logging.INFO)

class ServingDataService:
    ARTIFACTS = [
        "manifest",
        "system_overview",
        "source_coverage_summary",
        "aquaculture_watchlist",
        "aquaculture_timeseries",
        "beach_daily_scores",
        "beach_timeseries",
        "explanations",
        "site_timeseries",
        "model_run_summary",
    ]

    @classmethod
    def s3_client(cls):
        return boto3.client("s3", region_name=settings.TOXIC_TIDE_AWS_REGION)

    @classmethod
    def artifact_key(cls, name: str) -> str:
        base = settings.TOXIC_TIDE_SERVING_PREFIX.rstrip("/")
        return f"{base}/{name}.json"

    @classmethod
    def fetch_json(cls, name: str, force: bool = False):
        cache_key = f"serving:{name}"

        if not force:
            cached = cache.get(cache_key)
            if cached is not None:
                return cached

        try:
            obj = cls.s3_client().get_object(
                Bucket=settings.TOXIC_TIDE_SERVING_BUCKET,
                Key=cls.artifact_key(name),
            )
            body = obj["Body"].read().decode("utf-8")
            data = json.loads(body)
        except Exception as e:
            # Catch EVERYTHING during the hackathon and print it so you aren't flying blind
            print(f"⚠️ S3 Fetch Error for {name}: {str(e)}") 
            data = {} if name == "manifest" else []

        cache.set(cache_key, data, timeout=settings.TOXIC_TIDE_SERVING_CACHE_SECONDS)
        return data

    @classmethod
    def sync_all(cls, force: bool = False):
        out = {}
        for name in cls.ARTIFACTS:
            payload = cls.fetch_json(name, force=force)
            out[name] = len(payload) if isinstance(payload, list) else (1 if payload else 0)
        return out