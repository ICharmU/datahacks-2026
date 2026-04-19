from django.conf import settings
from .mock_data import MOCK_MAP, MOCK_DETAILS, MOCK_FISHING


class RiskService:
    @staticmethod
    def use_mock_data() -> bool:
        return settings.TOXIC_TIDE_USE_MOCK_DATA

    @staticmethod
    def get_map(wrapper: str, horizon: str):
        if settings.TOXIC_TIDE_USE_MOCK_DATA:
            payload = dict(MOCK_MAP)
            payload["wrapper"] = wrapper
            payload["horizon"] = horizon
            return payload
        # later: query DB / S3 / Databricks export
        return {"generated_at_utc": None, "wrapper": wrapper, "horizon": horizon, "locations": []}

    @staticmethod
    def get_beach_detail(slug: str, horizon: str):
        if settings.TOXIC_TIDE_USE_MOCK_DATA:
            return MOCK_DETAILS.get(slug, MOCK_DETAILS["la-jolla-shores"])
        return None

    @staticmethod
    def get_fishing_detail(segment_id: str, horizon: str):
        if settings.TOXIC_TIDE_USE_MOCK_DATA:
            return MOCK_FISHING.get(segment_id, MOCK_FISHING["san-diego-nearshore-01"])
        return None