from django.urls import path
from .views import (
    risk_map,
    beach_risk_detail,
    fishing_risk_detail,
    risk_query,
)

urlpatterns = [
    path("risk/map/", risk_map, name="risk-map"),
    path("risk/beach/<slug:slug>/", beach_risk_detail, name="risk-beach-detail"),
    path("risk/fishing/<str:segment_id>/", fishing_risk_detail, name="risk-fishing-detail"),
    path("risk/query/", risk_query, name="risk-query"),
]