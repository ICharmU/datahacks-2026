from django.urls import path
from .views import (
    grower_dashboard,
    grower_site_detail,
    fleet_dashboard,
    fleet_zone_detail,
    risk_map,
    beach_risk_detail,
    risk_query,
)

urlpatterns = [
    path("product/grower/dashboard/", grower_dashboard, name="grower-dashboard"),
    path("product/grower/sites/<str:site_id>/", grower_site_detail, name="grower-site-detail"),
    path("product/fleet/dashboard/", fleet_dashboard, name="fleet-dashboard"),
    path("product/fleet/zones/<str:zone_id>/", fleet_zone_detail, name="fleet-zone-detail"),

    path("risk/map/", risk_map, name="risk-map"),
    path("risk/beach/<slug:slug>/", beach_risk_detail, name="risk-beach-detail"),
    path("risk/query/", risk_query, name="risk-query"),
]