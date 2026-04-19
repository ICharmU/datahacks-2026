from django.contrib import admin
from django.urls import path, include

api_patterns = [
    path("", include("core.urls")),
    path("", include("locations.urls")),
    path("", include("risk.urls")),
    path("", include("alerts.urls")),
    path("", include("explain.urls")),
    path("", include("ingestion.urls")),
]

urlpatterns = [
    path("admin/", admin.site.urls),
    path("api/", include(api_patterns)),
    path("api/v1/", include(api_patterns)),
]