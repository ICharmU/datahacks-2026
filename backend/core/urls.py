from django.urls import path
from .views import health, config_view, wrappers

urlpatterns = [
    path("health/", health, name="health"),
    path("config/", config_view, name="config"),
    path("wrappers/", wrappers, name="wrappers"),
]