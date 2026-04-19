from django.urls import path
from .views import app_config, science_summary

urlpatterns = [
    path("config/", app_config, name="app-config"),
    path("product/science/summary/", science_summary, name="science-summary"),
]