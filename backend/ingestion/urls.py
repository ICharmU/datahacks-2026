from django.urls import path
from .views import ingestion_status, ingestion_trigger

urlpatterns = [
    path("ingestion/status/", ingestion_status, name="ingestion-status"),
    path("ingestion/trigger/", ingestion_trigger, name="ingestion-trigger"),
]