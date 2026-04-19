from django.urls import path
from .views import ingestion_status, ingestion_trigger, pipeline_status

urlpatterns = [
    path("ingestion/status/", ingestion_status, name="ingestion-status"),
    path("ingestion/pipeline-status/", pipeline_status, name="pipeline-status"),
    path("ingestion/trigger/", ingestion_trigger, name="ingestion-trigger"),
]