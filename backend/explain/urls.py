from django.urls import path
from .views import beach_explanation, fishing_explanation

urlpatterns = [
    path("explain/beach/<slug:slug>/", beach_explanation, name="explain-beach"),
    path("explain/fishing/<str:segment_id>/", fishing_explanation, name="explain-fishing"),
]