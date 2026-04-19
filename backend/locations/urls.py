from django.urls import path
from .views import (
    beach_list,
    beach_detail,
    coastal_segment_list,
    coastal_segment_detail,
)

urlpatterns = [
    path("beaches/", beach_list, name="beach-list"),
    path("beaches/<slug:slug>/", beach_detail, name="beach-detail"),
    path("coastal-segments/", coastal_segment_list, name="segment-list"),
    path("coastal-segments/<str:segment_id>/", coastal_segment_detail, name="segment-detail"),
]