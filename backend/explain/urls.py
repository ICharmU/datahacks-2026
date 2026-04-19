from django.urls import path
from .views import beach_explanation, grower_explanation

urlpatterns = [
    path("explain/beach/<slug:slug>/", beach_explanation, name="explain-beach"),
    path("explain/grower/<str:site_id>/", grower_explanation, name="explain-grower"),
]