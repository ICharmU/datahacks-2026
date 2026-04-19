from django.urls import path
from .views import alert_list, alert_create, alert_update, alert_delete

urlpatterns = [
    path("alerts/", alert_list, name="alert-list"),
    path("alerts/create/", alert_create, name="alert-create"),
    path("alerts/<int:alert_id>/", alert_update, name="alert-update"),
    path("alerts/<int:alert_id>/delete/", alert_delete, name="alert-delete"),
]