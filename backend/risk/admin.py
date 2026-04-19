from django.contrib import admin
from .models import RiskSnapshot

@admin.register(RiskSnapshot)
class RiskSnapshotAdmin(admin.ModelAdmin):
    list_display = ("wrapper", "location_id", "forecast_for_utc", "risk_score", "risk_bucket")
    list_filter = ("wrapper", "risk_bucket")
    search_fields = ("location_id",)