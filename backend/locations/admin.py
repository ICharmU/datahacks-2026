from django.contrib import admin
from .models import Beach, CoastalSegment

@admin.register(Beach)
class BeachAdmin(admin.ModelAdmin):
    list_display = ("name", "slug", "county", "is_active")
    search_fields = ("name", "slug", "county")


@admin.register(CoastalSegment)
class CoastalSegmentAdmin(admin.ModelAdmin):
    list_display = ("segment_id", "name")
    search_fields = ("segment_id", "name")