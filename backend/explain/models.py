from django.db import models


class ExplanationCache(models.Model):
    wrapper = models.CharField(max_length=20)
    location_id = models.CharField(max_length=100)
    forecast_for_utc = models.DateTimeField()
    explanation_markdown = models.TextField()
    citations_json = models.JSONField(default=list)
    generated_at_utc = models.DateTimeField(auto_now_add=True)