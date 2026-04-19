from django.db import models


class RiskSnapshot(models.Model):
    WRAPPER_CHOICES = [
        ("beach", "Beach"),
        ("fishing", "Fishing"),
        ("surf", "Surf"),
        ("ecosystem", "Ecosystem"),
    ]

    location_type = models.CharField(max_length=50)
    location_id = models.CharField(max_length=100)
    wrapper = models.CharField(max_length=20, choices=WRAPPER_CHOICES)
    forecast_for_utc = models.DateTimeField()
    predicted_at_utc = models.DateTimeField()
    risk_score = models.FloatField()
    risk_bucket = models.CharField(max_length=50)
    uncertainty_score = models.FloatField(null=True, blank=True)
    top_factors = models.JSONField(default=list)
    source_artifact_uri = models.CharField(max_length=500, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["wrapper", "location_id", "forecast_for_utc"]),
        ]

    def __str__(self):
        return f"{self.wrapper}:{self.location_id}:{self.forecast_for_utc.isoformat()}"