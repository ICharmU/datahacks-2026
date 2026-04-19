from django.db import models


class CoastalSegment(models.Model):
    segment_id = models.CharField(max_length=100, unique=True)
    name = models.CharField(max_length=200)
    lat = models.FloatField()
    lon = models.FloatField()
    geojson = models.JSONField(default=dict)

    def __str__(self):
        return self.name


class Beach(models.Model):
    slug = models.SlugField(unique=True)
    name = models.CharField(max_length=200)
    county = models.CharField(max_length=100, blank=True)
    lat = models.FloatField()
    lon = models.FloatField()
    coastal_segment = models.ForeignKey(
        CoastalSegment,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="beaches",
    )
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return self.name