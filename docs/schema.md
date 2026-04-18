3A. Canonical scientific schema

This is your lakehouse-style normalized core.

canonical_observation

One row = one measured variable at one place/time/depth.

observation_id          string
source_name             string   # calcofi, cce_mooring, easyoneargo, cdip, beach_advisory, biotoxin
source_record_id        string
observed_at_utc         timestamp
lat                     double
lon                     double
depth_m                 double nullable
location_type           string   # station, mooring, profile, beach, coastal_segment, offshore_point
location_id             string
variable_name           string   # temperature, salinity, ph, chlorophyll_a, wave_height_m, advisory_flag
value_numeric           double nullable
value_text              string nullable
unit                    string nullable
quality_flag            string nullable
ingested_at_utc         timestamp

Why this matters:
everything upstream can be ugly, but modeling code gets one universal input surface.

canonical_feature_snapshot

One row = one place/time horizon-ready feature vector.

feature_snapshot_id     string
generated_at_utc        timestamp
target_time_utc         timestamp
location_id             string
wrapper_scope           string   # shared, beach, fishing, surf, ecosystem
feature_family          string   # physics, chemistry, biology, waves, satellite, labels
feature_name            string
feature_value           double
canonical_label_event

One row = one target/label instance.

label_event_id          string
label_type              string   # beach_unsafe, fishing_toxicity_risk, ecosystem_stress_event
label_source            string   # state_beach_advisory, cdph_biotoxin, weak_label_hab
location_id             string
start_at_utc            timestamp
end_at_utc              timestamp nullable
label_value             int      # 0/1 or bucket
label_confidence        double nullable
notes                   string nullable
canonical_prediction

One row = one model output.

prediction_id           string
model_name              string
model_version           string
wrapper_scope           string
location_id             string
forecast_for_utc        timestamp
predicted_at_utc        timestamp
risk_score              double
risk_bucket             string
uncertainty_score       double nullable
top_factors_json        json
artifact_uri            string nullable
3B. Source-specific staging schemas

These are the raw-to-clean intermediate tables.

stg_calcofi_sample
calcofi_sample_id
cruise_id
station_code
sample_time_utc
lat
lon
depth_m
temperature_c
salinity_psu
oxygen_ml_l
phosphate
silicate
nitrate
chlorophyll
ph
...
raw_payload_uri
stg_cce_mooring_observation
cce_obs_id
mooring_id
observed_at_utc
lat
lon
depth_m
temperature_c
salinity_psu
ph
pco2
dissolved_oxygen
wind_speed
air_temp_c
...
raw_payload_uri
stg_easyoneargo_profile
argo_profile_id
float_id
profile_time_utc
lat
lon
pressure_dbar
depth_m
temperature_c
salinity_psu
qc_flag
raw_payload_uri
stg_cdip_wave_obs
cdip_obs_id
station_id
observed_at_utc
lat
lon
significant_wave_height_m
dominant_period_s
peak_period_s
mean_direction_deg
sea_surface_temp_c nullable
raw_payload_uri
stg_beach_advisory_event
advisory_event_id
beach_id
beach_name
county
lat
lon
started_at_utc
ended_at_utc nullable
advisory_type
status
reason_text
source_url
raw_payload_uri
stg_biotoxin_sample
biotoxin_sample_id
program_name
sample_time_utc
lat nullable
lon nullable
zone_id nullable
species_or_shellfish_type
domoic_acid_ppm nullable
psp_level nullable
closure_flag nullable
source_url
raw_payload_uri
3C. Django product-serving schema

Django should hold only what the app actually serves and manages.

locations.Beach
from django.db import models

class Beach(models.Model):
    slug = models.SlugField(unique=True)
    name = models.CharField(max_length=200)
    county = models.CharField(max_length=100, blank=True)
    lat = models.FloatField()
    lon = models.FloatField()
    coastal_segment_id = models.CharField(max_length=100, blank=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return self.name
locations.CoastalSegment
class CoastalSegment(models.Model):
    segment_id = models.CharField(max_length=100, unique=True)
    name = models.CharField(max_length=200)
    lat = models.FloatField()
    lon = models.FloatField()
    geojson = models.JSONField(default=dict)
risk.RiskSnapshot
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
alerts.UserAlert
from django.contrib.auth import get_user_model

User = get_user_model()

class UserAlert(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    wrapper = models.CharField(max_length=20)
    location_type = models.CharField(max_length=50)
    location_id = models.CharField(max_length=100)
    min_risk_score = models.FloatField(default=0.7)
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
explain.ExplanationCache
class ExplanationCache(models.Model):
    wrapper = models.CharField(max_length=20)
    location_id = models.CharField(max_length=100)
    forecast_for_utc = models.DateTimeField()
    explanation_markdown = models.TextField()
    citations_json = models.JSONField(default=list)
    generated_at_utc = models.DateTimeField(auto_now_add=True)