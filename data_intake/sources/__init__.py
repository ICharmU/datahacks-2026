from . import (
    calcofi,
    cce_moorings,
    easyoneargo,
    ca_beach_water_quality,
    sd_beach_water_quality,
    noaa_tides,
    cdip,
    nws_marine,
    cdph_biotoxin,
    nccos_hab,
    nasa_ocean_color,
)

RUNNERS = {
    "calcofi": calcofi.run,
    "cce_moorings": cce_moorings.run,
    "easyoneargo": easyoneargo.run,
    "ca_beach_water_quality": ca_beach_water_quality.run,
    "sd_beach_water_quality": sd_beach_water_quality.run,
    "noaa_tides": noaa_tides.run,
    "cdip": cdip.run,
    "nws_marine": nws_marine.run,
    "cdph_biotoxin": cdph_biotoxin.run,
    "nccos_hab": nccos_hab.run,
    "nasa_ocean_color": nasa_ocean_color.run,
}