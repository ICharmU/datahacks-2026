from . import calcofi, cce_moorings, easyoneargo

RUNNERS = {
    "calcofi": calcofi.run,
    "cce_moorings": cce_moorings.run,
    "easyoneargo": easyoneargo.run,
}