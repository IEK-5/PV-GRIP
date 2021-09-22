import celery
import numpy as np
import pandas as pd

from datetime import datetime

from pvgrip.globals \
    import COPERNICUS_ADS_HASH_LENGTH, \
    COPERNICUS_CDS_HASH_LENGTH

from pvgrip.storage.remotestorage_path \
    import searchif_instorage

from pvgrip.weather.tasks \
    import retrieve_source, \
    sample_irradiance_bbox, \
    sample_irradiance_route, \
    sample_reanalysis_bbox, \
    sample_reanalysis_route

from pvgrip.weather.utils \
    import bbox_tl, route_tl

from pvgrip.weather.copernicus \
    import cams_solar_radiation_timeseries, \
    reanalysis_era5_land


def _get_sources_tasks(calls):
    res = []
    for call in calls:
        if searchif_instorage(call['ofn']):
            continue

        res += [retrieve_source.signature\
                (kwargs = call,
                 immutable = True)]

    return celery.group(res)


def _irradiance_source_jobs(tl):
    # source fn is unique for year-week-location
    is_unique = -tl[['year','week','region_hash']].duplicated()
    calls = tl[is_unique].apply\
        (lambda x: \
         cams_solar_radiation_timeseries\
         (location = x), axis = 1).to_list()

    return _get_sources_tasks(calls)


def _reanalysis_source_jobs(tl):
    # source fn is unique for a day and a region
    is_unique = -tl[['date','region_hash']].duplicated()
    calls = tl[is_unique].apply\
        (lambda x: \
         reanalysis_era5_land\
         (location = x), axis = 1).to_list()

    return _get_sources_tasks(calls)


def irradiance_bbox(box, time_range, time_step, what):
    tl = bbox_tl(box = box,
                 time_range = time_range,
                 time_step = '1day',
                 hash_length = COPERNICUS_CDS_HASH_LENGTH,
                 region_type = 'coordinate')

    jobs = _irradiance_source_jobs(tl)
    jobs |= sample_irradiance_bbox.signature\
        (kwargs = {'bbox': box,
                   'time_range': time_range,
                   'time_step': time_step,
                   'what': what},
         immutable = True)

    return jobs


def irradiance_route(tsvfn_uploaded, what):
    tl = route_tl(route_fn = tsvfn_uploaded,
                  hash_length = COPERNICUS_CDS_HASH_LENGTH,
                  region_type = 'coordinate')

    jobs = _irradiance_source_jobs(tl)
    jobs |= sample_irradiance_route.signature\
        (kwargs = {'route_fn': tsvfn_uploaded,
                   'what': what},
         immutable = True)

    return jobs


def reanalysis_route(tsvfn_uploaded, what):
    tl = route_tl(route_fn = tsvfn_uploaded,
                  hash_length = COPERNICUS_ADS_HASH_LENGTH,
                  region_type = 'bbox')

    jobs = _reanalysis_source_jobs(tl)
    jobs |= sample_reanalysis_route.signature\
        (kwargs = {'route_fn': tsvfn_uploaded,
                   'what': what},
         immutable = True)

    return jobs


def reanalysis_bbox(box, time_range, time_step, what):
    tl = bbox_tl(box = box,
                 time_range = time_range,
                 time_step = '1day',
                 hash_length = COPERNICUS_ADS_HASH_LENGTH,
                 region_type = 'bbox')

    jobs = _reanalysis_source_jobs(tl)
    jobs |= sample_reanalysis_bbox.signature\
        (kwargs = {'bbox': box,
                   'time_range': time_range,
                   'time_step': time_step,
                   'what': what},
         immutable = True)

    return jobs
