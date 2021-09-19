import celery
import numpy as np
import pandas as pd

from pvgrip.globals \
    import COPERNICUS_HASH_LENGTH

from pvgrip.storage.remotestorage_path \
    import searchif_instorage

from pvgrip.utils.times import \
    time_range2list

from pvgrip.weather.tasks \
    import retrieve_source, \
    sample_irradiance_bbox, \
    sample_irradiance_route

from pvgrip.weather.utils \
    import timelocation_add_datetimes, \
    timelocation_add_hash, \
    timelocation_add_region, \
    bbox2hash

from pvgrip.weather.copernicus \
    import cams_solar_radiation_timeseries


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
    tl = timelocation_add_region(tl, 'coordinate')

    # source fn is unique for a day and a region
    is_unique = -tl[['date','region_hash']].duplicated()
    calls = tl[is_unique].apply\
        (lambda x: \
         cams_solar_radiation_timeseries\
         (date = x['datetime'],
          location = x[['region_latitude',
                        'region_longitude',
                        'region_hash']]),
         axis = 1).to_list()

    return _get_sources_tasks(calls)


def irradiance_bbox(box, time_range, time_step, what):
    tl = bbox2hash(box, COPERNICUS_HASH_LENGTH)
    times = time_range2list(time_range = time_range,
                            time_step = '1day',
                            time_format = '%Y-%m-%d_%H:%M:%S')
    times = pd.DataFrame(times, columns = ['timestr'])
    tl = tl.merge(times, how='cross')
    tl = timelocation_add_datetimes(tl)

    jobs = _irradiance_source_jobs(tl)
    jobs |= sample_irradiance_bbox.signature\
        (kwargs = {'bbox': box,
                   'time_range': time_range,
                   'time_step': time_step,
                   'what': what},
         immutable = True)

    return jobs


def irradiance_route(tsvfn_uploaded, what):
    tl = pd.read_csv(tsvfn_uploaded, sep=None, engine='python')

    if 'timestr' not in tl or \
       'longitude' not in tl or \
       'latitude' not in tl:
        raise RuntimeError\
            ("longitude, latitude or timestr are missing!")

    tl = timelocation_add_datetimes(tl)
    tl = timelocation_add_hash(tl, COPERNICUS_HASH_LENGTH)
    jobs = _irradiance_source_jobs(tl)
    jobs |= sample_irradiance_route.signature\
        (kwargs = {'route_fn': tsvfn_uploaded,
                   'what': what},
         immutable = True)

    return jobs
