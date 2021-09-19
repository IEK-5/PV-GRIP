import logging
import functools

import pandas as pd

from pvgrip.globals \
    import COPERNICUS_HASH_LENGTH, \
    COPERNICUS_CDS, COPERNICUS_ADS

from pvgrip \
    import CELERY_APP
from pvgrip.globals \
    import get_SPATIAL_DATA
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance

from pvgrip.utils.files \
    import get_tempfile, remove_file, get_tempdir
from pvgrip.utils.format_dictionary \
    import format_dictionary

from pvgrip.utils.times \
    import time_range2list

from pvgrip.weather.copernicus \
    import retrieve, \
    cams_solar_radiation_timeseries, \
    sample_irradiance

from pvgrip.weather.utils \
    import timelocation_add_datetimes, \
    timelocation_add_hash, \
    timelocation_add_region, \
    bbox2hash


@CELERY_APP.task()
@cache_fn_results(ofn_arg = 'ofn')
@one_instance(expire = 600)
def retrieve_source(credentials_type, what, args, ofn):
    logging.debug("retrieve_source\n{}"\
                  .format(format_dictionary(locals())))
    if 'cds' == credentials_type:
        credentials = COPERNICUS_CDS
    elif 'ads' == credentials_type:
        credentials = COPERNICUS_ADS
    else:
        raise RuntimeError("unknown credentials_type = {}"\
                           .format(credentials_type))

    retrieve(credentials = credentials,
             what = what,
             args = args,
             ofn = ofn)

    return ofn


def _sample_irradiance(tl, what):
    res = []
    for sfn, piece in tl.groupby(tl['source_fn']):
        piece[list(what)] = sample_irradiance\
            (piece, sfn, what = what)
        res += [piece]
    res = functools.reduce(lambda x,y: x.append(y),res)

    # drop duplicated data columns
    res = res.drop(['datetime','date','source_fn'],
                   axis = 1)

    ofn = get_tempfile()
    try:
        res.to_csv(ofn, sep='\t', index=False)
        return ofn
    except Exception as e:
        remove_file(ofn)
        raise e


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 600)
def sample_irradiance_route(route_fn, what):
    logging.debug("sample_irradiance_route\n{}"\
                  .format(format_dictionary(locals())))

    tl = pd.read_csv(route_fn, sep=None, engine='python')
    tl = timelocation_add_datetimes(tl)
    tl = timelocation_add_hash(tl, COPERNICUS_HASH_LENGTH)
    tl = timelocation_add_region(tl, 'coordinate')
    tl['source_fn'] = tl.apply\
        (lambda x: \
         cams_solar_radiation_timeseries\
         (date = x['datetime'],
          location = x[['region_latitude',
                        'region_longitude',
                        'region_hash']])['ofn'],
         axis = 1)

    return _sample_irradiance(tl, what)


@CELERY_APP.task()
@cache_fn_results(minage = 1632258318)
@one_instance(expire = 600)
def sample_irradiance_bbox(bbox, time_range, time_step, what):
    logging.debug("sample_irradiance_bbox\n{}"\
                  .format(format_dictionary(locals())))

    tl = bbox2hash(bbox, COPERNICUS_HASH_LENGTH)
    times = time_range2list(time_range = time_range,
                            time_step = time_step,
                            time_format = '%Y-%m-%d_%H:%M:%S')
    times = pd.DataFrame(times, columns = ['timestr'])
    tl = tl.merge(times, how='cross')
    tl = timelocation_add_datetimes(tl)
    tl = timelocation_add_region(tl, 'coordinate')
    tl['source_fn'] = tl.apply\
        (lambda x: \
         cams_solar_radiation_timeseries\
         (date = x['datetime'],
          location = x[['region_latitude',
                        'region_longitude',
                        'region_hash']])['ofn'],
         axis = 1)

    return _sample_irradiance(tl, what)
