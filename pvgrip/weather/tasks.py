import logging
import geohash
import functools

from pvgrip.globals \
    import COPERNICUS_CDS, COPERNICUS_ADS, \
    COPERNICUS_CDS_HASH_LENGTH, \
    COPERNICUS_ADS_HASH_LENGTH

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

from pvgrip.weather.copernicus \
    import retrieve, \
    cams_solar_radiation_timeseries, \
    reanalysis_era5_land, \
    sample_irradiance, \
    sample_reanalysis

from pvgrip.weather.utils \
    import bbox_tl, route_tl


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


def _save_tsv(df):
    ofn = get_tempfile()
    try:
        df.to_csv(ofn, sep='\t', index=False)
        return ofn
    except Exception as e:
        remove_file(ofn)
        raise e


def _sample(tl, what, how, drop):
    res = []
    for sfn, piece in tl.groupby(tl['source_fn']):
        piece[list(what)] = how(piece, sfn, what = what)
        res += [piece]
    res = functools.reduce(lambda x,y: x.append(y),res)

    # drop duplicated data columns
    res = res.drop(drop, axis = 1)

    return _save_tsv(res)


@CELERY_APP.task()
@cache_fn_results(minage = 1632547215)
@one_instance(expire = 600)
def sample_irradiance_route(route_fn, what):
    logging.debug("sample_irradiance_route\n{}"\
                  .format(format_dictionary(locals())))

    tl = route_tl(route_fn = route_fn,
                  hash_length = COPERNICUS_CDS_HASH_LENGTH,
                  region_type = 'coordinate')
    tl['source_fn'] = tl.apply\
        (lambda x: \
         cams_solar_radiation_timeseries(location = x)['ofn'],
         axis = 1)

    return _sample(tl, what = what, how = sample_irradiance,
                   drop = ['datetime','date','year','week','source_fn'])


@CELERY_APP.task()
@cache_fn_results(minage = 1632547215)
@one_instance(expire = 600)
def sample_irradiance_bbox(bbox, time_range, time_step, what):
    logging.debug("sample_irradiance_bbox\n{}"\
                  .format(format_dictionary(locals())))

    tl = bbox_tl(box = bbox,
                 time_range = time_range,
                 time_step = time_step,
                 hash_length = COPERNICUS_CDS_HASH_LENGTH,
                 region_type = 'coordinate')

    tl['source_fn'] = tl.apply\
        (lambda x: \
         cams_solar_radiation_timeseries(location = x)['ofn'],
         axis = 1)

    return _sample(tl, what = what, how = sample_irradiance,
                   drop = ['datetime','date','year','week','source_fn'])


@CELERY_APP.task()
@cache_fn_results(minage = 1632547215)
@one_instance(expire = 600)
def sample_reanalysis_route(route_fn, what):
    logging.debug("sample_reanalysis_route\n{}"\
                  .format(format_dictionary(locals())))

    tl = route_tl(route_fn = route_fn,
                  hash_length = COPERNICUS_ADS_HASH_LENGTH,
                  region_type = 'bbox')
    tl['source_fn'] = tl.apply\
        (lambda x: \
         reanalysis_era5_land(location = x)['ofn'],
         axis = 1)

    return _sample(tl, what = what, how = sample_reanalysis,
                   drop = ['datetime','date','year','week','source_fn',
                           'dtimes','dlats','dlons','region_bbox'])


@CELERY_APP.task()
@cache_fn_results(minage = 1632547215)
@one_instance(expire = 600)
def sample_reanalysis_bbox(bbox, time_range, time_step, what):
    logging.debug("sample_reanalsysis_bbox\n{}"\
                  .format(format_dictionary(locals())))

    tl = bbox_tl(box = bbox,
                 time_range = time_range,
                 time_step = time_step,
                 hash_length = COPERNICUS_ADS_HASH_LENGTH,
                 sample_hash_length = COPERNICUS_CDS_HASH_LENGTH,
                 region_type = 'bbox')
    tl['latitude'], tl['longitude'] = \
        zip(*tl['sample_hash']\
            .map(lambda x: \
                 geohash.decode_exactly(x)[:2]))
    tl['source_fn'] = tl.apply\
        (lambda x: \
         reanalysis_era5_land(location = x)['ofn'],
         axis = 1)

    return _sample(tl, what = what, how = sample_reanalysis,
                   drop = ['datetime','date','year','week','source_fn',
                           'dtimes','dlats','dlons','region_bbox'])
