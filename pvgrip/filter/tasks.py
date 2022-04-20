import logging
import pickle

import numpy as np

from pvgrip \
        import CELERY_APP
from pvgrip.utils.cache_fn_results \
        import cache_fn_results
from pvgrip.utils.celery_one_instance \
        import one_instance
from pvgrip.utils.basetask \
        import WithRetry

from pvgrip.utils.format_dictionary \
    import format_dictionary

from pvgrip.utils.files \
    import get_tempfile, remove_file

from pvgrip.filter.variance \
    import variance
from pvgrip.filter.filters \
    import const_weights, average_per_sqm, convolve


def _read_pickle(fn):
    with open(fn, 'rb') as f:
        return pickle.load(f)


def _write_pickle(raster, mesh):
    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump({'raster': raster,
                         'mesh': mesh}, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix = 'filter', minage = 1650884152)
@one_instance(expire = 10)
def stdev(self, fns, filter_size):
    logging.debug("stdev\n{}"\
                  .format(format_dictionary(locals())))
    stdev = _read_pickle(fns[0])
    mean = _read_pickle(fns[1])
    count = _read_pickle(fns[2])

    res = variance(stdev = stdev['raster'],
                   mean = mean['raster'],
                   count = count['raster'],
                   step = stdev['mesh']['step'],
                   filter_size = filter_size)
    res = np.power(res,0.5)
    res[np.isnan(res)] = 0

    return _write_pickle(raster = res, mesh = stdev['mesh'])


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix = 'filter', minage = 1650884152)
@one_instance(expire = 10)
def apply_filter(self, fn, filter_type, filter_size):
    logging.debug("apply_filter\n{}"\
                  .format(format_dictionary(locals())))
    raster = _read_pickle(fn)

    if 'average' == filter_type:
        weights = average_per_sqm\
            (filter_size = filter_size,
             step = raster['mesh']['step'])
    elif 'sum' == filter_type:
        weights = const_weights\
            (filter_size = filter_size,
             step = raster['mesh']['step'])
    else:
        raise RuntimeError("unknown filter_type = {}"\
                           .format(filter_type))

    res = convolve(raster = raster['raster'], weights = weights)
    return _write_pickle(raster = res, mesh = raster['mesh'])
