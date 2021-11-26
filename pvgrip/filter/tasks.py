import logging
import pickle

import numpy as np

from pvgrip \
        import CELERY_APP
from pvgrip.globals \
        import get_SPATIAL_DATA
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


def _read_pickle(fn):
    with open(fn, 'rb') as f:
        return pickle.load(f)


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='filter')
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

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump({'raster': res,
                         'mesh': stdev['mesh']}, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn
