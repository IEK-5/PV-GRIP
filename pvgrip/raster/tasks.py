import pickle
import logging
import itertools

import numpy as np

import pvgrip.raster.io as io

from pvgrip.raster.utils \
    import fill_missing, index2fn
from pvgrip.raster.mesh \
    import mesh
from pvgrip.raster.gdalinterface \
    import GDALInterface

from pvgrip.storage.remotestorage_path \
    import searchandget_locally

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

from pvgrip.utils.files \
    import get_tempfile, remove_file
from pvgrip.utils.format_dictionary \
    import format_dictionary


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='raster')
@one_instance(expire = 10)
def save_geotiff(self, pickle_fn):
    logging.debug("save_geotiff\n{}"\
                  .format(format_dictionary(locals())))
    with open(pickle_fn, 'rb') as f:
        data = pickle.load(f)
    ofn = get_tempfile()
    try:
        io.save_geotiff(data, ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='raster')
@one_instance(expire=10)
def save_png(self, pickle_fn, normalize = False):
    logging.debug("save_png\n{}"\
                  .format(format_dictionary(locals())))
    with open(pickle_fn, 'rb') as f:
        data = pickle.load(f)

    ofn = get_tempfile()
    try:
        io.save_png(data = data, ofn = ofn,
                    normalize = normalize)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='raster')
@one_instance(expire = 10)
def save_pnghillshade(self, geotiff_fn):
    logging.debug("save_pnghillshade\n{}"\
                  .format(format_dictionary(locals())))
    ofn = get_tempfile()
    try:
        io.save_pnghillshade(geotiff_fn, ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='raster')
@one_instance(expire = 30)
def save_pickle(self, geotiff_fn):
    logging.debug("save_pickle\n{}"\
                  .format(format_dictionary(locals())))
    ofn = get_tempfile()
    try:
        io.save_pickle(geotiff_fn, ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='raster')
@one_instance(expire = 60*10)
def sample_from_box(self, box, data_re, stat,
                    mesh_type = 'metric', step = 1,
                    pdal_resolution = 0.3, ensure_las = False):
    logging.debug("sample_from_box\n{}"\
                  .format(format_dictionary(locals())))
    SPATIAL_DATA = get_SPATIAL_DATA()
    index = SPATIAL_DATA.subset(box = box, data_re = data_re)

    grid = mesh(box = box, step = step, which = mesh_type)
    points = list(itertools.product(*grid['mesh']))

    res = None
    for fn_idx in index.iterate():
        fn = index2fn(fn_idx, stat = stat,
                      pdal_resolution = pdal_resolution,
                      ensure_las = ensure_las)
        fn = searchandget_locally(fn)
        interface = GDALInterface(fn)
        x = np.array(interface.lookup(points = points,
                                      box = box))

        if res is None:
            res = x
            continue

        if x.shape != res.shape:
            raise RuntimeError\
                ("cannot join data sources of different shape")

        res = np.array((res,x)).max(axis=0)
    res = np.array(res).reshape(len(grid['mesh'][0]),
                                len(grid['mesh'][1]),
                                res.shape[1])
    res = fill_missing(res)
    res = np.transpose(np.flip(res, axis = 1),
                       axes=(1,0,2))

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump({'raster': res, 'mesh': grid}, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn
