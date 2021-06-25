import pickle
import logging
import itertools

import numpy as np

import pvgrip.raster.io as io

from pvgrip.raster.utils \
    import fill_missing
from pvgrip.raster.mesh \
    import mesh
from pvgrip.raster.gdalinterface \
    import GDALInterface

from pvgrip.storage.cassandra_path \
    import Cassandra_Path

from pvgrip \
    import CELERY_APP
from pvgrip.globals \
    import get_SPATIAL_DATA
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance

from pvgrip.utils.files \
    import get_tempfile, remove_file
from pvgrip.utils.format_dictionary \
    import format_dictionary


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def save_geotiff(pickle_fn):
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


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire=10)
def save_png(pickle_fn, normalize = False):
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


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def save_binary_png(tif_fn):
    logging.debug("save_binary_png\n{}"\
                  .format(format_dictionary(locals())))
    ofn = get_tempfile()
    try:
        io.save_binary_png_from_tif(ifn = tif_fn, ofn = ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def save_pnghillshade(geotiff_fn):
    ofn = get_tempfile()
    try:
        io.save_pnghillshade(geotiff_fn, ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 60*10)
def sample_from_box(box, data_re, stat,
                    mesh_type = 'metric', step = 1):
    logging.debug("sample_from_box\n{}"\
                  .format(format_dictionary(locals())))
    SPATIAL_DATA = get_SPATIAL_DATA()
    index = SPATIAL_DATA.subset(box = box,
                                data_re = data_re,
                                stat = stat)

    grid = mesh(box = box, step = step,
                which = mesh_type)

    points = list(itertools.product(*grid['mesh']))

    res = None
    for fn in index.files():
        fn = Cassandra_Path(fn).get_locally()
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
