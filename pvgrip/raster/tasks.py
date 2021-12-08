import re
import os
import pickle
import shutil
import logging
import itertools

import numpy as np
import pandas as pd

import pvgrip.raster.io as io

from pvgrip.raster.utils \
    import fill_missing, index2fn, \
    route_neighbours
from pvgrip.raster.mesh \
    import mesh, mesh2box
from pvgrip.raster.gdalinterface \
    import GDALInterface
from pvgrip.raster.pickle_lookup \
    import pickle_lookup

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
    import get_tempfile, remove_file, get_tempdir
from pvgrip.utils.format_dictionary \
    import format_dictionary

from pvgrip.utils.timeout \
    import Timeout


def _read_pickle(fn):
    with open(fn, 'rb') as f:
        return pickle.load(f)


def _process_lookup(arr, grid):
    arr = np.array(arr).reshape(len(grid['mesh'][0]),
                                len(grid['mesh'][1]),
                                arr.shape[1])
    arr = fill_missing(arr)
    arr = np.transpose(np.flip(arr, axis = 1),
                       axes=(1,0,2))

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump({'raster': arr, 'mesh': grid}, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='raster')
@one_instance(expire = 10)
def save_geotiff(self, pickle_fn):
    logging.debug("save_geotiff\n{}"\
                  .format(format_dictionary(locals())))
    data = _read_pickle(pickle_fn)
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
    data = _read_pickle(pickle_fn)

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
    with Timeout(600):
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
                ("""cannot join data sources of different shape!
                data_re matches data sources with shapes:
                {} and {}""".format(x.shape, res.shape))

        # take maximum among multiple data sources!
        res = np.array((res,x)).max(axis=0)

    return _process_lookup(arr = res, grid = grid)


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='raster')
@one_instance(expire = 60*10)
def resample_from_pickle(self, pickle_fn, new_step):
    logging.debug("resample_from_pickle\n{}"\
                  .format(format_dictionary(locals())))
    src = _read_pickle(pickle_fn)

    if src['mesh']['step'] == new_step:
        return pickle_fn

    box, mesh_type = mesh2box(src['mesh'])
    grid = mesh(box = box, step = new_step, which = mesh_type)
    points = list(itertools.product(*grid['mesh']))
    return _process_lookup(arr = pickle_lookup(src,points,box),
                           grid = grid)


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='raster')
@one_instance(expire = 10)
def sample_route_neighbour(self, pickle_fn, route_fn,
                           azimuth_default, neighbour_step,
                           prefix):
    logging.debug("sample_route_neighbour\n{}"\
                  .format(format_dictionary(locals())))
    with open(route_fn, 'rb') as f:
        route = pickle.load(f)

    src = _read_pickle(pickle_fn)

    box, mesh_type = mesh2box(src['mesh'])
    points, names = route_neighbours\
        (route, azimuth_default, neighbour_step)

    reg = re.compile(r'[\[,\]\s\*]')
    names = [reg.sub('.','{}_{}'.format(prefix, x)) for x in names]

    res = pickle_lookup(src, points, box)
    res = pd.DataFrame\
        (np.transpose(np.array(res).reshape(9,len(route))),
         columns=names)
    res = pd.concat\
        ([pd.DataFrame(route).reset_index(drop=True), res],
         axis=1)

    ofn = get_tempfile()
    try:
        res.to_csv(ofn, sep='\t', index=False)
        return ofn
    except Exception as e:
        remove_file(ofn)
        raise e
