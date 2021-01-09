import os
import time
import pickle
import logging
import tempfile
import itertools
import numpy as np

from celery_once import QueueOnce, AlreadyQueued

import open_elevation.celery_tasks.app as app
import open_elevation.utils as utils
import open_elevation.mesh as mesh
import open_elevation.gdal_interfaces as gdal


def _query_coordinate(lon, lat, gdal_data):
    nearest = list(gdal_data._index.nearest((lon,lat)))
    data = gdal.choose_highest_resolution(nearest)

    interface = None
    while not interface:
        try:
            interface = gdal_data.open_gdal_interface\
                (data['file'])
        except utils.TASK_RUNNING:
            time.sleep(5)
            continue
    return float(interface.lookup(lat, lon))


def _remove_file(fn):
    try:
        os.remove(fn)
    except:
        logging.error("cannot remove file: %s" % fn)
        pass


def _get_args(box, data_re, mesh_type, step):
    return ("_sample_from_box", tuple(box), data_re, mesh_type, step)


@app.CELERY_APP.task(base=QueueOnce,
                     once = {'keys': ['box', 'data_re',
                                      'mesh_type','step']})
def _sample_from_box(index_fn, box, data_re,
                     mesh_type = 'metric', step = 1,
                     max_points = 2e+7):
    gdal_data = gdal.GDALTileInterface(tiles_folder = None,
                                       index_file = index_fn,
                                       use_only_index = True)
    _remove_file(index_fn)

    grid = mesh.mesh(box = box, step = step,
                     which = mesh_type)

    if len(grid['mesh'][0])*len(grid['mesh'][1]) \
       > max_points:
        raise RuntimeError\
            ("either box or resolution is too high!")

    res = []
    for lon, lat in itertools.product(*grid['mesh']):
        try:
            res += [_query_coordinate(lon = lon, lat = lat,
                                      gdal_data = gdal_data)]
        except Exception as e:
            logging.error("error: %s" % str(e))
            res += [-9999]
            continue
    res = np.array(res).reshape(len(grid['mesh'][0]),
                                len(grid['mesh'][1]))
    res = np.transpose(res)[::-1,]

    args_key = _get_args(box = box, data_re = data_re,
                         mesh_type = mesh_type, step = step)
    fn = app.RESULTS_CACHE.add(args_key)
    with open(fn, 'wb') as f:
        pickle.dump({'raster': res, 'mesh': grid}, f)
    return fn


def start_sample_from_box(gdal, box, data_re, mesh_type, step):
    args_key = _get_args(box = box, data_re = data_re,
                         mesh_type = mesh_type, step = step)
    if args_key in app.RESULTS_CACHE:
        return app.RESULTS_CACHE.get(args_key)

    index_fn = gdal.subset(box = box, data_re = data_re)
    try:
        _sample_from_box.delay\
            (index_fn = index_fn,
             box = box, data_re = data_re,
             mesh_type = mesh_type, step = step)
    except AlreadyQueued:
        _remove_file(index_fn)
    raise utils.TASK_RUNNING()
