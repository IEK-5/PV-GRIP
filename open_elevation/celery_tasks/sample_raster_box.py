import os
import time
import pickle
import logging
import tempfile
import itertools
import numpy as np

from celery_once import QueueOnce, AlreadyQueued

import open_elevation.celery_tasks.app \
    as app
import open_elevation.celery_tasks.save_geotiff \
    as save_geotiff
import open_elevation.celery_tasks.save_hillshade \
    as save_hillshade
import open_elevation.utils \
    as utils
import open_elevation.mesh \
    as mesh
import open_elevation.gdal_interfaces \
    as gdal


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


@app.CELERY_APP.task(base=QueueOnce,
                     once = {'keys': ['box', 'data_re',
                                      'mesh_type','step'],
                             'timeout': 400})
def sample_from_box(index_fn, box, data_re,
                    mesh_type = 'metric', step = 1,
                    max_points = 2e+7):
    ofn = app.RESULTS_CACHE.get\
        (('sample_from_box', box, data_re,
          mesh_type, step), check = False)
    if app.RESULTS_CACHE.file_in(ofn):
        _remove_file(index_fn)
        return ofn

    if not index_fn:
        raise RuntimeError("Cached result vanished!")

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
            res += [-9999]
            continue
    res = np.array(res).reshape(len(grid['mesh'][0]),
                                len(grid['mesh'][1]))
    res = np.transpose(res)[::-1,]

    with open(ofn, 'wb') as f:
        pickle.dump({'raster': res, 'mesh': grid}, f)
    app.RESULTS_CACHE.add_file(ofn)
    return ofn


def sample_raster(gdal, box, data_re,
                  mesh_type, step, output_type):
    if output_type not in ('geotiff', 'pickle', 'pnghillshade'):
        raise RuntimeError("Invalid 'output_type' argument!")

    ofn_pickle = app.RESULTS_CACHE.get\
        (('sample_from_box', box, data_re,
          mesh_type, step), check = False)
    if app.RESULTS_CACHE.file_in(ofn_pickle):
        index_fn = None
    else:
        index_fn = gdal.subset(box = box, data_re = data_re)

    tasks = sample_from_box.signature\
        ((), {'index_fn': index_fn, 'box': box,
              'data_re': data_re, 'mesh_type': mesh_type,
              'step': step})

    if output_type in ('geotiff', 'pnghillshade'):
        tasks |= save_geotiff.save_geotiff.signature()

    if output_type == 'pnghillshade':
        tasks |= save_hillshade.save_pnghillshade.signature()

    return tasks
