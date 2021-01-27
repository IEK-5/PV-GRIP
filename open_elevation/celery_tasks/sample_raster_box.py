import os
import time
import pickle
import logging
import tempfile
import itertools
import numpy as np

from scipy import ndimage as nd

import celery

import open_elevation.celery_tasks.app \
    as app
import open_elevation.celery_tasks.save_geotiff \
    as save_geotiff
import open_elevation.celery_tasks.save_png \
    as save_png
import open_elevation.celery_tasks.save_hillshade \
    as save_hillshade
import open_elevation.celery_tasks.las_processing \
    as las_processing
import open_elevation.utils \
    as utils
import open_elevation.mesh \
    as mesh
import open_elevation.gdal_interfaces \
    as gdal
import open_elevation.polygon_index \
    as polygon_index


def fill_missing(data, missing_value = -9999):
    """Replace the value of missing 'data' cells (indicated by
    'missing_value') by the value of the nearest cell

    Taken from: https://stackoverflow.com/a/27745627

    """
    missing = data == missing_value
    ind = nd.distance_transform_edt(missing,
                                    return_distances=False,
                                    return_indices=True)
    return data[tuple(ind)]


def check_all_data_available(index_fn):
    index = polygon_index.Polygon_File_Index()
    index.load(index_fn)

    tasks = []
    for x in index.iterate():
        if os.path.exists(x['file']):
            continue

        if 'remote_meta' in x:
            tasks += [las_processing.process_laz\
                      (url = x['url'],
                       ofn = x['file'],
                       resolution = x['pdal_resolution'],
                       what = x['stat'],
                       if_compute_las = x['if_compute_las'])]
    return celery.group(tasks)


def _compute_mesh(box, step, mesh_type):
    return mesh.mesh(box = box, step = step,
                     which = mesh_type)


@app.CELERY_APP.task()
@app.cache_fn_results(keys = ['index_fn', 'box',
                              'mesh_type','step'])
@app.one_instance(expire = 60*10)
def sample_from_box(index_fn, box,
                    mesh_type = 'metric', step = 1):
    logging.debug("""
    sample_from_box
    index_fn = %s
    box = %s
    mesh_type = %s
    step = %s
    """ % (index_fn, str(box),
           str(mesh_type), str(step)))
    index = polygon_index.Polygon_File_Index()
    index.load(index_fn)

    grid = _compute_mesh(box = box, step = step,
                         mesh_type = mesh_type)

    points = list(itertools.product(*grid['mesh']))

    res = None
    for fn in index.files():
        interface = gdal.GDALInterface(fn)
        x = np.array(interface.lookup(points))

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

    ofn = utils.get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump({'raster': res, 'mesh': grid}, f)
    except Exception as e:
        utils.remove_file(ofn)
        raise e
    return ofn


def sample_raster(gdal, box, data_re, stat,
                  mesh_type, step, output_type,
                  max_points = 2e+7):
    if output_type not in ('geotiff', 'pickle',
                           'pnghillshade','png'):
        raise RuntimeError("Invalid 'output_type' argument!")

    grid = _compute_mesh(box = box, step = step,
                         mesh_type = mesh_type)

    if len(grid['mesh'][0])*len(grid['mesh'][1]) \
       > max_points:
        raise RuntimeError\
            ("either box or resolution is too high!")

    index_fn = gdal.subset(box = box,
                           data_re = data_re,
                           stat = stat)
    tasks = celery.chain\
        (check_all_data_available(index_fn),\
         sample_from_box.signature\
         (kwargs = {'index_fn': index_fn, 'box': box,
                    'mesh_type': mesh_type, 'step': step},
          immutable = True))

    if output_type in ('png'):
        tasks |= save_png.save_png.signature()
        return tasks

    if output_type in ('geotiff', 'pnghillshade'):
        tasks |= save_geotiff.save_geotiff.signature()

    if output_type == 'pnghillshade':
        tasks |= save_hillshade.save_pnghillshade.signature()

    return tasks
