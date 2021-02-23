import os
import pickle
import logging
import itertools
import numpy as np

from scipy import ndimage as nd

import celery

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.globals \
    import get_SPATIAL_DATA
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.utils \
    import get_tempfile, remove_file
from open_elevation.celery_tasks.save_geotiff \
    import save_geotiff
from open_elevation.celery_tasks.save_png \
    import save_png
from open_elevation.celery_tasks.save_hillshade \
    import save_pnghillshade
from open_elevation.celery_tasks.las_processing \
    import process_laz
from open_elevation.gdalinterface \
    import GDALInterface
from open_elevation.cassandra_path \
    import Cassandra_Path

import open_elevation.mesh as mesh


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


def check_all_data_available(*args, **kwargs):
    SPATIAL_DATA = get_SPATIAL_DATA()
    index = SPATIAL_DATA.subset(*args, **kwargs)

    tasks = []
    for x in index.iterate():
        if Cassandra_Path(x['file']).in_cassandra():
            continue

        if 'remote_meta' in x:
            tasks += [process_laz\
                      (url = x['url'],
                       ofn = x['file'],
                       resolution = x['pdal_resolution'],
                       what = x['stat'],
                       if_compute_las = x['if_compute_las'])]
            continue

        raise RuntimeError('%s file is not available'\
                           % x['file'])
    return celery.group(tasks)


def _compute_mesh(box, step, mesh_type):
    return mesh.mesh(box = box, step = step,
                     which = mesh_type)


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 60*10)
def sample_from_box(box, data_re, stat,
                    mesh_type = 'metric', step = 1):
    logging.debug("""
    sample_from_box
    box = %s
    data_re = %s
    stat = %s
    mesh_type = %s
    step = %s
    """ % (str(box), str(data_re), str(stat),
           str(mesh_type), str(step)))
    SPATIAL_DATA = get_SPATIAL_DATA()
    index = SPATIAL_DATA.subset(box = box,
                                data_re = data_re,
                                stat = stat)

    grid = _compute_mesh(box = box, step = step,
                         mesh_type = mesh_type)

    points = list(itertools.product(*grid['mesh']))

    res = None
    for fn in index.files():
        interface = GDALInterface(fn)
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

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump({'raster': res, 'mesh': grid}, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


def _check_box_not_too_big(box, step, mesh_type,
                           limit = 0.1, max_points = 2e+7):
    if abs(box[2] - box[0]) > limit \
       or abs(box[3] - box[1]) > limit:
        raise RuntimeError\
            ("step in box should not be larger than %.2f" % limit)

    grid = _compute_mesh(box = box, step = step,
                         mesh_type = mesh_type)
    if len(grid['mesh'][0])*len(grid['mesh'][1]) \
       > max_points:
        raise RuntimeError\
            ("either box or resolution is too high!")


def sample_raster(box, data_re, stat,
                  mesh_type, step, output_type):
    if output_type not in ('geotiff', 'pickle',
                           'pnghillshade','png'):
        raise RuntimeError("Invalid 'output_type' argument!")

    _check_box_not_too_big(box = box, step = step,
                           mesh_type = mesh_type)

    tasks = celery.chain\
        (check_all_data_available(box = box,
                                  data_re = data_re,
                                  stat = stat),\
         sample_from_box.signature\
         (kwargs = {'box': box,
                    'data_re': data_re,
                    'stat': stat,
                    'mesh_type': mesh_type,
                    'step': step},
          immutable = True))

    if output_type in ('png'):
        tasks |= save_png.signature()
        return tasks

    if output_type in ('geotiff', 'pnghillshade'):
        tasks |= save_geotiff.signature()

    if output_type == 'pnghillshade':
        tasks |= save_pnghillshade.signature()

    return tasks
