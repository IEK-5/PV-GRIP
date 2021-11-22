import os
import cv2
import shutil
import pickle
import logging
import requests

import numpy as np

from pvgrip.osm.utils \
    import form_query

from pvgrip.raster.mesh \
    import mesh

from pvgrip \
    import CELERY_APP
from pvgrip.globals \
    import PVGRIP_CONFIGS
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance
from pvgrip.utils.basetask \
    import WithRetry

from pvgrip.utils.files \
    import get_tempfile, remove_file, get_tempdir
from pvgrip.utils.run_command \
    import run_command
from pvgrip.utils.format_dictionary \
    import format_dictionary


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='osm')
@one_instance(expire = 10)
def find_osm_data_online(self, bbox, tag):
    logging.debug("find_osm_data_online\n{}"\
                  .format(format_dictionary(locals())))
    query = form_query(bbox, tag)

    response = requests.get\
        (PVGRIP_CONFIGS['osm']['url'],
         params={'data':query},
         headers={'referer': PVGRIP_CONFIGS['osm']['referer']})

    ofn = get_tempfile()
    try:
        with open(ofn, 'w') as f:
            f.write(response.text)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='osm')
@one_instance(expire = 10)
def readpng_asarray(self, png_fn, box, step, mesh_type):
    logging.debug("readpng_asarray\n{}"\
                  .format(format_dictionary(locals())))
    grid = mesh(box = box, step = step,
                which = mesh_type)

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump({'raster': \
                         np.expand_dims(cv2.imread(png_fn, 0),
                                        axis=2),
                         'mesh': grid}, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='osm')
@one_instance(expire = 10)
def merge_osm(self, osm_files):
    logging.debug("merge_osm\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    ofn = get_tempfile()
    try:
        run_command\
            (what = ['osmconvert',
                     *osm_files,
                     '-o='+'output.osm'],
             cwd = wdir)
        os.rename(os.path.join(wdir,'output.osm'), ofn)
    finally:
        shutil.rmtree(wdir)

    return ofn


@CELERY_APP.task(bind=True, base=WithRetry)
@cache_fn_results(path_prefix='osm')
@one_instance(expire = 10)
def render_osm_data(self, osm_fn, rules_fn, box, width):
    logging.debug("render_osm_data\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    ofn = get_tempfile()
    # -P specifies dimensions in mm
    # -d specifies density (points in inch)
    try:
        run_command\
            (what = \
             ['smrender',
              '-i', osm_fn,
              '-o', 'output.png',
              f"{str(box[0])}:{str(box[1])}:{str(box[2])}:{str(box[3])}",
              '-r', rules_fn,
              '-P','%.1fx0' % (width/5),
              '-d','127',
              '-b','black'],
             cwd = wdir)
        os.rename(os.path.join(wdir,'output.png'), ofn)
    finally:
        shutil.rmtree(wdir)

    return ofn
