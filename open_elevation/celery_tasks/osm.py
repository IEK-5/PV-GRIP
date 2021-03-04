import os
import shutil
import geohash
import requests

import xml.etree.ElementTree as etree

import celery

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.celery_tasks.sample_raster_box \
    import check_box_not_too_big
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.globals \
    import PVGRIP_CONFIGS

from open_elevation.utils \
    import get_tempfile, remove_file, \
    run_command, get_tempdir

from cassandra_io.utils \
    import bbox2hash

import open_elevation.mesh as mesh


def _form_query(bbox, tag):
    bbox = tuple(bbox)
    query_tags = ""
    if tag:
        query_tags = query_tags + \
            f"""node{str(bbox)};
            way[{str(tag)}]{str(bbox)};
            relation[{str(tag)}]{str(bbox)};"""
    else:
        query_tags = f"""node{str(bbox)};
                         way{str(bbox)};
                         relation{str(bbox)};"""
    return f"""
    [out:xml];
    (
    {query_tags}

    );
    out center;
    """


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def find_osm_data_online(bbox, tag):
    query = _form_query(bbox, tag)

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


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def create_rules(tag):
    root = etree.Element('osm')

    tree = etree.ElementTree(root)

    type_ = etree.Element('way')

    root.append(type_)

    tag_name = etree.Element('tag')
    type_.append(tag_name)

    tag_name.set('k',tag)
    tag_name.set('v','')

    tag_action = etree.Element('tag')
    type_.append(tag_action)

    tag_action.set('k','_action_')
    tag_action.set('v','draw:color=black;bcolor=black')

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            tree.write(f)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def render_osm_data(osm_fn, rules_fn, box, width):
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
              '-d','127'],
             cwd = wdir)
        os.rename(os.path.join(wdir,'output.png'), ofn)
    finally:
        shutil.rmtree(wdir)

    return ofn


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def merge_osm(osm_files):
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


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def readpng_asarray(png_fn, box, step, mesh_type):
    grid = mesh.mesh(box = box, step = step,
                     which = mesh_type)

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump({'raster': cv2.imread(png_fn, 0),
                         'mesh': grid}, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


def _get_box_list(box):
    hash_length = PVGRIP_CONFIGS['osm']['hash_length']
    f = (geohash.bbox(i) \
         for i in bbox2hash(box, hash_length))
    return [(x['s'],x['w'],x['n'],x['e']) for x in f]


def osm_render(box, step, mesh_type, tag = 'building'):
    width, _ = check_box_not_too_big\
        (box = box, step = step,
         mesh_type = mesh_type)

    box_list = _get_box_list(box = box)

    tasks = celery.group\
        (*[find_osm_data_online.signature\
           (kwargs={'tag': tag, 'bbox':x}) \
           for x in box_list])

    tasks |= celery.group\
        (*[merge_osm.signature(),
           create_rules.si(tag)])

    tasks |= render_osm_data.signature\
        (kwargs={'box': box, 'width': width})

    tasks |= readpng_asarray.signature\
        (kwargs={'box': box, 'step': step,
                 'mesh_type': mesh_type})

    return tasks
