import os
import sys
import json
import shutil
import logging
import requests
import tempfile

import open_elevation.celery_tasks.app as app
import open_elevation.utils as utils


@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 60*5)
def download_laz(url):
    logging.debug("""
    download_laz
    url = %s
    """ % url)
    r = requests.get(url, allow_redirects=True)

    if 200 != r.status_code:
        raise RuntimeError("""
        cannot download data!
        url: %s
        """ % url)

    ofn = utils.get_tempfile()
    with open(ofn, 'wb') as f:
        f.write(r.content)
    return ofn


@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def write_pdaljson(laz_fn, ofn, resolution, what):
    logging.debug("""
    write_pdaljson
    laz_fn = %s
    ofn = %s
    resolution = %s
    what = %s
    """ % (laz_fn, ofn, str(resolution), str(what)))
    data = {}
    data['pipeline'] = [{
        'type': 'readers.las',
        'filename': laz_fn}]
    data['pipeline'] += \
    [{'filename': ofn,
      'gdaldriver': 'GTiff',
      'output_type': what,
      'resolution': resolution,
      'type': 'writers.gdal'}]

    ofn = utils.get_tempfile()
    with open(ofn, 'w') as f:
        json.dump(data, f)
    return ofn


@app.CELERY_APP.task()
@app.one_instance(expire = 60*20)
def run_pdal(path, ofn):
    logging.debug("""
    run_pdal
    path = %s
    ofn = %s
    """ % (path, ofn))
    if app.RESULTS_CACHE.file_in(ofn):
        logging.debug("File is in cache!")
        return ofn

    wdir = utils.get_tempdir()
    try:
        utils.run_command\
            (what = ['pdal','pipeline',path],
             cwd = wdir)

        app.RESULTS_CACHE.add_file(ofn)
        return ofn
    finally:
        shutil.rmtree(wdir)


@app.CELERY_APP.task()
@app.one_instance(expire = 5)
def link_ofn(ifn, ofn):
    logging.debug("""
    link_ofn
    ifn = %s
    ofn = %s
    """ % (ifn, ofn))
    if app.RESULTS_CACHE.file_in(ofn):
        logging.debug("File is in cache!")
        return ofn

    os.link(ifn, ofn)
    app.RESULTS_CACHE.add_file(ofn)
    return ofn


def process_laz(url, ofn, resolution, what, if_compute_las):
    tasks = download_laz\
        .signature(kwargs = {'url': url})

    if if_compute_las:
        tasks |= write_pdaljson\
            .signature(kwargs = {'ofn': ofn,
                                 'resolution': resolution,
                                 'what': what})
        tasks |= run_pdal\
            .signature(kwargs = {'ofn': ofn})
    else:
        tasks |= link_ofn\
            .signature(kwargs = {'ofn': ofn})

    return tasks
