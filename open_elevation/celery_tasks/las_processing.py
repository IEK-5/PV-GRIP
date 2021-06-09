import os
import sys
import json
import shutil
import logging
import requests
import tempfile

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.utils \
    import get_tempfile, run_command, \
    get_tempdir, format_dictionary


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 60*5)
def download_laz(url):
    logging.debug("download_laz\n{}"\
                  .format(format_dictionary(locals())))
    r = requests.get(url, allow_redirects=True)

    if 200 != r.status_code:
        raise RuntimeError("""
        cannot download data!
        url: %s
        """ % url)

    ofn = get_tempfile()
    with open(ofn, 'wb') as f:
        f.write(r.content)
    return ofn


def _write_pdaljson(laz_fn, ofn, resolution, what, json_ofn):
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

    with open(json_ofn, 'w') as f:
        json.dump(data, f)


@CELERY_APP.task()
@cache_fn_results(ofn_arg = 'ofn')
@one_instance(expire = 60*20)
def run_pdal(laz_fn, resolution, what, ofn):
    logging.debug("run_pdal\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    try:
        pdalfn = os.path.join(wdir,'run_pdal.json')
        _write_pdaljson(laz_fn = laz_fn, ofn = ofn,
                        resolution = resolution, what = what,
                        json_ofn = pdalfn)
        run_command\
            (what = ['pdal','pipeline',pdalfn],
             cwd = wdir)
        return ofn
    finally:
        shutil.rmtree(wdir)


@CELERY_APP.task()
@cache_fn_results(ofn_arg = 'ofn')
@one_instance(expire = 5)
def link_ofn(ifn, ofn):
    logging.debug("link_ofn\n{}"\
                  .format(format_dictionary(locals())))
    os.link(ifn, ofn)
    return ofn


def process_laz(url, ofn, resolution, what, if_compute_las):
    tasks = download_laz\
        .signature(kwargs = {'url': url})

    if if_compute_las:
        tasks |= run_pdal\
            .signature(kwargs = {'ofn': ofn,
                                 'resolution': resolution,
                                 'what': what})
    else:
        tasks |= link_ofn\
            .signature(kwargs = {'ofn': ofn})

    return tasks
