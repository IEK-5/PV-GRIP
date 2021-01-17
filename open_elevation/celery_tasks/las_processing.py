import os
import sys
import json
import shutil
import requests
import tempfile
import subprocess

import open_elevation.celery_tasks.app as app
import open_elevation.utils as utils

def _touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)


def _write_pdaljson(path, resolution, whats):
    data = {}
    data['pipeline'] = ['src.laz']

    for what in whats:
        data['pipeline'] += \
        [{
            'filename': 'dtm_laz_%s.tif' % what,
            'gdaldriver': 'GTiff',
            'output_type': what,
            'resolution': resolution,
            'type': 'writers.gdal'
        }]

    with open(os.path.join(path, 'pdal.json'), 'w') as f:
        json.dump(data, f)


def _download_laz(url, path):
    r = requests.get(url, allow_redirects=True)

    if 200 != r.status_code:
        raise RuntimeError("""
        cannot download data!
        url: %s
        """ % url)

    with open(os.path.join(path, 'src.laz'), 'wb') as f:
        f.write(r.content)


def _run_pdal(path):
    subprocess.run(['pdal','pipeline','pdal.json'],
                   cwd = path)


@app.CELERY_APP.task()
@app.one_instance(expire = 60*10)
def task_las_processing(url, spath, dpath, resolution, whats):
    wdir = utils.get_tempdir()

    try:
        if os.path.exists(dpath + ".failed"):
            raise RuntimeError("Fail file exists: %s" % \
                               (dpath + ".failed",))
        _download_laz(url = url, path = wdir)
        _write_pdaljson(path = wdir,
                        resolution = resolution,
                        whats = whats)
        _run_pdal(path = wdir)

        for what in whats:
            os.rename(os.path.join\
                      (wdir,'dtm_laz_%s.tif' % what),
                      dpath % what)
    except Exception as e:
        print("""
        Cannot process file!
        url:       %s
        File name: %s
        Error:     %s
        """ % (url, dpath, str(e)), file=sys.stderr)
        _touch(dpath + ".failed")
    finally:
        shutil.rmtree(wdir)
