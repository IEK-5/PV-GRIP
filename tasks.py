import os
import json
import celery
import shutil
import requests
import tempfile
import diskcache
import subprocess

CELERY_APP = celery.Celery()


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
    open(os.path.join(path, 'src.laz'), 'wb')\
        .write(r.content)


def _run_pdal(path):
    subprocess.run(['pdal','pipeline','pdal.json'],
                   cwd = path)


def _convert_wgs84(path, whats):
    for what in whats:
        subprocess.run(['gdalwarp',
                        'dtm_laz_%s.tif' % what,
                        'res_%s.tif' % what,
                        '-t_srs','+proj=longlat +ellps=WGS84'],
                       cwd = path)


@CELERY_APP.task()
def task_las_processing(url, spath, dpath, resolution, whats):
    try:
        wdir = tempfile.mkdtemp(dir=spath)

        _download_laz(url = url, path = wdir)
        _write_pdaljson(path = wdir,
                        resolution = resolution,
                        whats = whats)
        _run_pdal(path = wdir)
        _convert_wgs84(path = wdir, whats = whats)

        for what in whats:
            os.rename(os.path.join(wdir,'res_%s.tif' % what),
                      dpath % what)

        shutil.rmtree(wdir)
    finally:
        NRW_TASKS = diskcache.Cache\
            (os.path.join(spath,"_NRW_LAZ_Processing_Tasks"))
        with diskcache.RLock(NRW_TASKS,
                             "lock: %s" % dpath):
            NRW_TASKS.delete("processing: %s" % dpath)
