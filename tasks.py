import os
import json
import celery
import shutil
import requests
import tempfile
import diskcache
import subprocess

CELERY_APP = celery.Celery()


def _write_pdaljson(path, resolution):
    data = {}
    data['pipeline'] = [
        'src.laz',
        {
            'filename': 'dtm_laz.tif',
            'gdaldriver': 'GTiff',
            'output_type': 'all',
            'resolution': resolution,
            'type': 'writers.gdal'
        }
    ]

    with open(os.path.join(path, 'pdal.json'), 'w') as f:
        json.dump(data, f)


def _download_laz(url, path):
    r = requests.get(url, allow_redirects=True)
    open(os.path.join(path, 'src.laz'), 'wb')\
        .write(r.content)


def _run_pdal(path):
    subprocess.run(['pdal','pipeline','pdal.json'],
                   cwd = path)


def _convert_wgs84(path):
    subprocess.run(['gdalwarp',
                    'dtm_laz.tif',
                    'res.tif',
                    '-t_srs',
                    '+proj=longlat +ellps=WGS84'],
                   cwd = path)


@CELERY_APP.task()
def task_las_processing(url, spath, dpath, resolution):
    try:
        wdir = tempfile.mkdtemp(dir=spath)

        _download_laz(url = url, path = wdir)
        _write_pdaljson(path = wdir,
                             resolution = resolution)
        _run_pdal(path = wdir)
        _convert_wgs84(path = wdir)
        os.rename(os.path.join(wdir,'res.tif'), dpath)

        shutil.rmtree(wdir)
    finally:
        NRW_TASKS = diskcache.Cache\
            (os.path.join(spath,"_NRW_LAZ_Processing_Tasks"))
        with diskcache.RLock(NRW_TASKS,
                             "lock: %s" % dpath):
            NRW_TASKS.delete("processing: %s" % dpath)
