import os
import sys
import json
import celery
import shutil
import requests
import tempfile
import diskcache
import subprocess


from open_elevation.celery_tasks.app \
    import CELERY_APP


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


@CELERY_APP.task()
def task_las_processing(url, spath, dpath, resolution, whats):
    wdir = tempfile.mkdtemp(dir=spath)

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
        NRW_TASKS = diskcache.Cache\
            (os.path.join(spath,"_NRW_LAZ_Processing_Tasks"),
             size_limit = 100*(1024**2))
        with diskcache.RLock(NRW_TASKS,
                             "lock: %s" % dpath):
            NRW_TASKS.delete("processing: %s" % dpath)
