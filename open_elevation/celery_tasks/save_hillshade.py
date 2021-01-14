import os
import shutil
import tempfile
import subprocess

from celery_once import QueueOnce
import open_elevation.celery_tasks.app as app


def _save_pnghillshade(ifn, ofn):
    """Compute the hillshade image of the

    :ifn: input file
    :ofn: output file

    """
    wdir = tempfile.mkdtemp(dir = '.')

    try:
        subprocess.run(['gdaldem','hillshade',
                        '-of','png',ifn, 'hillshade.png'],
                       cwd = wdir)
        os.rename(os.path.join(wdir, 'hillshade.png'), ofn)
    finally:
        shutil.rmtree(wdir)


@app.CELERY_APP.task(base=QueueOnce, once={'timeout': 10})
def save_pnghillshade(geotiff_fn):
    ofn = app.RESULTS_CACHE\
             .get(('save_pnghillshade',geotiff_fn),
                  check=False)
    if app.RESULTS_CACHE.file_in(ofn):
        return ofn

    _save_pnghillshade(geotiff_fn, ofn)
    app.RESULTS_CACHE.add_file(ofn)
    return ofn
