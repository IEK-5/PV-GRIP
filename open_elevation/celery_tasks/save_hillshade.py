import os
import shutil
import tempfile

import open_elevation.utils as utils
import open_elevation.celery_tasks.app as app


def _save_pnghillshade(ifn, ofn):
    """Compute the hillshade image of the

    :ifn: input file
    :ofn: output file

    """
    wdir = utils.get_tempdir()

    try:
        utils.run_command\
            (what = ['gdaldem','hillshade',
                     '-of','png',ifn, 'hillshade.png'],
             cwd = wdir)
        os.rename(os.path.join(wdir, 'hillshade.png'), ofn)
    finally:
        shutil.rmtree(wdir)


@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire = 10)
def save_pnghillshade(geotiff_fn):
    ofn = utils.get_tempfile()
    try:
        _save_pnghillshade(geotiff_fn, ofn)
    except Exception as e:
        utils.remove_file(ofn)
        raise e
    return ofn
