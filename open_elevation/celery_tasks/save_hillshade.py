import os
import shutil

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.utils \
    import get_tempfile, remove_file, \
    run_command, get_tempdir


def _save_pnghillshade(ifn, ofn):
    """Compute the hillshade image of the

    :ifn: input file
    :ofn: output file

    """
    wdir = get_tempdir()

    try:
        run_command\
            (what = ['gdaldem','hillshade',
                     '-of','png',ifn, 'hillshade.png'],
             cwd = wdir)
        os.rename(os.path.join(wdir, 'hillshade.png'), ofn)
    finally:
        shutil.rmtree(wdir)


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def save_pnghillshade(geotiff_fn):
    ofn = get_tempfile()
    try:
        _save_pnghillshade(geotiff_fn, ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn
