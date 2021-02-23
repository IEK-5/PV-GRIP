import os
import cv2
import shutil
import pickle

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.utils \
    import get_tempfile, remove_file, get_tempdir


def _save_png(data, ofn):
    wdir = get_tempdir()

    try:
        tfn = os.path.join(wdir, 'image.png')
        cv2.imwrite(tfn, data['raster'])
        os.rename(tfn, ofn)
    finally:
        shutil.rmtree(wdir)


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire=10)
def save_png(pickle_fn):
    with open(pickle_fn, 'rb') as f:
        data = pickle.load(f)

    ofn = get_tempfile()
    try:
        _save_png(data = data, ofn = ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn
