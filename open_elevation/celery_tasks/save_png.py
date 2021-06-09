import os
import cv2
import shutil
import pickle
import logging

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.utils \
    import get_tempfile, remove_file, \
    get_tempdir, format_dictionary


def _save_png(data, ofn, normalize):
    wdir = get_tempdir()

    try:
        tfn = os.path.join(wdir, 'image.png')
        arr = data['raster']
        if normalize:
            arr = 255*(arr - arr.min())/(arr.max() - arr.min())
        cv2.imwrite(tfn, arr)
        os.rename(tfn, ofn)
    finally:
        shutil.rmtree(wdir)


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire=10)
def save_png(pickle_fn, normalize = False):
    logging.debug("save_png\n{}"\
                  .format(format_dictionary(locals())))
    with open(pickle_fn, 'rb') as f:
        data = pickle.load(f)

    ofn = get_tempfile()
    try:
        _save_png(data = data, ofn = ofn,
                  normalize = normalize)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn
