import os
import cv2
import shutil
import pickle

import open_elevation.utils as utils
import open_elevation.celery_tasks.app as app


def _save_png(data, ofn):
    wdir = utils.get_tempdir()

    try:
        tfn = os.path.join(wdir, 'image.png')
        cv2.imwrite(tfn, data['raster'])
        os.rename(tfn, ofn)
    finally:
        shutil.rmtree(wdir)


@app.CELERY_APP.task()
@app.cache_fn_results()
@app.one_instance(expire=10)
def save_png(pickle_fn):
    with open(pickle_fn, 'rb') as f:
        data = pickle.load(f)

    ofn = utils.get_tempfile()
    try:
        _save_png(data = data, ofn = ofn)
    except Exception as e:
        utils.remove_file(ofn)
        raise e
    return ofn
