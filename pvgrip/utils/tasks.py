import os
import time

from pvgrip \
    import CELERY_APP
from pvgrip.utils.celery_one_instance \
    import one_instance

from pvgrip.storage.remotestorage_path \
    import RemoteStoragePath, is_remote_path

from pvgrip.utils.files \
    import move_file



@CELERY_APP.task()
@one_instance()
def call_fn_cache(result, ofn, storage_type):
    if result is None:
        return ofn

    ofn = RemoteStoragePath\
        (ofn, remotetype = storage_type)
    result_rmt = RemoteStoragePath\
        (result, remotetype = storage_type)

    if os.path.exists(result_rmt.path):
        move_file(result_rmt.path, ofn.path, True)

    if is_remote_path(result):
        ofn.link(result_rmt.path)
        return str(ofn)

    ofn_rpath.upload()
    return str(ofn)


@CELERY_APP.task()
@one_instance()
def task_test_queueonce(sleep = 5, dummy = 1):
    time.sleep(sleep)
    return True
