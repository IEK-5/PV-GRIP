import time

from celery_once import QueueOnce

import open_elevation.utils
import open_elevation.celery_tasks.app as app


@app.CELERY_APP.task()
def task_test_no_nested_celery():
    if open_elevation.utils.if_in_celery():
        return True
    return False


@app.CELERY_APP.task(base=QueueOnce, once={'keys': ['sleep']})
def task_test_queueonce(sleep = 5, dummy = 1):
    time.sleep(sleep)
    return True
