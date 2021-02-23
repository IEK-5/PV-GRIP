import time

from cassandra_io.files \
    import Cassandra_Files
from open_elevation.utils \
    import if_in_celery
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.celery_tasks \
    import CELERY_APP


@CELERY_APP.task()
def task_test_no_nested_celery():
    if if_in_celery():
        return True
    return False


@CELERY_APP.task()
@one_instance()
def task_test_queueonce(sleep = 5, dummy = 1):
    time.sleep(sleep)
    return True
