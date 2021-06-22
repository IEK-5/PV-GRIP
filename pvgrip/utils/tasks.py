import time

from cassandra_io.files \
    import Cassandra_Files
from pvgrip.utils.celery_one_instance \
    import one_instance
from pvgrip \
    import CELERY_APP



@CELERY_APP.task()
@one_instance()
def task_test_queueonce(sleep = 5, dummy = 1):
    time.sleep(sleep)
    return True
