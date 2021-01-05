import time

import open_elevation.nrw_las as nrw_las
import open_elevation.celery_tasks.app as app


def retry(max_attempts = 100, sleep_on_task = 30):
    def wrapper_f(f):
        def wrap(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return f(*args, **kwargs)
                except nrw_las.TASK_RUNNING:
                    time.sleep(sleep_on_task)
                    pass
                except:
                    attempts += 1
                    pass
            return f(*args, **kwargs)
        return wrap
    return wrapper_f


def if_in_celery():
    if not app.CELERY_APP.current_worker_task:
        return False
    return True
