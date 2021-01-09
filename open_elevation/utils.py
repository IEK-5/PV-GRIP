import re
import os
import time

import open_elevation.celery_tasks.app as app


class TASK_RUNNING(Exception):
    pass


def retry(max_attempts = 100, sleep_on_task = 30):
    def wrapper_f(f):
        def wrap(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return f(*args, **kwargs)
                except TASK_RUNNING:
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


def list_files(path, regex):
    r = re.compile(regex)
    return [os.path.join(dp, f) \
            for dp, dn, filenames in \
            os.walk(path) \
            for f in filenames \
            if r.match(os.path.join(dp, f))]
