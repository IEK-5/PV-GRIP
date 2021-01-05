import time

from open_elevation.nrw_las import \
    TASK_RUNNING


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
