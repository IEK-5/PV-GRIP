import os
import diskcache

from functools import wraps

from open_elevation.utils \
    import TASK_RUNNING

from open_elevation.float_hash \
    import float_hash

from open_elevation.globals \
    import RESULTS_PATH


def one_instance(expire=60):
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            TASKS_LOCK = diskcache.Cache\
                (directory = os.path.join(RESULTS_PATH,"_tasks_lock"),
                 size_limit = (1024**3))
            key = float_hash(("one_instance_lock",
                              fun.__name__, args, kwargs))
            lock = diskcache.Lock(TASKS_LOCK, key, expire = expire)

            if lock.locked():
                raise TASK_RUNNING()

            with lock:
                return fun(*args, **kwargs)
        return wrap
    return wrapper
