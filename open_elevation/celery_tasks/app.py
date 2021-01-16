import os
import sys
import celery
import diskcache
import subprocess

from functools import wraps

from open_elevation.utils \
    import TASK_RUNNING, git_root

from open_elevation.results_lrucache \
    import ResultFiles_LRUCache, float_hash



CELERY_APP = celery.Celery(broker='redis://localhost:6379/0',
                           backend='redis://localhost:6379/0',
                           task_track_started=True)

_RESULTS_PATH = os.path.join(git_root(),'data','results_cache')
RESULTS_CACHE = ResultFiles_LRUCache(path = _RESULTS_PATH,
                                     maxsize = 20)
TASKS_LOCK = diskcache.Cache\
    (directory = os.path.join(_RESULTS_PATH,"_tasks_lock"),
     size_limit = 100*(1024**2))


def one_instance(expire=60):
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            key = float_hash(("one_instance_lock",
                              fun.__name__, args, kwargs))
            lock = diskcache.Lock(TASKS_LOCK, key, expire = expire)

            if lock.locked():
                raise TASK_RUNNING()

            with lock:
                return fun(*args, **kwargs)
        return wrap
    return wrapper


def cache_fn_results(keys = None):
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            if keys is not None:
                uniq = (args,)
                if kwargs:
                    uniq = {k:v for k,v in kwargs.items()
                            if k in keys}
            else:
                uniq = (args, kwargs)
            key = ("cache_results", fun.__name__, uniq)
            ofn = RESULTS_CACHE.get(key, check = False)
            if RESULTS_CACHE.file_in(ofn):
                return ofn

            tfn = fun(*args, **kwargs)
            os.replace(tfn, ofn)
            RESULTS_CACHE.add_file(ofn)
            return ofn
        return wrap
    return wrapper
