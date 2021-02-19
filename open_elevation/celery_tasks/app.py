import os
import sys
import celery
import logging
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
                                     maxsize = 150)
TASKS_LOCK = diskcache.Cache\
    (directory = os.path.join(_RESULTS_PATH,"_tasks_lock"),
     size_limit = (1024**3))


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


def _compute_ofn(keys, args, kwargs, fname):
      if keys is not None:
          uniq = (args,)
          if kwargs:
              uniq = {k:v for k,v in kwargs.items()
                      if k in keys}
      else:
          uniq = (args, kwargs)
      key = ("cache_results", fname, uniq)
      return RESULTS_CACHE.get(key, check = False), key


def cache_fn_results(keys = None,
                     link = False,
                     ignore = lambda x: False,
                     ofn_arg = None):
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            if not ofn_arg or ofn_arg not in kwargs:
                ofn, key = _compute_ofn(keys = keys,
                                   args = args,
                                   kwargs = kwargs,
                                   fname = fun.__name__)
            else:
                ofn, key = kwargs[ofn_arg], 'NA'

            if RESULTS_CACHE.file_in(ofn):
                logging.debug("""
                File is in cache!
                key = %s
                ofn = %s
                """ % (str(key), ofn))
                return ofn

            logging.debug("""
                File is NOT in cache!
                key = %s
                ofn = %s
                """ % (str(key), ofn))

            tfn = fun(*args, **kwargs)
            if ignore(tfn):
                return tfn

            if link:
                os.link(tfn, ofn)
            else:
                os.replace(tfn, ofn)
            RESULTS_CACHE.add_file(ofn)
            return ofn
        return wrap
    return wrapper
