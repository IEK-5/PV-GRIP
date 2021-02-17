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

from cassandra_io.files \
    import Cassandra_Files



CELERY_APP = celery.Celery(broker='redis://localhost:6379/0',
                           backend='redis://localhost:6379/0',
                           task_track_started=True)

_RESULTS_PATH = os.path.join(git_root(),'data','results_cache')
RESULTS_CACHE = ResultFiles_LRUCache(path = _RESULTS_PATH,
                                     maxsize = 150)

_CASSANDRA_STORAGE_IP = '172.17.0.2'
_CASSANDRA_STORAGE_CHUNKSIZE = 1048576
_CASSANDRA_STORAGE_KEYSPACE_SUFFIX = '_open_elevation'
_CASSANDRA_REPLICATION = 'SimpleStrategy'
_CASSANDRA_REPLICATION_ARGS = {'replication_factor': 1}
CASSANDRA_STORAGE = Cassandra_Files\
    (cluster_ips = [_CASSANDRA_STORAGE_IP],
     keyspace_suffix = _CASSANDRA_STORAGE_KEYSPACE_SUFFIX,
     chunk_size = _CASSANDRA_STORAGE_CHUNKSIZE,
     replication = _CASSANDRA_REPLICATION,
     replication_args = _CASSANDRA_REPLICATION_ARGS)

TASKS_LOCK = diskcache.Cache\
    (directory = os.path.join(_RESULTS_PATH,"_tasks_lock"),
     size_limit = (1024**3))


class Cassandra_Path(str):

    def in_cassandra(self):
        return str(self) in CASSANDRA_STORAGE


    def get_locally(self):
        fn = str(self)
        if fn not in CASSANDRA_STORAGE:
            raise RuntimeError\
                ("%s not in CASSANDRA_STORAGE" % fn)

        if RESULTS_CACHE.file_in(fn):
            return ofn

        CASSANDRA_STORAGE.download(fn, fn)
        return RESULTS_CACHE.add_file(fn)


def get_locally(*args, **kwargs):
    """Make sure all remote files are available locally

    Note, only the first level of argument is walked. For example, if
    argument is a list of files, this list is not checked.
    """
    args = [x.get_locally()\
            for x in args
            if isinstance(x, Cassandra_Path)]

    kwargs = {k:(v.get_locally()) \
              for k,v in kwargs.items()
              if isinstance(v, Cassandra_Path)}

    return args, kwargs


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
                     to_cassandra = True,
                     ofn_arg = None):
    """Cache results of a function that returns a file

    :keys: list of arguments name to use for computing the unique name
    for the cache item

    :link: if using a hardlink instead of moving file to local cache

    :to_cassandra: if True then cache is searched in cassandra storage

    :ignore: a boolean function that is computed if result of a
    function should be ignored.

    :ofn_arg: optional name of the argument that is intented to be
    used as a output filename

    """
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

            ofn = Cassandra_Path(ofn)
            if (to_cassandra and ofn.in_cassandra()) or \
               (not to_cassandra and RESULTS_CACHE.file_in(ofn)):
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

            args, kwargs = get_locally(*args, **kwargs)
            tfn = fun(*args, **kwargs)
            if ignore(tfn):
                return tfn

            CASSANDRA_STORAGE.upload(tfn, ofn)
            if link:
                os.link(tfn, ofn)
            else:
                os.replace(tfn, ofn)
            RESULTS_CACHE.add_file(ofn)
            return Cassandra_Path(ofn)
        return wrap
    return wrapper
