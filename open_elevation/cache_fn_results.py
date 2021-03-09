import os
import logging

from functools import wraps

from open_elevation.cassandra_path \
    import Cassandra_Path, is_cassandra_path
from open_elevation.float_hash \
    import float_hash_fn


def _get_locally(*args, **kwargs):
    """Make sure all remote files are available locally

    Note, only the first level of argument is walked. For example, if
    argument is a list of files, this list is not checked.
    """
    args = [Cassandra_Path(x).get_locally() \
            if is_cassandra_path(x) else x\
            for x in args]

    kwargs = {k:(Cassandra_Path(v).get_locally() \
                 if is_cassandra_path(v) else v) \
              for k,v in kwargs.items()}

    return args, kwargs


def _compute_ofn(keys, args, kwargs, fname):
      if keys is not None:
          uniq = (args,)
          if kwargs:
              uniq = {k:v for k,v in kwargs.items()
                      if k in keys}
      else:
          uniq = (args, kwargs)
      key = ("cache_results", fname, uniq)
      return float_hash_fn(key), key


def cache_fn_results(keys = None,
                     link = False,
                     ignore = lambda x: False,
                     ofn_arg = None):
    """Cache results of a function that returns a file

    :keys: list of arguments name to use for computing the unique name
    for the cache item

    :link: if using a hardlink instead of moving file to local cache

    :ignore: a boolean function that is computed if result of a
    function should be ignored.

    :ofn_arg: optional name of the argument that is intented to be
    used as a output filename

    """
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            from open_elevation.globals \
                import get_CASSANDRA_STORAGE, get_RESULTS_CACHE

            if not ofn_arg or ofn_arg not in kwargs:
                ofn, key = _compute_ofn(keys = keys,
                                   args = args,
                                   kwargs = kwargs,
                                   fname = fun.__name__)
            else:
                ofn, key = kwargs[ofn_arg], 'NA'

            RESULTS_CACHE = get_RESULTS_CACHE()

            if Cassandra_Path(ofn).in_cassandra():
                logging.debug("""
                File is in cache!
                key = %s
                ofn = %s
                """ % (str(key), str(ofn)))
                return str(Cassandra_Path(ofn))

            logging.debug("""
                File is NOT in cache!
                key = %s
                ofn = %s
                """ % (str(key), ofn))

            args, kwargs = _get_locally(*args, **kwargs)
            tfn = fun(*args, **kwargs)
            if ignore(tfn):
                return tfn

            if is_cassandra_path(tfn):
                tfn = Cassandra_Path(tfn).get_locally()

            CASSANDRA_STORAGE = get_CASSANDRA_STORAGE()
            CASSANDRA_STORAGE.upload(tfn, Cassandra_Path(ofn).get_path())
            if link:
                os.link(tfn, ofn)
            else:
                os.replace(tfn, ofn)
            RESULTS_CACHE.add(ofn)
            return str(Cassandra_Path(ofn))
        return wrap
    return wrapper
