import os
import pickle
import logging

from functools import wraps

from pvgrip.storage.cassandra_path \
    import Cassandra_Path, is_cassandra_path
from pvgrip.utils.float_hash \
    import float_hash_fn

from pvgrip.utils.files \
    import get_tempfile, remove_file


def _get_locally_item(x):
    if isinstance(x, dict):
        x = {k:_get_locally_item(v) for k,v in x.items()}

    if isinstance(x, (list, tuple)) and \
       len(x) and isinstance(x[0], str):
        x = [_get_locally_item(v) for v in x]

    if isinstance(x, str) and \
       is_cassandra_path(x):
        x = Cassandra_Path(x).get_locally()

    return x


def _get_locally(*args, **kwargs):
    """Make sure all remote files are available locally

    Note, only the first level of argument is walked. For example, if
    argument is a list of files, this list is not checked.
    """
    args = [_get_locally_item(x) for x in args]

    kwargs = {k:_get_locally_item(v) \
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
                     ofn_arg = None,
                     minage = None):
    """Cache results of a function that returns a file

    :keys: list of arguments name to use for computing the unique name
    for the cache item

    :link: if using a hardlink instead of moving file to local cache

    :ignore: a boolean function that is computed if result of a
    function should be ignored.

    :ofn_arg: optional name of the argument that is intented to be
    used as a output filename

    :minage: unixtime, minage of acceptable stored cached value. if
    None any cached value is accepted

    """
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            from pvgrip.globals \
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
                if not minage:
                    return str(Cassandra_Path(ofn))

                if Cassandra_Path(ofn).get_timestamp() > minage:
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


def _results2pickle(fun):
    """Pickle results to a storage

    """
    @wraps(fun)
    def wrap(*args, **kwargs):
        res = fun(*args, **kwargs)

        ofn = get_tempfile()
        try:
            with open(ofn, 'wb') as f:
                pickle.dump(res, f)
        except Exception as e:
            remove_file(ofn)
            raise e

        return ofn
    return wrap


def _pickle2results(fun):
    """Get a pickled file and load it

    """
    @wraps(fun)
    def wrap(*args, **kwargs):
        ifn = fun(*args, **kwargs)

        ifn = Cassandra_Path(ifn).get_locally()
        with open(ifn, 'rb') as f:
            return pickle.load(f)
    return wrap


def cache_results(keys = None,
                  minage = None):
    def wrapper(fun):
        return _pickle2results\
            (cache_fn_results\
             (keys = keys, minage = minage)\
             (_results2pickle(fun)))
    return wrapper
