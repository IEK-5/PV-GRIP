import os
import pickle
import logging

from functools import wraps

from pvgrip.globals \
    import DEFAULT_REMOTE

from pvgrip.storage.remotestorage_path \
    import RemoteStoragePath, is_remote_path, \
    searchandget_locally, search_determineremote
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
       is_remote_path(x):
        x = RemoteStoragePath(x).get_locally()

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
                     minage = None,
                     storage_type = DEFAULT_REMOTE):
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

    :storage_type: type of remote storage. Either: 'cassandra_path' or 'ipfs_path'.

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
            rpath = RemoteStoragePath\
                (ofn, remotetype=storage_type)
            _rpath = search_determineremote\
                (ofn, ignore_remote = storage_type)

            if rpath.in_storage() or _rpath is not None:
                if _rpath is not None:
                    rpath = _rpath

                logging.debug("""
                File is in cache!
                key = %s
                ofn = %s
                """ % (str(key), str(ofn)))
                if not minage:
                    return str(rpath)

                if rpath.get_timestamp() > minage:
                    return str(rpath)

            logging.debug("""
                File is NOT in cache!
                key = %s
                ofn = %s
                """ % (str(key), ofn))

            args, kwargs = _get_locally(*args, **kwargs)
            tfn = fun(*args, **kwargs)
            if ignore(tfn):
                return tfn

            if is_remote_path(tfn):
                tfn = searchandget_locally(tfn)

            if link:
                os.link(tfn, ofn)
            else:
                os.replace(tfn, ofn)
            rpath.upload()
            return str(rpath)
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

        ifn = RemoteStoragePath(ifn).get_locally()
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
