import celery
import geohash

import pandas as pd

from datetime import datetime
from functools import wraps

from pvgrip.weather.utils \
    import timelocation_add_hash, \
    timelocation_add_datetimes

from pvgrip.storage.upload \
    import upload

from pvgrip.route.tasks \
    import merge_tsv


class _saveas_request_data:
    """just a dummy class that has .save method

    """

    def __init__(self, df):
        self._df = df


    def save(self, ofn, overwrite):
        self._df.to_csv(ofn, sep='\t', index = False)


def _read_route(route_fn, hows, hash_length):
    """read route

    :route_fn: path to the route file containing
    'latitude','longitude','timestr'
    """
    route = pd.read_csv(route_fn, sep=None, engine='python')

    if 'latitude' not in route or \
       'longitude' not in route or \
       'timestr' not in route:
        raise RuntimeError\
            ("'latitude' or 'longitude' or 'timestr' " +\
             "not in the route file!")

    if 'region_hash' in hows:
        route = timelocation_add_hash(route, hash_length)

    if 'month' in hows or \
       'week' in hows or \
       'date' in hows:
        route = timelocation_add_datetimes(route)
        route['month'], route['week'] = \
            zip(*route['datetime']\
                .map(lambda x: \
                     datetime.strftime\
                     (x, '%m|%G-W%V')\
                     .split('|')))

    return route


def _split_route(route, hows, maxnrows):
    if hows == ():
        return [route]
    car, cdr = hows[0], hows[1:]

    res = []
    chunks = [x for _,x in route.groupby(car)]

    for chunk in chunks:
        if chunk.shape[0] < maxnrows:
            res += [chunk]
            continue

        res += _split_route(chunk, cdr, maxnrows)

    return res


def split_route(route_fn,
                hows = ("region_hash","month","week","date"),
                hash_length = 4,
                maxnrows = 3000):
    """Split route file on chunks

    :route_fn: path to the route file containing
    'latitude','longitude','timestr'

    :hows: defines how to split the file.
    the order defines rules how to split the file.

    :hash_length: hash length that defines the locations split.
    the value of 4 corresponds to the a square with ~22km side.

    :maxnrows: defines when to try to split the file on chunk

    """
    route = _read_route(route_fn, hows = hows,
                        hash_length = hash_length)
    chunks = _split_route(route,
                          hows = hows,
                          maxnrows = maxnrows)

    return [upload(_saveas_request_data(x))['storage_fn']
            for x in chunks]


def split_route_calls\
    (fn_arg,
     hows = ("region_hash","month","week","date"),
     hash_length = 4,
     maxnrows = 3000):
    """A decorator that splits route onto chunks

    The result of the passed tasks should be a tsv file.
    """
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            if fn_arg not in kwargs:
                raise RuntimeError\
                    ("{} was not passed as kwargs"\
                     .format(fn_arg))

            chunks = split_route\
                (route_fn = kwargs[fn_arg],
                 hows = hows,
                 hash_length = hash_length,
                 maxnrows = maxnrows)
            tasks = []
            for x in chunks:
                chunk_kwargs = kwargs
                chunk_kwargs.update({fn_arg: x})
                tasks += [fun(*args, **chunk_kwargs)]
            tasks = celery.group(tasks)
            tasks |= merge_tsv.signature()

            return tasks
        return wrap
    return wrapper
