from typing import Dict

import celery
import geohash

import pandas as pd

from datetime import datetime
from functools import wraps

from pvgrip.weather.utils \
    import timelocation_add_hash, \
    timelocation_add_datetimes, FieldMissing

from pvgrip.storage.upload \
    import upload, Saveas_Requestdata

from pvgrip.route.tasks \
    import merge_tsv

from pvgrip.storage.remotestorage_path \
    import searchandget_locally


def _read_route(route_fn, hows, hash_length):
    """read route

    :route_fn: path to the route file containing
    'latitude','longitude','timestr'
    """
    route = pd.read_csv(route_fn, sep=None, engine='python')

    try:
        if 'region_hash' in hows:
            route = timelocation_add_hash(route, hash_length)
    except FieldMissing as e:
        hows = tuple([x for x in hows if x != 'region_hash'])

    try:
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
    except FieldMissing as e:
        hows = tuple([x for x in hows \
                      if x not in ('month', 'week', 'date')])

    return route, hows


def _drop_columns(route, hows):
    drop = []

    if 'region_hash' in hows:
        drop += ['region_hash']

    if 'month' in hows or \
       'week' in hows or \
       'date' in hows:
        drop += ['month','week','datetime',
                 'date','year','week']

    route = route.drop(drop, axis=1)

    return route


def _split_route(route, hows, maxnrows):
    if hows == ():
        return [_drop_columns(route, hows = hows)]
    car, cdr = hows[0], hows[1:]

    res = []
    chunks = [x for _,x in route.groupby(car)]

    for chunk in chunks:
        if chunk.shape[0] < maxnrows:
            res += [_drop_columns(chunk, hows = hows)]
            continue

        res += _split_route(chunk, cdr, maxnrows)

    return res


def split_route(route_fn, hows, hash_length = 4, maxnrows = 3000):
    """Split route file on chunks

    :route_fn: path to the route file containing
    'latitude','longitude','timestr'

    :hows: defines how to split the file.
    the order defines rules how to split the file.

    :hash_length: hash length that defines the locations split.
    the value of 4 corresponds to the a square with ~22km side.

    :maxnrows: defines when to try to split the file on chunk

    """
    route_fn = searchandget_locally(route_fn)

    route, hows = _read_route(route_fn, hows = hows,
                              hash_length = hash_length)
    chunks = _split_route(route,
                          hows = hows,
                          maxnrows = maxnrows)

    return [upload(Saveas_Requestdata(x))['storage_fn']
            for x in chunks]


def split_route_calls\
    (fn_arg,
     hows = ("region_hash","month","week","date"),
     hash_length = 4,
     maxnrows = 10000,
     merge_task = merge_tsv,
     merge_task_args: Dict[str, str] = None):
    """A decorator that splits route onto chunks

    The result of the passed tasks is together. The combiner is
    defined by the `merge_task` argument.

    If the merge_task should have access some arguments of the
    decoratored task they can be specified in `merge_task_args`.
    `merge_task_args` is a dict that maps names of the args of the merge task
    to names of the args of the decorated task

    """
    if merge_task_args is None:
        merge_task_args = dict()
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            if fn_arg not in kwargs:
                raise RuntimeError\
                    ("{} was not passed as kwargs"\
                     .format(fn_arg))
            for decorated_task_args in merge_task_args.values():
                if decorated_task_args not in kwargs:
                    raise RuntimeError \
                        ("Merge task args:{} was not passed as kwargs" \
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

            return celery.group(tasks) | merge_task.signature(
                kwargs={merge_task_arg: kwargs[decorated_task_arg]
                        for (merge_task_arg, decorated_task_arg) in merge_task_args.items()})
        return wrap
    return wrapper
