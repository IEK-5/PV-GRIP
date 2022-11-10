import re
import celery
import geohash

import pandas as pd

from datetime import datetime
from functools import wraps
from typing import Dict

from pvgrip.weather.utils \
    import timelocation_add_hash, \
    timelocation_add_datetimes, FieldMissing

from pvgrip.storage.upload \
    import upload, Saveas_Requestdata

from pvgrip.route.tasks \
    import merge_tsv

from pvgrip.storage.remotestorage_path \
    import searchandget_locally


def _read_route(route_fn, hows):
    """read route

    :route_fn: path to the route file containing
    'latitude','longitude','timestr'
    """
    route = pd.read_csv(route_fn, sep=None, engine='python')
    drop = []
    rex = re.compile(r'^region_hash(\d{1,})')

    try:
        for how in hows:
            if not rex.match(how):
                continue

            l = int(rex.match(how).groups()[0])
            route, d = timelocation_add_hash(route, l)
            route = route.rename(columns={f'{d}': f'region_hash{l}'})
            drop += list([f'region_hash{l}'])
    except FieldMissing as e:
        hows = tuple([x for x in hows if not rex.match(x)])

    try:
        if 'month' in hows or \
           'week' in hows or \
           'date' in hows:
            route, d = timelocation_add_datetimes(route)
            route['month'], route['week'] = \
                zip(*route['datetime']\
                    .map(lambda x: \
                         datetime.strftime\
                         (x, '%m|%G-W%V')\
                         .split('|')))
            drop += list(set(list(d) + ['month', 'week']))
    except FieldMissing as e:
        hows = tuple([x for x in hows \
                      if x not in ('month', 'week', 'date')])

    return route, hows, drop


def _split_route(route, hows, drop, maxnrows):
    if hows == ():
        return [route.drop(drop, axis=1)]
    car, cdr = hows[0], hows[1:]

    res = []
    chunks = [x for _,x in route.groupby(car)]

    for chunk in chunks:
        if chunk.shape[0] < maxnrows:
            res += [chunk.drop(drop, axis=1)]
            continue

        res += _split_route(chunk, cdr, drop, maxnrows)

    return res


def split_route(route_fn, hows, maxnrows = 3000):
    """Split route file on chunks

    :route_fn: path to the route file containing
    'latitude','longitude','timestr'

    :hows: defines how to split the file.
    the order defines rules how to split the file.

    :maxnrows: defines when to try to split the file on chunk

    """
    route_fn = searchandget_locally(route_fn)

    route, hows, drop = _read_route(route_fn, hows = hows)
    chunks = _split_route(route,
                          hows = hows,
                          drop = drop,
                          maxnrows = maxnrows)

    return [upload(Saveas_Requestdata(x))['storage_fn']
            for x in chunks]


def split_route_calls\
    (fn_arg,
     hows = ("region_hash4","region_hash5","region_hash6"),
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
