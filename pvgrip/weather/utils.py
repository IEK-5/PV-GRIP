import geohash
import itertools
import numpy as np

import pandas as pd

from datetime import datetime

from pvgrip.utils.times import \
    timestr2datetime, time_range2list


def bbox2hash(bbox, hash_length):
    """Split bounding box coordinates onto smaller boxes

    :bbox: (lat_min, lon_min, lat_max, lon_max)

    :hash_length: maximum of the geohash string

    :return: pd.DataFrame with 'hash' column
    """
    by = geohash.decode_exactly\
        (geohash.encode\
         (*bbox[:2], precision = hash_length))[2:]
    res = (geohash.encode(*x, precision = hash_length) \
           for x in itertools.product\
           (np.arange(bbox[0],bbox[2] + by[0],by[0]),
            np.arange(bbox[1],bbox[3] + by[1],by[1])))
    return pd.DataFrame(set(res), columns=['region_hash'])


def timelocation_add_hash(tl, hash_length):
    if 'latitude' not in tl or 'longitude' not in tl:
        raise RuntimeError\
            ("'latitude' or 'longitude' not in time_location!")

    tl['region_hash'] = tl.apply\
        (lambda x:
         geohash.encode(x['latitude'],x['longitude'],
                        precision = hash_length),
         axis = 1)
    return tl


def timelocation_add_datetimes(tl):
    if 'timestr' not in tl:
        raise RuntimeError\
            ("'timestr' not in time_location!")

    tl['datetime'] = tl.apply\
        (lambda x: \
         timestr2datetime(x['timestr']),
         axis = 1)
    tl['date'], tl['year'], tl['week'] = \
        zip(*tl['datetime']\
            .map(lambda x: \
                 datetime.strftime\
                 (x,'%Y-%m-%d|%G|%V')\
                 .split('|')))
    return tl


def timelocation_add_region(tl, output):
    if 'region_hash' not in tl:
        raise RuntimeError\
            ("'region_hash' is not in time_location!")

    if 'coordinate' == output:
        tl['region_latitude'], tl['region_longitude'] = \
            zip(*tl['region_hash']\
                .map(lambda x: \
                     geohash.decode_exactly(x)[:2]))
        return tl

    tl['region_bbox'] = tl['region_hash'].apply\
        (lambda x: tuple(geohash.bbox(x).values()))

    return tl


def _raiseiftoorecent(tl):
    if (datetime.now() - tl.datetime.max()).days < 8:
        raise RuntimeError\
            ("""you ask for too recent observations!

            take a break for a week...
            """)


def bbox_tl(box, time_range, time_step,
            hash_length,
            sample_hash_length = None,
            region_type = 'coordinate'):
    if not sample_hash_length:
        sample_hash_length = hash_length
    tl = bbox2hash(box, sample_hash_length)
    tl['sample_hash'] = tl['region_hash']
    tl['region_hash'] = tl['region_hash']\
        .str.slice(0,hash_length)
    times = time_range2list(time_range = time_range,
                            time_step = time_step,
                            time_format = '%Y-%m-%d_%H:%M:%S')
    times = pd.DataFrame(times, columns = ['timestr'])
    tl = tl.merge(times, how='cross')
    tl = timelocation_add_datetimes(tl)
    _raiseiftoorecent(tl)
    tl = timelocation_add_region(tl, region_type)
    return tl


def route_tl(route_fn, hash_length,region_type = 'coordinate'):
    tl = pd.read_csv(route_fn, sep=None, engine='python')

    if 'timestr' not in tl or \
       'longitude' not in tl or \
       'latitude' not in tl:
        raise RuntimeError\
            ("longitude, latitude or timestr are missing!")

    tl = timelocation_add_datetimes(tl)
    _raiseiftoorecent(tl)
    tl = timelocation_add_hash(tl, hash_length)
    tl = timelocation_add_region(tl, region_type)
    return tl
