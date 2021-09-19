import geohash
import itertools
import numpy as np

import pandas as pd

from datetime import datetime

from pvgrip.utils.times import \
    timestr2datetime


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
    tl['date'] = tl.apply\
        (lambda x: \
         datetime.strftime(x['datetime'],'%Y-%m-%d'),
         axis = 1)
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
