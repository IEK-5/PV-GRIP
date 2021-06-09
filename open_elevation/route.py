import rtree
import pyproj
import pickle

import numpy as np
import pandas as pd


from open_elevation.cache_fn_results \
    import cache_fn_results

from open_elevation.utils \
    import get_tempfile, remove_file


_T2LL = pyproj.Transformer.from_crs(3857, 4326,
                                    always_xy=True)
_T2MT = pyproj.Transformer.from_crs(4326, 3857,
                                    always_xy=True)


def _build_route_index(route, box):
    idx = rtree.index.Index()

    for index, x in route.iterrows():
        bounds = (x['latitude_metric'] + box[0],
                  x['longitude_metric'] + box[1],
                  x['latitude_metric'] + box[2],
                  x['longitude_metric'] + box[3])
        idx.insert(index, bounds,
                   obj = {'bounds': bounds,
                          'index': index,
                          'row': x})

    return idx


def _get_bigbox(box, box_delta):
    if box_delta < 1:
        return box

    return (box[0]*box_delta,box[1]*box_delta,
            box[2]*box_delta,box[3]*box_delta)


def _add_metric_coordinates(route):
    x = _T2MT.transform(route['longitude'],route['latitude'])
    x = pd.DataFrame(np.transpose(x))
    route['longitude_metric'] = x[0]
    route['latitude_metric'] = x[1]

    return route


def _box_mt2ll(box_metric):
    box = _T2LL.transform(box_metric[1],box_metric[0]) + \
        _T2LL.transform(box_metric[3],box_metric[2])
    box = (box[1],box[0],box[3],box[2])
    return box


@cache_fn_results()
def get_list_rasters(route_fn, box, box_delta):
    """Cluster route in boxes

    :route: fn for the dataframe with 'latitude' and 'longitude'
    columns

    :box: a box describing a minimum required neighbourhood for each
    route point

    :box_delta: a constant describing the maximum allow raster box to
    be sampled

    :return: a list, where each elements contains a box 'A' and a list
    of route coordinates that are contained in the box 'A' together
    with its neighbourhood described by the 'box' parameter

    """
    route = pd.read_csv(route_fn, sep=None, engine='python')
    route.columns = [x.lower() for x in route.columns]

    route = _add_metric_coordinates(route)
    idx = _build_route_index(route = route, box = box)
    bigbox = _get_bigbox(box = box, box_delta = box_delta)

    included = set()
    res = []
    for data in idx.intersection(idx.bounds, objects='raw'):
        if data['index'] in included:
            continue

        bounds = (data['row']['latitude_metric'] + bigbox[0],
                  data['row']['longitude_metric'] + bigbox[1],
                  data['row']['latitude_metric'] + bigbox[2],
                  data['row']['longitude_metric'] + bigbox[3])

        points = []
        raster = data['bounds']
        for x in idx.contains(bounds, objects='raw'):
            if x['index'] in included:
                continue

            points += [x['row'].to_dict()]
            raster = (min(raster[0], x['bounds'][0]),
                      min(raster[1], x['bounds'][1]),
                      max(raster[2], x['bounds'][2]),
                      max(raster[3], x['bounds'][3]))
            included.add(x['index'])
        res += [{'box_metric': raster,
                 'box': _box_mt2ll(raster),
                 'route': points}]

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump(res, f)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn
