import pyproj
import numpy as np


def _mesh_metric(box, step):
    """Generate mesh in 'epsg:3857'

    :box: box location in WGS84 ('epsg:4326')

    :step: a step to make in meters (in 'epsg:3857')

    :return: dictionary with 'mesh', 'raster_box', and 'step': 'mesh'
    is a tuple of lon and lat coordinates defining the
    grid. 'raster_box' and 'step' define what needed to make a
    georaster
    """
    t2ll = pyproj.Transformer.from_crs(3857, 4326, always_xy=True)
    t2mt = pyproj.Transformer.from_crs(4326, 3857, always_xy=True)

    box_mt = t2mt.transform(box[1], box[0]) + \
        t2mt.transform(box[3], box[2])

    lon = np.arange(box_mt[0], box_mt[2], step)
    lat = np.arange(box_mt[1], box_mt[3], step)

    lon = [t2ll.transform(i, box_mt[1])[0] for i in lon]
    lat = [t2ll.transform(box_mt[0], i)[1] for i in lat]
    return {'mesh': (lon,lat),
            'raster_box': box_mt,
            'step': step,
            'epsg': 3857}


def _mesh_wgs84(box, step):
    """Generate mesh in 'epsg:4326'

    :box: box location in WGS84 ('epsg:4326')

    :step: a step to make in degrees (in 'epsg:4326')

    """
    lon = np.arange(box[1], box[3], step)
    lat = np.arange(box[0], box[2], step)

    return {'mesh': (lon,lat),
            'raster_box': box,
            'step': step,
            'epsg': 4326}


def mesh(box, step, which = 'metric'):
    if which == 'metric':
        return _mesh_metric(box = box, step = step)

    if which == 'wgs84':
        return _mesh_wgs84(box = box, step = step)
