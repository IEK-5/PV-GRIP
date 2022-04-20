import os
import pyproj
import itertools

import numpy as np

from scipy import ndimage as nd

from pvgrip.raster.mesh \
    import mesh
from pvgrip.utils.epsg \
    import epsg2ll, ll2epsg


def fill_missing(data, missing_value = -9999):
    """Replace the value of missing 'data' cells (indicated by
    'missing_value') by the value of the nearest cell

    Taken from: https://stackoverflow.com/a/27745627

    """
    missing = data == missing_value
    ind = nd.distance_transform_edt(missing,
                                    return_distances=False,
                                    return_indices=True)
    return data[tuple(ind)]


def check_box_not_too_big(box, step, mesh_type,
                          limit = 4400, max_points = 2e+7):
    const = 1 if '4326' == str(mesh_type) else 111000
    if const*abs(box[2] - box[0])/step > limit \
       or const*abs(box[3] - box[1])/step > limit:
        raise RuntimeError\
            ("step in box should not be larger than %.2f" % limit)

    grid = mesh(box = box, step = step, mesh_type = mesh_type)
    if len(grid['mesh'][0])*len(grid['mesh'][1]) \
       > max_points:
        raise RuntimeError\
            ("either box or resolution is too high!")

    return len(grid['mesh'][0]), len(grid['mesh'][1])


def index2fn(x, stat, pdal_resolution, ensure_las):
    """Convert whatever dictionary given in the index to the proper
filenames

    Why? One index entry may point to many file, for example, in the
    case of the lidar data. one region points to source lidar data,
    and a bunch of various sampled statistics.

    :x: a dictionary

    :return: a string
    """
    if ensure_las and ('remote_meta' not in x or \
                       not x['if_compute_las']):
        raise RuntimeError("""data is not LIDAR data!
        It seems that here the LIDAR data is explicitly needed.
        Check what you query!""")

    if 'remote_meta' not in x:
        return x['file']

    if x['if_compute_las']:
        return os.path.join\
            (x['file'],
             '{}_{:.8f}'.format(stat, pdal_resolution))

    return os.path.join(x['file'],'src')


def _rotate(arr, centre, azimuth):
    azimuth = np.deg2rad(azimuth)
    cos = np.cos(azimuth)
    sin = np.sin(azimuth)
    arr -= centre
    return np.transpose((arr[:,0]*cos - arr[:,1]*sin,
                         arr[:,0]*sin + arr[:,1]*cos)) + centre


def _neighbours(coords, neighbour_step):
    if len(neighbour_step) != 2:
        raise RuntimeError\
            ("neighbour_step.shape = {} != 2"\
             .format(neighbour_step))

    names=[''.join(x) \
           for x in itertools.product\
           (('c','s','n'),('c','e','w'))]

    res = []
    for lat_step, lon_step in itertools.product\
        ((0,neighbour_step[0],-neighbour_step[0]),
         (0,neighbour_step[1],-neighbour_step[1])):
        t = coords.copy()
        t[:,:2] += np.array((lat_step, lon_step))
        res += [t]

    return np.concatenate(res), names


def route_neighbours(route, azimuth_default,
                     neighbour_step, epsg):
    coords = []
    for x in route:
        if 'longitude' not in x or 'latitude' not in x:
            raise RuntimeError\
                ("longitude or latitude is missing!")

        lat_met, lon_met = ll2epsg\
            (lat = x['latitude'], lon = x['longitude'],
             epsg = epsg)
        if 'azimuth' not in x:
            x['azimuth'] = azimuth_default

        coords += [(lat_met, lon_met,
                    lat_met, lon_met, x['azimuth'])]

    res, names = _neighbours(np.array(coords), neighbour_step)
    res = _rotate(res[:,:2], res[:,2:4], res[:,4])
    res = epsg2ll(lat = res[:,0],lon = res[:,1], epsg = epsg)
    return np.transpose(res)[:,[1,0]].tolist(), names
