import os

from scipy import ndimage as nd

from pvgrip.raster.mesh \
    import mesh


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
    const = 1 if 'wgs84' == mesh_type else 111000
    if const*abs(box[2] - box[0])/step > limit \
       or const*abs(box[3] - box[1])/step > limit:
        raise RuntimeError\
            ("step in box should not be larger than %.2f" % limit)

    grid = mesh(box = box, step = step, which = mesh_type)
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
