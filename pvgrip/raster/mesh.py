import pyproj
import numpy as np

from pvgrip.utils.epsg \
    import epsg2ll, ll2epsg

from pyproj.aoi \
    import AreaOfInterest
from pyproj.database \
    import query_utm_crs_info


def determine_epsg(box, mesh_type):
    """Select epsg

    :box: box location in WGS84 ('epsg:4326')

    :mesh_type: either 'metric' or a epsg code

    :return: integer epsg code

    """
    if 'utm' == mesh_type:
        utm_crs_list = query_utm_crs_info\
            (datum_name="WGS 84",
             area_of_interest=AreaOfInterest(
                 south_lat_degree = box[0],
                 west_lon_degree = box[1],
                 north_lat_degree = box[2],
                 east_lon_degree = box[3]
             ))

        if 0 == len(utm_crs_list):
            raise RuntimeError\
                ("cannot determine any single utm for the box = {box}"\
                 .format(box))

        return int(utm_crs_list[0].code)

    try:
        return int(mesh_type)
    except:
        pass

    raise RuntimeError("mesh_type = {}, but should be "
                       "either 'utm' or an integer")


def _mesh_epsg(box, step, epsg):
    """Generate mesh in a given epsg system

    :box: box location in WGS84 ('epsg:4326')

    :step: a step to make in the units of the provided epsg

    :epsg: integer code for the coordinate system

    :return: dictionary with 'mesh', 'raster_box', and 'step': 'mesh'
    is a tuple of lon and lat coordinates defining the
    grid. 'raster_box' and 'step' define what needed to make a
    georaster

    """
    box_mt = ll2epsg(lat = box[0], lon = box[1], epsg = epsg) \
           + ll2epsg(lat = box[2], lon = box[3], epsg = epsg)

    lat = np.arange(box_mt[0], box_mt[2], step)
    lon = np.arange(box_mt[1], box_mt[3], step)

    lat = [epsg2ll(i, box_mt[1], epsg = epsg)[0] for i in lat]
    lon = [epsg2ll(box_mt[0], i, epsg = epsg)[1] for i in lon]
    return {'mesh': (lat,lon),
            'raster_box': box_mt,
            'step': step,
            'epsg': epsg}


def mesh(box, step, mesh_type):
    epsg = determine_epsg(box = box, mesh_type = mesh_type)
    return _mesh_epsg(box = box, step = step, epsg = epsg)


def mesh2box(mesh):
    boxmt = mesh['raster_box']
    box = epsg2ll(lat = boxmt[0], lon = boxmt[1], epsg = mesh['epsg']) \
        + epsg2ll(lat = boxmt[2], lon = boxmt[3], epsg = mesh['epsg'])
    return box, mesh['epsg']
