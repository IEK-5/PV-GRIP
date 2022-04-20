import pyproj

from functools import lru_cache


@lru_cache(maxsize = 5)
def _TSFR(fr, to):
    return pyproj.Transformer.from_crs\
        (fr, to, always_xy=True)


def _TSF(lat, lon, fr, to):
    return _TSFR(fr = fr, to = to).transform(lon, lat)[::-1]


def epsg2ll(lat, lon, epsg):
    """Convert epsg coordinates to WGS84 latitude and longitude

    :lat: latitude (wrt epsg)

    :lon: latitude (wrt epsg)

    :epsg: source coordinate system code

    :return: a tuple of latitude and longtitude in WGS84

    """
    return _TSF(lat = lat, lon = lon, fr = epsg, to = 4326)


def ll2epsg(lat, lon, epsg):
    """Convert WGS84 coordinates to epsg latitude and longitude

    :lat: latitude (wrt WGS84)

    :lon: latitude (wrt WGS84)

    :epsg: target coordinate system code

    :return: a tuple of latitude and longtitude in given epsg
    coordinate system

    """
    return _TSF(lat = lat, lon = lon, fr = 4326, to = epsg)
