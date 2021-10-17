import cv2
import shutil
import logging
import pickle

import numpy as np

from pvgrip.utils.solar_time \
    import solar_time

from pvgrip.raster.gdalinterface \
    import GDALInterface
from pvgrip.raster.io \
    import save_gdal

from pvgrip.grass.io \
    import upload_grass_data, download_grass_data
from pvgrip.grass.rsun \
    import call_rsun_incidence

from pvgrip \
    import CELERY_APP
from pvgrip.globals \
    import get_SPATIAL_DATA, GRASS_NJOBS
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance

from pvgrip.utils.files \
    import get_tempfile, remove_file, get_tempdir
from pvgrip.utils.format_dictionary \
    import format_dictionary


@CELERY_APP.task(bind=True)
@cache_fn_results()
@one_instance(expire = 10)
def compute_shadow_map(self, ifn):
    """convert incidence geotiff to binary shadow map

    :ifn: geotiff with incidence angle. nan are shadows

    """
    logging.debug("compute_shadow_map\n{}"\
                  .format(format_dictionary(locals())))
    incidence = GDALInterface(ifn)
    shadow = np.invert(np.isnan(incidence.points_array))
    shadow = shadow.astype(int)

    ofn = get_tempfile()
    try:
        save_gdal(ofn, shadow,
                  incidence.geo_transform,
                  incidence.epsg)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn


@CELERY_APP.task(bind=True)
@cache_fn_results(minage = 1626846910)
@one_instance(expire = 60*10)
def compute_incidence(self, tif_fn, timestr):
    """compute sun incidence angle

    :tif_fn: elevation geotiff

    :timestr: utc time string

    """
    logging.debug("compute_incidence\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    ofn = get_tempfile()

    try:
        tif = GDALInterface(tif_fn)
        time = solar_time(timestr = timestr,
                          lon = tif.get_centre()['lon'])
        upload_grass_data(wdir = wdir,
                          geotiff_fn = tif_fn,
                          grass_fn = 'elevation')
        call_rsun_incidence(wdir = wdir,
                            solar_time = time,
                            njobs = GRASS_NJOBS,
                            npartitions = 1)
        download_grass_data(wdir = wdir,
                            grass_fn = 'incidence',
                            geotiff_fn = ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    finally:
        shutil.rmtree(wdir)

    return ofn


@CELERY_APP.task(bind=True)
@cache_fn_results(minage = 1626846910)
@one_instance(expire = 60*10)
def average_png(self, png_files):
    """Compute average

    :png_files: a list of binary png files (with 0 or 1 values)

    :return:

    """
    logging.debug("average_png\n{}"\
                  .format(format_dictionary(locals())))

    res = cv2.imread(png_files[0],0)

    for png_fn in png_files[1:]:
        res += cv2.imread(png_fn, 0)

    res = res.astype(float)
    res /= len(png_files)

    ofn = get_tempfile()
    try:
        with open(ofn, 'wb') as f:
            pickle.dump({'raster': res}, f)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn
