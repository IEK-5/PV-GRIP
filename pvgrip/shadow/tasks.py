import shutil
import logging

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


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def compute_shadow_map(ifn):
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


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 60*10)
def compute_incidence(tif_fn, timestr):
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
