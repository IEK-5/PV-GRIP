import shutil
import logging

from pvgrip \
    import CELERY_APP
from pvgrip.globals \
    import GRASS_NJOBS
from pvgrip.utils.cache_fn_results \
    import cache_fn_results
from pvgrip.utils.celery_one_instance \
    import one_instance

from pvgrip.utils.files \
    import get_tempfile, remove_file, get_tempdir
from pvgrip.utils.run_command \
    import run_command
from pvgrip.utils.format_dictionary \
    import format_dictionary
from pvgrip.utils.solar_time \
    import solar_time

from pvgrip.ssdp.utils \
    import pickle2ssdp_topography, array_1d_2pickle, call_ssdp
from pvgrip.ssdp.generate_script \
    import poa_raster

from pvgrip.grass.io \
    import upload_grass_many, combine_grass_many
from pvgrip.grass.rsun \
    import call_rsun_irradiance

from pvgrip.raster.gdalinterface \
    import GDALInterface


@CELERY_APP.task(bind=True)
@cache_fn_results()
@one_instance(expire = 60*10)
def compute_irradiance_ssdp(self, ifn,
                            utc_time, lat, lon,
                            ghi, dhi, albedo,
                            nsky):
    logging.debug("compute_irradiance_ssdp\n{}"\
                  .format(format_dictionary(locals())))
    ssdp_ifn = get_tempfile()
    ssdp_ofn = get_tempfile()
    ofn = get_tempfile()
    try:
        ssdp_ifn, data, grid = \
            pickle2ssdp_topography(ifn, ssdp_ifn)

        call = poa_raster\
            (topography_fname = ssdp_ifn,
             utc_time = utc_time,
             albedo = albedo,
             lat = lat, lon = lon,
             ghi = ghi, dhi = dhi,
             nsky = nsky, ofn = ssdp_ofn,
             grid = grid)

        call_ssdp(call)
        return array_1d_2pickle(ssdp_ofn, data, ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    finally:
        remove_file(ssdp_ifn)
        remove_file(ssdp_ofn)


@CELERY_APP.task(bind=True)
@cache_fn_results(minage = 1626846910)
@one_instance(expire = 60*10)
def compute_irradiance_grass(self, elevation_fn, timestr,
                             aspect_fn = None, aspect_value = None,
                             slope_fn = None, slope_value = None,
                             linke_fn = None, linke_value = None,
                             albedo_fn = None, albedo_value = None,
                             coeff_bh_fn = None, coeff_dh_fn = None):
    logging.debug("compute_irradiance_grass\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    ofn = get_tempfile()

    try:
        tif = GDALInterface(elevation_fn)
        time = solar_time(timestr = timestr,
                          lon = tif.get_centre()['lon'])

        fns = upload_grass_many(wdir = wdir,
                                elevation = elevation_fn,
                                aspect = aspect_fn,
                                slope = slope_fn,
                                linke = linke_fn,
                                albedo = albedo_fn,
                                coeff_bh = coeff_bh_fn,
                                coeff_dh = coeff_dh_fn)

        ofns = call_rsun_irradiance\
            (wdir = wdir,
             solar_time = time,
             aspect_value = aspect_value,
             slope_value = slope_value,
             linke_value = linke_value,
             albedo_value = albedo_value,
             **fns,
             njobs = GRASS_NJOBS,
             npartitions = 1)

        combine_grass_many(wdir = wdir,
                           ofn = ofn,
                           grass_outputs = ofns)
    except Exception as e:
        remove_file(ofn)
        raise e
    finally:
        shutil.rmtree(wdir)

    return ofn
