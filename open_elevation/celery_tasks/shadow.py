import os
import shutil
import logging

import numpy as np

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.globals \
    import GRASS, GRASS_NJOBS
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.solar_time \
    import solar_time
from open_elevation.grass \
    import upload_grass_data, download_grass_data, \
    get_npartitions
from open_elevation.utils \
    import get_tempfile, remove_file, \
    run_command, get_tempdir
from open_elevation.celery_tasks.sample_raster_box \
    import sample_raster
from open_elevation.celery_tasks.save_geotiff \
    import save_gdal


def _compute_sun_incidence(wdir, solar_time,
                           njobs = 4, npartitions = 4):
    grass_path = os.path.join(wdir,'grass','PERMANENT')

    run_command\
        (what = [GRASS, grass_path,
                 '--exec','r.sun',
                 'elevation=elevation',
                 'incidout=incidence',
                 'day=%d' % int(solar_time['day']),
                 'time=%f' % float(solar_time['hour']),
                 'nprocs=%d' % int(njobs),
                 'npartitions=%d' % int(npartitions)],
         cwd = wdir)

    return 'incidence'


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def compute_shadow_map(ifn):
    from open_elevation.gdalinterface \
        import GDALInterface
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
    wdir = get_tempdir()
    ofn = get_tempfile()

    try:
        from open_elevation.gdalinterface \
            import GDALInterface
        tif = GDALInterface(tif_fn)
        time = solar_time(timestr = timestr,
                          lon = tif.get_centre()['lon'])
        upload_grass_data(wdir = wdir,
                          geotiff_fn = tif_fn,
                          grass_fn = 'elevation')
        _compute_sun_incidence(wdir = wdir,
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


def _save_binary_png(ifn, ofn):
    wdir = get_tempdir()

    try:
        run_command\
            (what = ['gdal_translate',
                     '-scale','0','1','0','255',
                     '-of','png',
                     ifn,'mask.png'],
             cwd = wdir)
        os.rename(os.path.join(wdir, 'mask.png'), ofn)
    finally:
        shutil.rmtree(wdir)


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def save_binary_png(tif_fn):
    ofn = get_tempfile()
    try:
        _save_binary_png(ifn = tif_fn, ofn = ofn)
    except Exception as e:
        remove_file(ofn)
        raise e
    return ofn


def shadow(timestr, what='shadow',
           output_type='png', **kwargs):
    """Start the shadow job

    :timestr: time string, format: %Y-%m-%d_%H:%M:%S

    :what: either shadow map or incidence

    :output_type: either geotiff or png.
    only makes sense for shadow binary map

    :kwargs: arguments passed to sample_raster

    """
    if what not in ('shadow', 'incidence'):
        raise RuntimeError("Invalid 'what' argument")
    if output_type not in ('png', 'geotiff'):
        raise RuntimeError("Invalid 'output' argument")

    kwargs['output_type'] = 'geotiff'
    tasks = sample_raster(**kwargs)

    tasks |= compute_incidence.signature\
        ((),{'timestr': timestr})

    if 'shadow' == what:
        tasks |= compute_shadow_map.signature()

        if 'png' == output_type:
            tasks |= save_binary_png.signature()

    return tasks
