import os
import shutil
import logging

import numpy as np

from datetime \
    import datetime, time, timedelta
from math \
    import pi, cos, sin

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.globals \
    import GRASS
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.utils \
    import get_tempfile, remove_file, \
    run_command, get_tempdir
from open_elevation.celery_tasks.sample_raster_box \
    import sample_raster
from open_elevation.celery_tasks.save_geotiff \
    import save_gdal


def solar_time(timestr, lon):
    """Compute solar time

    Taken from:
    https://stackoverflow.com/a/13424528
    www.esrl.noaa.gov/gmd/grad/solcalc/solareqns.PDF

    :timestr: date and time UTC string in the format
    YYYY-MM-DD_HH:MM:SS]

    :lon: longitude

    :return: solar time {'day': int, 'hour': float}
    """
    dt = datetime.strptime(timestr, '%Y-%m-%d_%H:%M:%S')
    gamma = 2*pi/365*\
        (dt.timetuple().tm_yday-1+float(dt.hour-12)/24)
    eqtime = 229.18*\
        (0.000075+0.001868*cos(gamma)-\
         0.032077*sin(gamma)-\
         0.014615*cos(2*gamma)-\
         0.040849*sin(2*gamma))
    decl = 0.006918-0.399912*cos(gamma)+\
        0.070257*sin(gamma)-0.006758*cos(2*gamma)+\
        0.000907*sin(2*gamma)-0.002697*cos(3*gamma)+\
        0.00148*sin(3*gamma)
    time_offset = eqtime+4*lon
    tst = dt.hour*60+dt.minute+dt.second/60+time_offset
    s = datetime.combine(dt.date(),time(0))+\
        timedelta(minutes=tst)
    return {'day': s.timetuple().tm_yday,
            'hour': s.hour + s.minute/60 + s.second/(60*60)}


def _create_temp_grassdata(wdir, geotiff_fn, grass_fn):
    grass_path = os.path.join(wdir,'grass','PERMANENT')

    run_command\
        (what = [GRASS,'-c',geotiff_fn,'-e',
                 os.path.join(wdir,'grass')],
         cwd = wdir)
    run_command\
        (what = [GRASS, grass_path,
                 '--exec','r.external',
                 'input=' + geotiff_fn,
                 'output=' + grass_fn],
         cwd = wdir)

    return wdir


def _compute_sun_incidence(wdir, ofn, solar_time, njobs = 4):
    grass_path = os.path.join(wdir,'grass','PERMANENT')

    run_command\
        (what = [GRASS, grass_path,
                 '--exec','r.sun',
                 'elevation=elevation',
                 'incidout=incidence',
                 'day=' + str(solar_time['day']),
                 'time=' + ('%.5f' % solar_time['hour']),
                 'nprocs=' + str(njobs)],
         cwd = wdir)
    run_command\
        (what = [GRASS, grass_path,
                 '--exec','r.out.gdal',
                 'input=incidence',
                 'output=incidence.tif'],
         cwd = wdir)
    os.rename(os.path.join(wdir, 'incidence.tif'), ofn)

    return ofn


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
        time = solar_time(timestr = timestr,
                          lon = GDALInterface(tif_fn)\
                          .get_centre()['lon'])
        _create_temp_grassdata(wdir = wdir,
                               geotiff_fn = tif_fn,
                               grass_fn = 'elevation')
        _compute_sun_incidence(wdir = wdir,
                               ofn = ofn,
                               solar_time = time)
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
