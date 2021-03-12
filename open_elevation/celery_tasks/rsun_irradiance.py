import os
import shutil

import numpy as np

from open_elevation.celery_tasks \
    import CELERY_APP
from open_elevation.globals \
    import GRASS, GRASS_NJOBS
from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance
from open_elevation.grass \
    import upload_grass_data, download_grass_data
from open_elevation.solar_time \
    import solar_time
from open_elevation.gdalinterface \
    import GDALInterface
from open_elevation.celery_tasks.save_geotiff \
    import save_gdal
from open_elevation.celery_tasks.sample_raster_box \
    import sample_raster
from open_elevation.utils \
    import get_tempfile, remove_file, \
    run_command, get_tempdir


def _join_tif(ofn, ifns):
    cur = GDALInterface(ifns[0])
    geotransform = cur.geo_transform
    epsg = cur.epsg
    res = cur.points_array
    for fn in ifns[1:]:
        res = np.concatenate\
            ((res, GDALInterface(fn).points_array),
             axis=2)
    return save_gdal(ofn = ofn, array = res,
                     geotransform = geotransform,
                     epsg = epsg)


def _compute_irradiance(wdir, solar_time,
                        elevation = None,
                        aspect = None, aspect_value = None,
                        slope = None, slope_value = None,
                        linke = None, linke_value = None,
                        albedo = None, albedo_value = None,
                        coeff_bh = None, coeff_dh = None,
                        njobs = 4, npartitions = 4):
    grass_path = os.path.join(wdir, 'grass','PERMANENT')
    radout = ['beam_rad','diff_rad','refl_rad','glob_rad']

    run_command\
        (what = \
         [GRASS, grass_path,
          '--exec','r.sun'] + \
         ['%s=%s' % (x,x) for x in radout] + \
         ['day=%d' % int(solar_time['day']),
          'time=%f' % float(solar_time['hour']),
          'nprocs=%d' % int(njobs),
          'npartitions=%d' % int(npartitions)] + \
         ['elevation=%s' % elevation] if elevation else [] + \
         ['aspect=%s' % aspect] if aspect else [] + \
         ['aspect_value=%f' % float(aspect_value)] \
         if aspect_value else [] + \
         ['slope=%s' % slope] if slope else [] + \
         ['slope_value=%f' % float(slope_value)] \
         if slope_value else [] + \
         ['linke=%s' % linke] if linke else [] + \
         ['linke_value=%f' % float(linke_value)] \
         if linke_value else [] + \
         ['albedo=%s' % albedo] if albedo else [] + \
         ['albedo_value=%f' % float(albedo_value)] \
         if albedo_value else [] + \
         ['coeff_bh=%s' % coeff_bh] if coeff_bh else [] + \
         ['coeff_dh=%s' % coeff_dh] if coeff_dh else [],
         cwd = wdir)

    return radout


def _combine_results(wdir, ofn, grass_outputs):
    grass_path = os.path.join(wdir, 'grass','PERMANENT')

    ofns = []
    try:
        for fn in grass_outputs:
            x = get_tempfile()
            ofns += [x]
            download_grass_data(wdir = wdir,
                                grass_fn = fn,
                                geotiff_fn = x)
        _join_tif(ofn = ofn, ifns = ofns)
    finally:
        for x in ofns:
            remove_file(x)

    return ofn


def _upload_to_grass(wdir, **kwargs):
    res = {}
    for k, v in kwargs.items():
        if v is None:
            res[k] = v
            continue

        upload_grass_data(wdir = wdir,
                          geotiff_fn = v,
                          grass_fn = k)
        res[k] = k

    return res


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 60*10)
def rsun_irradiance(elevation_fn, timestr,
                    aspect_fn = None, aspect_value = None,
                    slope_fn = None, slope_value = None,
                    linke_fn = None, linke_value = None,
                    albedo_fn = None, albedo_value = None,
                    coeff_bh_fn = None, coeff_dh_fn = None):
    wdir = get_tempdir()
    ofn = get_tempfile()

    try:
        tif = GDALInterface(elevation_fn)
        time = solar_time(timestr = timestr,
                          lon = tif.get_centre()['lon'])

        fns = _upload_to_grass(wdir = wdir,
                               elevation = elevation_fn,
                               aspect = aspect_fn,
                               slope = slope_fn,
                               linke = linke_fn,
                               albedo = albedo_fn,
                               coeff_bh = coeff_bh_fn,
                               coeff_dh = coeff_dh_fn)

        ofns = _compute_irradiance(wdir = wdir,
                                   solar_time = time,
                                   aspect_value = aspect_value,
                                   slope_value = slope_value,
                                   linke_value = linke_value,
                                   albedo_value = albedo_value,
                                   **fns,
                                   njobs = GRASS_NJOBS,
                                   npartitions = 1)

        _combine_results(wdir = wdir,
                         ofn = ofn,
                         grass_outputs = ofns)
    except Exception as e:
        remove_file(ofn)
        raise e
    finally:
        shutil.rmtree(wdir)

    return ofn


def irradiance(timestr, rsun_args, **kwargs):
    """Start irradiance job

    :timestr: time string, format: %Y-%m-%d_%H:%M:%S

    :kwargs: arguments passed to sample_raster

    """
    kwargs['output_type'] = 'geotiff'
    tasks = sample_raster(**kwargs)

    args = {'timestr': timestr}
    args.update(rsun_args)

    tasks |= rsun_irradiance.signature((),args)

    return tasks
