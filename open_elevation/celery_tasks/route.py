import os
import pickle
import pyproj
import celery
import shutil
import logging

import pandas as pd

from open_elevation.route \
    import get_list_rasters

from open_elevation.ssdp \
    import poa_route

from open_elevation.celery_tasks \
    import CELERY_APP

from open_elevation.cache_fn_results \
    import cache_fn_results
from open_elevation.celery_one_instance \
    import one_instance

from open_elevation.celery_tasks.sample_raster_box \
    import check_all_data_available, sample_from_box, \
    check_box_not_too_big
from open_elevation.celery_tasks.ssdp \
    import pickle2ssdp_topography, \
    timestr2utc_time, call_ssdp, \
    centre_of_box

from open_elevation.utils \
    import get_tempfile, remove_file, \
    run_command, get_tempdir, format_dictionary

from open_elevation.cassandra_path \
    import Cassandra_Path, is_cassandra_path


_T2MT = pyproj.Transformer.from_crs(4326, 3857,
                                    always_xy=True)


def _convert_to_metric(lon, lat):
    return _T2MT.transform(lon, lat)


def write_locations(route,
                    ghi_default, dhi_default,
                    time_default, locations_fn):
    with open(locations_fn, 'w') as f:
        for x in route:
            if 'longitude' not in x or 'latitude' not in x:
                raise RuntimeError\
                    ("longitude or latitude is missing!")

            lon_met, lat_met = \
                _convert_to_metric(x['longitude'], x['latitude'])

            if 'dhi' not in x:
                x['dhi'] = dhi_default

            if 'ghi' not in x:
                x['ghi'] = ghi_default

            if 'timestr' not in x:
                x['utc_time'] = timestr2utc_time(time_default)
            else:
                x['utc_time'] = timestr2utc_time(x['timestr'])

            fmt = '\t'.join(('%.12f',)*4 + ('%d\n',))

            f.write(fmt %(lat_met, lon_met, x['ghi'],x['dhi'],x['utc_time']))

    return route


def write_result(route, ssdp_ofn, ofn):
    df_a = pd.DataFrame(route)
    df_b = pd.read_csv(ssdp_ofn, header=None)
    df_b.columns = ['POA']
    df_c = pd.concat([df_a.reset_index(drop=True), df_b], axis=1)
    return df_c.to_csv(ofn, sep='\t', index=False)


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 60*10)
def compute_route(ifn, route, lat, lon,
                  ghi_default, dhi_default,
                  time_default, albedo, nsky):
    logging.debug("compute_route\n{}"\
                  .format(format_dictionary(locals())))
    wdir = get_tempdir()
    ofn = get_tempfile()

    ssdp_ifn = os.path.join(wdir, 'ssdp_ifn')
    ssdp_ofn = os.path.join(wdir, 'ssdp_ofn')
    route_fn = os.path.join(wdir, 'route_fn')
    locations_fn = os.path.join(wdir, 'locations_fn')

    try:
        ssdp_ifn, data, grid = \
            pickle2ssdp_topography(ifn, ssdp_ifn)

        route = write_locations(route = route,
                                ghi_default = ghi_default,
                                dhi_default = dhi_default,
                                time_default = time_default,
                                locations_fn = locations_fn)

        call = poa_route\
            (topography_fname = ssdp_ifn,
             albedo = albedo,
             nsky = nsky,
             ofn = ssdp_ofn,
             locations_fn = locations_fn,
             grid = grid,
             lat = lat,
             lon = lon)

        call_ssdp(call)
        write_result(route = route,
                     ssdp_ofn = ssdp_ofn,
                     ofn = ofn)
        return ofn
    except Exception as e:
        remove_file(ofn)
        raise e
    finally:
        shutil.rmtree(wdir)


@CELERY_APP.task()
@cache_fn_results()
@one_instance(expire = 10)
def merge_tsv(tsv_files):
    logging.debug("merge_tsv\n{}"\
                  .format(format_dictionary(locals())))
    ofn = get_tempfile()

    try:
         res = pd.concat([pd.read_csv(fn, sep=None, engine='python') \
                          for fn in tsv_files])
         res.to_csv(ofn, sep='\t', index=False)
    except Exception as e:
        remove_file(ofn)
        raise e

    return ofn


def ssdp_route(tsvfn_uploaded, box, box_delta,
               dhi, ghi, albedo, timestr, nsky, **kwargs):
    kwargs['output_type'] = 'pickle'
    kwargs['mesh_type'] = 'metric'

    rasters_fn = get_list_rasters(route_fn = tsvfn_uploaded,
                                  box = box,
                                  box_delta = box_delta)
    with open(Cassandra_Path(rasters_fn)\
              .get_locally(),'rb') as f:
        rasters = pickle.load(f)

    check_box_not_too_big(box = rasters[0]['box'],
                          step = kwargs['step'],
                          mesh_type = kwargs['mesh_type'])

    tasks = check_all_data_available\
        (rasters = rasters,
         data_re = kwargs['data_re'],
         stat = kwargs['stat'])
    group = []
    for x in rasters:
        lon, lat = centre_of_box(x['box'])
        group += \
            [sample_from_box.signature\
             (kwargs = {'box': x['box'],
                        'data_re': kwargs['data_re'],
                        'stat': kwargs['stat'],
                        'mesh_type': kwargs['mesh_type'],
                        'step': kwargs['step']},
              immutable = True) | \
             compute_route.signature\
             (kwargs = \
              {'route': x['route'],
               'lat': lat,
               'lon': lon,
               'ghi_default': ghi,
               'dhi_default': dhi,
               'time_default': timestr,
               'albedo': albedo,
               'nsky': nsky})]
    tasks |= celery.group(group)
    tasks |= merge_tsv.signature()

    return tasks
