import celery
import pickle

from pvgrip.utils.cache_fn_results \
    import call_cache_fn_results

from pvgrip.route.cluster_route_boxes \
    import get_list_rasters
from pvgrip.route.tasks \
    import compute_route, merge_tsv

from pvgrip.raster.calls \
    import check_all_data_available
from pvgrip.raster.tasks \
    import sample_from_box
from pvgrip.raster.utils \
    import check_box_not_too_big

from pvgrip.ssdp.utils \
    import centre_of_box

from pvgrip.storage.remotestorage_path \
    import searchandget_locally

from pvgrip.route.split_route \
    import split_route_calls


@split_route_calls(
    fn_arg = 'tsvfn_uploaded',
    hows = ("region_hash","month","week","date"),
    hash_length = 4,
    maxnrows = 10000)
@call_cache_fn_results(minage=1637232422)
def ssdp_route(tsvfn_uploaded, box, box_delta,
               dhi, ghi, albedo, timestr,
               offset, azimuth, zenith, nsky, **kwargs):
    kwargs['output_type'] = 'pickle'
    kwargs['mesh_type'] = 'metric'

    rasters_fn = get_list_rasters\
        (route_fn = searchandget_locally(tsvfn_uploaded),
         box = box,
         box_delta = box_delta)
    with open(searchandget_locally(rasters_fn),'rb') as f:
        rasters = pickle.load(f)

    check_box_not_too_big(box = rasters[0]['box'],
                          step = kwargs['step'],
                          mesh_type = kwargs['mesh_type'])

    tasks = check_all_data_available\
        (rasters = rasters,
         data_re = kwargs['data_re'])
    group = []
    for x in rasters:
        lon, lat = centre_of_box(x['box'])
        group += \
            [sample_from_box.signature\
             (kwargs = {'box': x['box'],
                        'data_re': kwargs['data_re'],
                        'stat': kwargs['stat'],
                        'mesh_type': kwargs['mesh_type'],
                        'step': kwargs['step'],
                        'pdal_resolution': kwargs['pdal_resolution']},
              immutable = True) | \
             compute_route.signature\
             (kwargs = \
              {'route': x['route'],
               'lat': lat,
               'lon': lon,
               'ghi_default': ghi,
               'dhi_default': dhi,
               'time_default': timestr,
               'offset': offset,
               'azimuth_default': azimuth,
               'zenith_default': zenith,
               'albedo': albedo,
               'nsky': nsky})]
    tasks |= celery.group(group)
    tasks |= merge_tsv.signature()

    return tasks
