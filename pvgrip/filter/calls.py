import celery

import numpy as np

from pvgrip.utils.cache_fn_results \
    import call_cache_fn_results

from pvgrip.raster.calls \
    import sample_raster, convert_from_to
from pvgrip.raster.tasks \
    import resample_from_pickle, sample_from_box, \
    sample_route_neighbour

from pvgrip.route.calls \
    import route_rasters, save_route
from pvgrip.route.tasks \
    import merge_tsv
from pvgrip.route.split_route \
    import split_route_calls

from pvgrip.filter.tasks \
    import stdev, apply_filter


@call_cache_fn_results()
def lidar_stdev(filter_size, **kwargs):
    output_type = kwargs['output_type']
    step = kwargs['step']
    kwargs['step'] = kwargs['pdal_resolution']
    kwargs['output_type'] = 'pickle'
    kwargs['mesh_type'] = 'metric'

    tasks = celery.group(
        sample_raster(stat='stdev', ensure_las=True, **kwargs),
        sample_raster(stat='mean', ensure_las=True, **kwargs),
        sample_raster(stat='count', ensure_las=True, **kwargs))

    tasks |= stdev.signature\
        (kwargs = {'filter_size': filter_size})
    tasks |= resample_from_pickle.signature\
        (kwargs = {'new_step': step})

    return convert_from_to(tasks,
                           from_type='pickle',
                           to_type = output_type)


@call_cache_fn_results()
def filter_raster(filter_type, filter_size, **kwargs):
    output_type = kwargs['output_type']
    kwargs['output_type'] = 'pickle'
    kwargs['mesh_type'] = 'metric'

    tasks = sample_raster(**kwargs)
    tasks |= apply_filter.signature\
        (kwargs = {'filter_type': filter_type,
                   'filter_size': filter_size})

    return convert_from_to(tasks,
                           from_type='pickle',
                           to_type = output_type)


def _check_filtersize_boxsize(box, filter_size,
                              neighbour_step, azimuth):
    if 4 != len(box):
        raise RuntimeError("len(box) = {} != 4".format(len(box)))
    if 2 != len(filter_size):
        raise RuntimeError("len(filter_size) = {} != 2"\
                           .format(len(filter_size)))
    if 2 != len(neighbour_step):
        raise RuntimeError("len(neighbour_step) = {} != 2"\
                           .format(len(neighbour_step)))

    box_size = [box[2],box[3]]
    size = [filter_size[i] + neighbour_step[i] \
            for i in range(0, 2)]
    azimuth = np.deg2rad(azimuth)
    cos = np.cos(azimuth)
    sin = np.sin(azimuth)
    size =(size[0]*cos - size[1]*sin,
           size[0]*sin + size[1]*cos)

    if box_size[0] < size[0] or \
       box_size[1] < size[1] or \
       box_size[0] < filter_size[0] or \
       box_size[1] < filter_size[1]:
        raise RuntimeError("box = {} is too small"\
                           .format(box))


@split_route_calls(
    fn_arg = 'tsvfn_uploaded',
    hows = ("region_hash","month","week","date"),
    hash_length = 4,
    maxnrows = 10000)
@call_cache_fn_results()
def lidar_stdev_route(tsvfn_uploaded, filter_size,
                      neighbour_step, azimuth, **kwargs):
    _check_filtersize_boxsize(box = kwargs['box'],
                              filter_size = filter_size,
                              neighbour_step = neighbour_step,
                              azimuth = azimuth)
    kwargs['ensure_las'] = True
    kwargs['mesh_type'] = 'metric'

    rasters = []
    tasks = []
    for stat in ('stdev','mean','count'):
        kwargs['stat'] = stat
        x, y = route_rasters\
            (tsvfn_uploaded = tsvfn_uploaded, **kwargs)
        tasks += [x]
        if not len(rasters):
            rasters = y
    tasks = celery.group(tasks)

    prefix='lidarstdev_filtersize{}_neighbour{}_azimuth{}'\
        .format(filter_size, neighbour_step, azimuth)
    group = []
    for x in rasters:
        route_fn = save_route(x['route'])

        sample = [sample_from_box.signature\
                  (kwargs = {'box': x['box'],
                             'data_re': kwargs['data_re'],
                             'stat': stat,
                             'mesh_type': 'metric',
                             'step': kwargs['pdal_resolution'],
                             'pdal_resolution': kwargs['pdal_resolution'],
                             'ensure_las': kwargs['ensure_las']},
                   immutable = True) \
                  for stat in ('stdev','mean','count')]
        group += \
            [celery.group(sample) | \
             stdev.signature\
             (kwargs = {'filter_size': filter_size}) | \
             sample_route_neighbour.signature\
             (kwargs = {'route_fn': route_fn,
                        'azimuth_default': azimuth,
                        'neighbour_step': neighbour_step,
                        'prefix': prefix})]
    tasks |= celery.group(group)
    tasks |= merge_tsv.signature()

    return tasks


@split_route_calls(
    fn_arg = 'tsvfn_uploaded',
    hows = ("region_hash","month","week","date"),
    hash_length = 4,
    maxnrows = 10000)
@call_cache_fn_results()
def filter_raster_route(tsvfn_uploaded,
                        filter_type, filter_size,
                        neighbour_step, azimuth, **kwargs):
    kwargs['mesh_type'] = 'metric'

    _check_filtersize_boxsize(box = kwargs['box'],
                              filter_size = filter_size,
                              neighbour_step = neighbour_step,
                              azimuth = azimuth)
    tasks, rasters = route_rasters\
        (tsvfn_uploaded = tsvfn_uploaded, **kwargs)
    prefix='filter_datare{}_stat{}_{}_filtersize{}_neighbour{}_azimuth{}'\
        .format(kwargs['data_re'], kwargs['stat'],
                filter_type, filter_size,
                neighbour_step, azimuth)
    group = []
    for x in rasters:
        route_fn = save_route(x['route'])

        group += \
            [sample_from_box.signature\
             (kwargs = {'box': x['box'],
                        'data_re': kwargs['data_re'],
                        'stat': kwargs['stat'],
                        'mesh_type': kwargs['mesh_type'],
                        'step': kwargs['step'],
                        'pdal_resolution': kwargs['pdal_resolution']},
              immutable = True) | \
             apply_filter.signature\
             (kwargs = {'filter_type': filter_type,
                        'filter_size': filter_size}) | \
             sample_route_neighbour.signature\
             (kwargs = {'route_fn': route_fn,
                        'azimuth_default': azimuth,
                        'neighbour_step': neighbour_step,
                        'prefix': prefix})]
    tasks |= celery.group(group)
    tasks |= merge_tsv.signature()

    return tasks
