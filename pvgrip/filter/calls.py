import celery

from pvgrip.utils.cache_fn_results \
    import call_cache_fn_results

from pvgrip.raster.calls \
    import sample_raster, convert_from_to
from pvgrip.raster.tasks \
    import resample_from_pickle

from pvgrip.filter.tasks \
    import stdev


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
