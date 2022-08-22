import celery

import pandas as pd

from pvgrip.utils.cache_fn_results \
    import call_cache_fn_results

from pvgrip.shadow.tasks \
    import compute_incidence, compute_shadow_map, \
    average_png

from pvgrip.raster.calls \
    import sample_raster, convert_from_to
from pvgrip.raster.mesh \
    import determine_epsg

from pvgrip.storage.remotestorage_path \
    import searchandget_locally


@call_cache_fn_results(minage = 1650884152)
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

    kwargs['output_type'] = 'geotiff'
    kwargs['mesh_type'] = determine_epsg(kwargs['box'], kwargs['mesh_type'])
    tasks = sample_raster(**kwargs)

    tasks |= compute_incidence.signature\
        ((),{'timestr': timestr})

    if 'shadow' == what:
        tasks |= compute_shadow_map.signature()

    return convert_from_to(tasks,
                           from_type = 'geotiff',
                           to_type = output_type)


@call_cache_fn_results(minage = 1650884152)
def average_shadow(timestrs_fn, output_type='png', **kwargs):
    """Compute a heatmap of shadows over some times

    """
    kwargs['output_type'] = 'geotiff'
    kwargs['mesh_type'] = determine_epsg(kwargs['box'], kwargs['mesh_type'])
    tasks = sample_raster(**kwargs)

    # read timestrs
    timestrs = pd.read_csv(searchandget_locally(timestrs_fn),
                           sep=None, engine = 'python')
    timestrs.column = [x.lower() for x in timestrs.columns]
    if 'timestr' not in timestrs:
        raise RuntimeError("timestr is missing!")
    timestrs = timestrs['timestr'].to_numpy()

    group = []
    for timestr in timestrs:
        x = compute_incidence.signature\
            ((),{'timestr': timestr}) | \
            compute_shadow_map.signature()
        x = convert_from_to(x,
                            from_type = 'geotiff',
                            to_type = 'png')
        group += [x]
    tasks |= celery.group(group)
    tasks |= average_png.signature()
    tasks |= convert_from_to(tasks,
                             from_type = 'pickle',
                             to_type = output_type)

    return tasks
