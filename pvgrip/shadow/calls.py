from pvgrip.shadow.tasks \
    import compute_incidence, compute_shadow_map

from pvgrip.raster.calls \
    import sample_raster
from pvgrip.raster.tasks \
    import save_binary_png


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
