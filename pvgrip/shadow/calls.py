from pvgrip.shadow.tasks \
    import compute_incidence, compute_shadow_map

from pvgrip.raster.calls \
    import sample_raster, convert_from_to


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
    tasks = sample_raster(**kwargs)

    tasks |= compute_incidence.signature\
        ((),{'timestr': timestr})

    if 'shadow' == what:
        tasks |= compute_shadow_map.signature()

    return convert_from_to(tasks,
                           from_type = 'geotiff',
                           to_type = output_type)

