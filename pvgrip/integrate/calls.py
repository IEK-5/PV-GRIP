from pvgrip.utils.cache_fn_results \
    import call_cache_fn_results

from pvgrip.raster.calls \
    import sample_raster, convert_from_to

from pvgrip.integrate.tasks \
    import integrate_irradiance
from pvgrip.integrate.split_irrtimes \
    import split_irrtimes_calls

from pvgrip.ssdp.utils \
    import centre_of_box


@split_irrtimes_calls(
    fn_arg = 'tsvfn_uploaded',
    maxnrows = 300000,
    scale_name = 'Wh/m^2',
    scale_constant = 3600)
@call_cache_fn_results(minage=1647003564)
def ssdp_integrate(tsvfn_uploaded, albedo,
                   offset, azimuth, zenith,
                   nsky, **kwargs):
    output_type = kwargs['output_type']
    kwargs['output_type'] = 'pickle'
    kwargs['mesh_type'] = 'metric'

    lon, lat = centre_of_box(kwargs['box'])

    tasks = sample_raster(**kwargs)
    tasks |= integrate_irradiance.signature\
        ((),{'times_fn': tsvfn_uploaded,
             'albedo': albedo,
             'offset': offset,
             'azimuth': azimuth,
             'zenith': zenith,
             'nsky': nsky,
             'lon': lon,
             'lat': lat})

    return convert_from_to(tasks,
                           from_type = 'pickle',
                           to_type = output_type)
