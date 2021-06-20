from pvgrip.raster.calls \
    import sample_raster, convert2output_type

from pvgrip.integrate.tasks \
    import integrate_irradiance

from pvgrip.ssdp.utils \
    import centre_of_box


def ssdp_integrate(tsvfn_uploaded,
                   albedo, nsky, **kwargs):
    output_type = kwargs['output_type']
    kwargs['output_type'] = 'pickle'
    kwargs['mesh_type'] = 'metric'

    lon, lat = centre_of_box(kwargs['box'])

    tasks = sample_raster(**kwargs)
    tasks |= integrate_irradiance.signature\
        ((),{'times_fn': tsvfn_uploaded,
             'albedo': albedo,
             'nsky': nsky,
             'lon': lon,
             'lat': lat})

    return convert2output_type(tasks,
                               output_type = output_type)
