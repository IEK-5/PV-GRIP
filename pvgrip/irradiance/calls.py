from pvgrip.utils.cache_fn_results \
    import call_cache_fn_results

from pvgrip.raster.calls \
    import sample_raster, convert_from_to
from pvgrip.irradiance.tasks \
    import compute_irradiance_ssdp, compute_irradiance_grass

from pvgrip.ssdp.utils \
    import timestr2utc_time, centre_of_box


@call_cache_fn_results(minage=1637232422)
def irradiance_ssdp(timestr, ghi, dhi, albedo,
                    offset, azimuth, zenith,
                    nsky, **kwargs):
    """Start the irradiance

    :timestr: time string, format: %Y-%m-%d_%H:%M:%S

    :ghi: global horizontal irradiance

    :dhi: diffused horizontal irradiance

    :albedo: albedo coefficient

    :offset,azimuth,zenith: position of a tilted surface

    :nsky: number of zenith discretizations (see `man ssdp`)

    :kwargs: arguments passed to sample_raster

    """
    output_type = kwargs['output_type']
    kwargs['output_type'] = 'pickle'
    kwargs['mesh_type'] = 'metric'

    utc_time = timestr2utc_time(timestr)
    lon, lat = centre_of_box(kwargs['box'])

    tasks = sample_raster(**kwargs)
    tasks |= compute_irradiance_ssdp.signature\
        ((),{'ghi':ghi,'dhi':dhi,'nsky':nsky,
             'lon':lon,'lat':lat,'albedo':albedo,
             'offset':offset,
             'azimuth':azimuth,'zenith':zenith,
             'utc_time':utc_time})

    return convert_from_to(tasks,
                           from_type = 'pickle',
                           to_type = output_type)


@call_cache_fn_results()
def irradiance_grass(timestr, rsun_args, **kwargs):
    """Start irradiance job

    :timestr: time string, format: %Y-%m-%d_%H:%M:%S

    :kwargs: arguments passed to sample_raster

    """
    kwargs['output_type'] = 'geotiff'
    tasks = sample_raster(**kwargs)

    args = {'timestr': timestr}
    args.update(rsun_args)

    tasks |= compute_irradiance_grass.signature((),args)

    return tasks
