import inspect


def _call_matching(func, kwargs):
    """Call function by passing matching keyword arguments

    :func: a function to call

    :kwargs: dictionary

    """
    args = inspect.getfullargspec(func).args
    args = {k:v \
            for k,v in kwargs.items() \
            if k in args}
    return func(**args)


def _set_time(utc_time):
    return """

    # set time
    make_scalar x=t val={utc_time}
    """.format(utc_time = utc_time)


def _set_coord(lat, lon, elevation=0):
    return """

    # set location
    make_scalar x=LAT val={lat}
    make_scalar x=LON val={lon}
    make_scalar x=ELEVATION val={elevation}

    deg2rad x=LAT
    deg2rad x=LON
    config_coord C=C lat=LAT lon=LON E=ELEVATION
    """.format(lat = lat, lon = lon, elevation = elevation)


def _set_irradiance(ghi, dhi):
    return """

    # set irradiance
    make_scalar x=GHI val={ghi}
    make_scalar x=DHI val={dhi}
    """.format(ghi = ghi, dhi = dhi)


def _set_sky(nsky = 20):
    return """

    # setup the sky
    config_sky N={nsky} C=C
    """.format(nsky = nsky)


def _import_topography(topography_fname, grid):
    nlat, nlon, lat1, lon1, lat2, lon2 = grid

    return """

    # read a topography
    read_array a0=z file={topography_fname}

    # import the topography in the configuration variable
    config_topogrid C=C z=z Ny={nlat} Nx={nlon} y1={lat1} x1={lon1} y2={lat2} x2={lon2}
    """.format(topography_fname = topography_fname,
               nlat = nlat, nlon = nlon,
               lat1 = lat1, lon1 = lon1,
               lat2 = lat2, lon2 = lon2)


def _config_location_time_irradiance(locations_fn, albedo):
    return """

    # read locations / time
    read_array a0=LAT a1=LON a2=GHI a3=DHI a4=AZI a5=ZEN a6=OFFSET a7=TIME file={fn}

    deg2rad x=AZI
    deg2rad x=ZEN

    # sample the topography to obtain the z values and the surface normal
    # corresponding to our mesh coordinates
    sample_topo C=C y=LAT x=LON z=Z azimuth=azi zenith=zen

    # offset topography
    offset_topo C=C o=OFFSET x=LON y=LAT xoff=offLON yoff=offLAT zoff=offZ

    # rotate POA to surface
    rotate_POA_to_surface surf_a=azi surf_z=zen poa_a=AZI poa_z=ZEN out_a=offAZI out_z=offZEN

    # setup the locations in the configuration variable
    # (note the POA is the topography surface)
    config_locations C=C y=offLAT x=offLON z=offZ azimuth=offAZI zenith=offZEN albedo={albedo}
    """.format(fn = locations_fn, albedo = albedo)


def _sim_static(ofn, pressure = 1010, temperature = 10):
    return """

    make_scalar x=PRESSURE    val={pressure}
    make_scalar x=TEMPERATURE val={temperature}

    # compute irradiance for the whole raster
    sim_static C=C t=t p=PRESSURE T=TEMPERATURE GHI=GHI DHI=DHI POA=P
    write_array a0=P file={ofn}
    """.format(ofn = ofn, pressure = pressure, temperature = temperature)


def _sim_route(ofn, pressure = 1010, temperature = 10):
    return """

    make_scalar x=PRESSURE    val={pressure}
    make_scalar x=TEMPERATURE val={temperature}

    # compute irradiance along a route
    sim_route C=C t=TIME p=PRESSURE T=TEMPERATURE GHI=GHI DHI=DHI POA=P
    write_array a0=P file={ofn}
    """.format(ofn = ofn, pressure = pressure, temperature = temperature)


def _config_time_irradiance(irrtimes):
    return """

    # read times
    read_array a0=TIMEs a1=GHIs a2=DHIs file={irrtimes}
    """.format(irrtimes = irrtimes)


def _sim_static_integral(ofn, pressure = 1010, temperature = 10):
    return """

    make_scalar x=PRESSURE    val={pressure}
    make_scalar x=TEMPERATURE val={temperature}

    # compute the integrated POA
    sim_static_integral C=C t=TIMEs p=PRESSURE T=TEMPERATURE GHI=GHIs DHI=DHIs POA=P
    write_array a0=P file={ofn}
    """.format(ofn = ofn, pressure = pressure, temperature = temperature)


def _sample_topogrid_locations(albedo, offset, azimuth, zenith):
    return """

    # get coordinates from config_topogrid
    get_grid C=C x=X y=Y

    make_scalar x=AZI val={azimuth}
    make_scalar x=ZEN val={zenith}
    make_scalar x=OFFSET val={offset}

    deg2rad x=AZI
    deg2rad x=ZEN

    # sample the topography to obtain the z values and the surface normal
    # corresponding to our mesh coordinates
    sample_topo C=C x=X y=Y z=Z azimuth=azi zenith=zen

    # offset topography
    offset_topo C=C o=OFFSET x=X y=Y xoff=offX yoff=offY zoff=offZ

    # rotate POA to surface
    rotate_POA_to_surface surf_a=azi surf_z=zen poa_a=AZI poa_z=ZEN out_a=offAZI out_z=offZEN

    # setup the locations in the configuration variable
    # (note the POA is the topography surface)
    config_locations C=C x=offX y=offY z=offZ azimuth=offAZI zenith=offZEN albedo={albedo}
    """.format(albedo=albedo, offset=offset, azimuth=azimuth, zenith=zenith)


def _init_config():
    return """

    # initialize simulation config variable
    init_sim_config C=C
    """


def _strip(text):
    return '\n'.join([x.strip() for x in text.split('\n')])



def poa_raster(**kwargs):
    call  = _call_matching(_init_config, kwargs)
    call += _call_matching(_set_sky, kwargs)
    call += _call_matching(_set_irradiance, kwargs)
    call += _call_matching(_set_time, kwargs)
    call += _call_matching(_set_coord, kwargs)
    call += _call_matching(_import_topography, kwargs)
    call += _call_matching(_sample_topogrid_locations, kwargs)
    call += _call_matching(_sim_static, kwargs)

    return _strip(call)


def poa_route(**kwargs):
    call  = _call_matching(_init_config, kwargs)
    call += _call_matching(_set_sky, kwargs)
    call += _call_matching(_set_coord, kwargs)
    call += _call_matching(_import_topography, kwargs)
    call += _call_matching(_config_location_time_irradiance,
                          kwargs)
    call += _call_matching(_sim_route, kwargs)

    return _strip(call)


def poa_integrate(**kwargs):
    call  = _call_matching(_init_config, kwargs)
    call += _call_matching(_set_sky, kwargs)
    call += _call_matching(_set_coord, kwargs)
    call += _call_matching(_import_topography, kwargs)
    call += _call_matching(_sample_topogrid_locations, kwargs)
    call += _call_matching(_config_time_irradiance, kwargs)
    call += _call_matching(_sim_static_integral, kwargs)

    return _strip(call)
