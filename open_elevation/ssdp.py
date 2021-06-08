from open_elevation.utils \
    import call_matching


def _set_time(utc_time):
    return """

    # set time
    make_scalar x=t val={utc_time}
    """.format(utc_time = utc_time)


def _set_coord(lat, lon):
    return """

    # set location
    make_scalar x=LAT val={lat}
    make_scalar x=LON val={lon}
    deg2rad x=LAT
    deg2rad x=LON
    config_coord C=C lat=LAT lon=LON
    """.format(lat = lat, lon = lon)

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
    ny, nx, y1, x1, y2, x2 = grid

    return """

    # read a topography
    read_array a0=z file={topography_fname}

    # import the topography in the configuration variable
    config_topogrid C=C z=z Nx={nx} Ny={ny} x1={x1} y1={y1} x2={x2} y2={y2}
    """.format(topography_fname = topography_fname,
               nx = nx, ny = ny,
               x1 = x1, y1 = y1,
               x2 = x2, y2 = y2)


def _config_location_time_irradiance(locations_fn, albedo):
    return """

    # read locations / time
    read_array a0=LAT a1=LON a2=GHI a3=DHI a4=TIME file={fn}

    # sample the topography to obtain the z values and the surface normal
    # corresponding to our mesh coordinates
    sample_topo C=C x=LAT y=LON z=Z azimuth=azi zenith=zen

    # setup the locations in the configuration variable
    # (note the POA is the topography surface)
    config_locations C=C x=LAT y=LON z=Z azimuth=azi zenith=zen albedo={albedo}
    """.format(fn = locations_fn, albedo = albedo)


def _sim_static(ofn):
    return """

    # compute irradiance for the whole raster
    sim_static C=C t=t GHI=GHI DHI=DHI POA=P
    write_array a0=P file={ofn}
    """.format(ofn = ofn)


def _sim_route(ofn):
    return """

    # compute irradiance along a route
    sim_route C=C t=TIME GHI=GHI DHI=DHI POA=P
    write_array a0=P file={ofn}
    """.format(ofn = ofn)


def _sample_topogrid_locations(albedo):
    return """

    # get coordinates from config_topogrid
    get_grid C=C x=X y=Y

    # sample the topography to obtain the z values and the surface normal
    # corresponding to our mesh coordinates
    sample_topo C=C x=X y=Y z=Z azimuth=azi zenith=zen

    # setup the locations in the configuration variable
    # (note the POA is the topography surface)
    config_locations C=C x=X y=Y z=Z azimuth=azi zenith=zen albedo={albedo}
    """.format(albedo=albedo)


def _init_config():
    return """

    # initialize simulation config variable
    init_sim_config C=C
    """


def _strip(text):
    return '\n'.join([x.strip() for x in text.split('\n')])



def poa_raster(**kwargs):
    call  = call_matching(_init_config, kwargs)
    call += call_matching(_set_sky, kwargs)
    call += call_matching(_set_irradiance, kwargs)
    call += call_matching(_set_time, kwargs)
    call += call_matching(_set_coord, kwargs)
    call += call_matching(_import_topography, kwargs)
    call += call_matching(_sample_topogrid_locations, kwargs)
    call += call_matching(_sim_static, kwargs)

    return _strip(call)


def poa_route(**kwargs):
    call  = call_matching(_init_config, kwargs)
    call += call_matching(_set_sky, kwargs)
    call += call_matching(_set_coord, kwargs)
    call += call_matching(_import_topography, kwargs)
    call += call_matching(_config_location_time_irradiance,
                          kwargs)
    call += call_matching(_sim_route, kwargs)

    return _strip(call)