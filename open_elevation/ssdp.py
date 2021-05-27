from open_elevation.utils \
    import call_matching


def _set_time(utc_time, lat, lon):
    return """
    # set time
    make_scalar x=t val={utc_time}

    # set location
    make_scalar x=LAT val={lat}
    make_scalar x=LON val={lon}
    deg2rad x=LAT
    deg2rad x=LON
    config_coord C=C lat=LAT lon=LON
    """.format(utc_time = utc_time,
               lat = lat, lon = lon).lstrip()


def _set_irradiance(ghi, dhi):
    return """
    # set irradiance
    make_scalar x=GHI val={ghi}
    make_scalar x=DHI val={dhi}
    """.format(ghi = ghi, dhi = dhi).lstrip()


def _set_sky(nsky = 20):
    return """
    # setup the sky
    config_sky N={nsky} C=C
    """.format(nsky = nsky).lstrip()


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
               x2 = x2, y2 = y2).lstrip()


def _sim_static(ofn):
    return """
    sim_static C=C t=t GHI=GHI DHI=DHI POA=P
    write_array a0=P file={ofn}
    """.format(ofn = ofn).lstrip()


def _sample_locations(albedo):
    return """
    # get coordinates from config_topogrid
    get_grid C=C x=X y=Y

    # sample the topography to obtain the z values and the surface normal
    # corresponding to our mesh coordinates
    sample_topo C=C x=X y=Y z=Z azimuth=azi zenith=zen

    # setup the locations in the configuration variable
    # (note the POA is the topography surface)
    config_locations C=C x=X y=Y z=Z azimuth=azi zenith=zen albedo={albedo}
    """.format(albedo=albedo).lstrip()


def _init_config():
    return """
    # initialize simulation config variable
    init_sim_config C=C
    """.lstrip()


def poa_raster(**kwargs):
    call  = call_matching(_init_config, kwargs)
    call += call_matching(_set_sky, kwargs)
    call += call_matching(_set_irradiance, kwargs)
    call += call_matching(_set_time, kwargs)
    call += call_matching(_import_topography, kwargs)
    call += call_matching(_sample_locations, kwargs)
    call += call_matching(_sim_static, kwargs)

    return call
