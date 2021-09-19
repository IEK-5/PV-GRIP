def calls_help():
    return """
    Query geo-spatial data

    /api/help            print this page
    /api/help/<what>     print help for <what>

    /api/datasets        list available datasets

    /api/upload          upload a file to the storage
    /api/download        upload a file from the storage

    /api/raster          get a raster image of a region

    /api/irradiance      compute irradiance raster map.

        Several library bindings are available (default ssdp):
        /api/irradiance/ssdp
                         use the SSDP library to compute irradiance maps

        /api/irradiance/grass
                         use the GRASS library

    /api/route           compute irradiance along a route

    /api/intergrate      integrate irradiance map over a period of time

    /api/shadow          compute binary map of a shadow at a time of a region

        /api/shadow/average
                         compute average value of shadows over given times

    /api/osm             render binary images of rendered from OSM

    /api/weather         get various weather data

        /api/weather/irradiance/{route,box}
                         get irradiance values for a route or region
        /api/weather/reanalysis
                         get various reanalysis data

    /api/status          print current active and scheduled jobs

    """


def global_defaults():
    return {'serve_type': \
            ('file',
             """
             type of output to serve

             choice: ("file","path","ipfs_cid")

              - "file": webserver sends back a file
              - "path": webserver sends back a pvgrip path
                    a file can be downloaded with /download
              - "ipfs_cid": webserver sends back an ipfs_cid
                    a file can be downloaded through ipfs""")}


def lookup_defaults():
    res = global_defaults()
    res.update({'location': \
                ([50.87,6.12],
                 '[latitude, longitude] of the desired location'),
                'data_re': \
                (r'.*',
                 'regular expression matching the dataset'),
                'stat': \
                ('max',
                 """
                 statistic to compute for LAS files

                 choice: ("max","min","mean","idw","count","stdev")""")})
    return res


def raster_defaults():
    res = lookup_defaults()
    del res['location']
    res.update({'box': \
                ([50.865,7.119,50.867,7.121],
                 """
                 bounding box of desired locations

                 format: [lat_min, lon_min, lat_max, lon_max]"""),
                'step': \
                (float(1),
                 'resolution of the sampling mesh in meters'),
                'mesh_type': \
                ('metric',
                 'type of mesh_type ("metric","wgs84")'),
                'output_type': \
                ('pickle',
                 """
                 type of output

                 choices: "pickle","geotiff","pnghillshade","png","pngnormalize"

                 "png" does not normalise data""")})
    return res


def timestr_argument():
    return {'timestr': \
            ("2020-07-01_06:00:00",
             "UTC time of the shadow")}


def shadow_defaults():
    res = raster_defaults()
    res.update({
        'what': \
        ('shadow',
         """
         what to compute

         choices: "shadow", "incidence"

         the incidence map always produces geotiff
         shadow produces a binary shadow map""")})
    res.update(timestr_argument())
    return res


def shadow_average_defaults():
    res = raster_defaults()
    res.update({
        'timestrs_fn': \
        ('NA',
        """a pvgrip path resulted from /api/upload

        The tsv file must contain a header with at least
        'timestr', where every time is given in the format.

        %Y-%m-%d_%H:%M:%S
        e.g.
        2020-07-01_06:00:00

        Note, in case csv contains a single column, wrap the column
        name in quotes, otherwise this might happen:
        https://github.com/pandas-dev/pandas/issues/17333""")})
    return res


def osm_defaults():
    res = raster_defaults()
    del res['stat']
    del res['data_re']
    res.update({
        'tag': \
        ('building',
         """type of an OpenStreetMap tag to show

         e.g. building, highway

         See more info on available tags in OSM:
         https://wiki.openstreetmap.org/wiki/Map_features

         tag can be in format: key=value

         e.g. landuse=forest
         """)
    })
    return res


def irradiance_defaults():
    res = shadow_defaults()
    del res['what']
    del res['output_type']
    res.update({
        'rsun_args': \
        ({'aspect_value': 270,
          'slope_value': 0},
         """Arguments passed to r.sun.

         Raster arguments are not currently supported""")
    })
    return res


def upload_defaults():
    return {'data': \
            ('NA',
             """a local path for a file to upload

             Must be specified as a form element.

             For example, for a local file '/files/example.txt', say
             curl -F data=@/files/example.txt <site>/api/upload
             """)}


def download_defaults():
    res = global_defaults()
    res.update({
        'path': \
        ('NA',
         """a storage path for a file to download
         """)})
    return res


def ssdp_defaults():
    res = raster_defaults()
    res.update({'ghi': \
                (1000,
                 "Global horizontal irradiance"),
                'dhi': \
                (100,
                 "Diffused horizontal irradiance"),
                'albedo':
                (0.5,
                 "albedo value between 0-1"),
                'nsky':
                (10,
                 """The  number  of  zenith  discretizations.

                 The  total number of sky patches equals
                 Ntotal(N)=3N(N-1)+1,
                 e.g. with Ntotal(7)=127

                 Based on POA simulations without topography: 7 is
                 good enough, 10 noticeably better. Beyond that it is
                 3rd significant digit improvements.
                 """)})
    res.update(timestr_argument())
    return res


def route_defaults():
    res = ssdp_defaults()
    res.update({'box': \
                ([-50,-50,50,50],
                 """
                 bounding box steps around each location in meters

                 format: [east,south,west,north]

                 """),
                'box_delta': \
                (3,
                 """a constant that defines a maximum raster being sampled

                 For example, if a 'box' is a rectangle with sizes
                 a x b, then the maximum sized raster being sampled has
                 dimensions: a*2*box_delta x b*2*box_delta.

                 The constant is replaced with max(1,box_delta).

                 """),
                'tsvfn_uploaded': \
                ('NA',
                 """a pvgrip path resulted from /api/upload

                 The tsv file must contain a header with at least
                 'latitude' and 'longitude' columns.

                 The following columns are optional: 'ghi', 'dhi',
                 'timestr'. In case some of the columns are missing, a
                 constant value for all locations is used.

                 """)})
    del res['output_type']
    del res['mesh_type']
    return res


def integrate_defaults():
    res = ssdp_defaults()

    res.update({'tsvfn_uploaded': \
                ('NA',
                 """a pvgrip path resulted from /api/upload

                 The tsv file must contain a header with 'timestr',
                 'ghi' and 'dhi' columns.

                 """)})
    del res['ghi']
    del res['dhi']
    del res['timestr']
    del res['mesh_type']
    return res


def _irradiance_options():
    return {
        'what': \
        (['GHI','DHI'],
         """what to get

         Options are:

         - 'TOA' Irradiation on horizontal plane at the top of
            atmosphere (W/m2)

          - 'Clear sky GHI' Clear sky global irradiation on horizontal
            plane at ground level (W/m2)

          - 'Clear sky BHI' Clear sky beam irradiation on horizontal
            plane at ground level (W/m2)

          - 'Clear sky DHI' Clear sky diffuse irradiation on
            horizontal plane at ground level (W/m2)

          - 'Clear sky BNI' Clear sky beam irradiation on mobile plane
            following the sun at normal incidence (W/m2)

          - 'GHI' Global irradiation on horizontal plane at ground
            level (W/m2)

          - 'BHI' Beam irradiation on horizontal plane at ground level
            (W/m2)

          - 'DHI' Diffuse irradiation on horizontal plane at ground
            level (W/m2)

          - 'BNI' Beam irradiation on mobile plane following the sun
            at normal incidence (W/m2)

          - 'Reliability' Proportion of reliable data in the
            summarization (0-1)
         """)}


def weather_irradiance_route():
    res = global_defaults()
    res.update(_irradiance_options())
    res.update({
        'tsvfn_uploaded': \
        ('NA',
         """a pvgrip path resulted from /api/upload

         The tsv file must contain a header with at least
         'latitude', 'longitude' and 'timestr' columns.
         """)})
    return res


def weather_irradiance_box():
    res = global_defaults()
    res.update(_irradiance_options())
    res.update({
        'box': \
        ([50.865,7.119,50.867,7.121],
         """
         bounding box of desired locations

         format: [lat_min, lon_min, lat_max, lon_max]"""),
        'time_range': \
        ('2019-07-01_10:00:00/2019-07-01_11:00:00',
         """a string specifying a time range in UTC

         It can be either in the format:
         '%Y-%m-%d_%H:%M:%S/%Y-%m-%d_%H:%M:%S'
         specifying a true range, or
         '%Y-%m-%d_%H:%M:%S'
         specifying a single time
         """),
        'time_step': \
        ('20minutes',
         """a string specifying a time step

         Format: '<integer><units>',
         where unit is second, minute, hour or day (or plural)
         """)})
    return res


def call_defaults(method):
    if 'raster' == method:
        res = raster_defaults()
    elif 'shadow' == method:
        res = shadow_defaults()
    elif 'shadow/average' == method:
        res = shadow_average_defaults()
    elif 'irradiance' == method:
        res = ssdp_defaults()
    elif 'irradiance/ssdp' == method:
        res = ssdp_defaults()
    elif 'irradiance/grass' == method:
        res = irradiance_defaults()
    elif 'osm' == method:
        res = osm_defaults()
    elif 'ssdp' == method:
        res = ssdp_defaults()
    elif 'upload' == method:
        res = upload_defaults()
    elif 'download' == method:
        res = download_defaults()
    elif 'route' == method:
        res = route_defaults()
    elif 'integrate' == method:
        res = integrate_defaults()
    elif 'weather/irradiance' == method:
        res = weather_irradiance_box()
    elif 'weather/irradiance/box' == method:
        res = weather_irradiance_box()
    elif 'weather/irradiance/route' == method:
        res = weather_irradiance_route()
    else:
        return None

    return res
