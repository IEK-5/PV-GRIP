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

    /api/status          print current active and scheduled jobs

    """

def lookup_defaults():
    return {'location': \
            ([50.87,6.12],
             '[latitude, longitude] of the desired location'),
            'data_re': \
            (r'.*',
             'regular expression matching the dataset'),
            'stat': \
            ('max',
             """
             statistic to compute for LAS files

             choice: ("max","min","mean","idw","count","stdev")""")}


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
        'timestr', where every time is given in the format

        %Y-%m-%d_%H:%M:%S
        e.g.
        2020-07-01_06:00:00
        """)})
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
    return {'path': \
            ('NA',
             """a storage path for a file to download
             """)}


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
    else:
        return None

    return res
