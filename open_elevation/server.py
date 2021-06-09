import os
import json
import bottle
import logging
import traceback

from open_elevation.globals \
    import LOGGING_LEVEL

logging.basicConfig(filename = 'data/server.log',
                    level = LOGGING_LEVEL,
                    format = "[%(asctime)s] %(levelname)s: %(filename)s::%(funcName)s %(message)s")

from celery.exceptions import TimeoutError

import open_elevation.celery_tasks as tasks

from open_elevation.globals \
    import get_SPATIAL_DATA, PVGRIP_CONFIGS

from open_elevation.utils \
    import TASK_RUNNING

from open_elevation.cache_fn_results \
    import cache_fn_results

from open_elevation.cassandra_path \
    import Cassandra_Path, is_cassandra_path

from open_elevation.upload \
    import upload


def _return_exception(e):
    return {'results':
            {'error': type(e).__name__ + ": " + str(e),
             'traceback': traceback.format_exc()}}


def _parse_args(data, defaults):
    res = {}

    for key, _ in data.items():
        if key not in defaults:
            raise RuntimeError("Unknown argument: '%s'" % key)


    for key, item in defaults.items():
        item = item[0]
        if key not in data:
            res[key] = item
        else:
            new = data[key]
            if type(item) != type(new) \
               and isinstance(new, str) \
               and isinstance(item, (list, dict)):
                new = json.loads(new)
            res[key] = type(item)(new)

    return res


def _serve(data):
    if isinstance(data, dict):
        return data

    if is_cassandra_path(data):
        with open(Cassandra_Path(data).get_locally(),
                  'rb') as f:
            return f.read()

    return data


def _get_job_results(job, timeout = 30):
    try:
        if 'SUCCESS' == job.state:
            fn = job.result

        if 'FAILURE' == job.state:
            raise job.result

        if 'PENDING' == job.state:
            fn = job.wait(timeout = timeout)

        return fn
    except TASK_RUNNING:
        return {'results': {'message': 'task is running'}}
    except TimeoutError:
        return {'results': {'message': 'task is running'}}
    except Exception as e:
        return _return_exception(e)


def _format_help(data):
    res = []
    for key, item in data.items():
        res += [("""%15s=%s
        %s
        """ % ((key,) + item)).lstrip()]

    return '\n'.join(res)


def _lookup_defaults():
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


def _raster_defaults():
    res = _lookup_defaults()
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


def _timestr_argument():
    return {'timestr': \
            ("2020-07-01_06:00:00",
             "UTC time of the shadow")}


def _shadow_defaults():
    res = _raster_defaults()
    res.update({
        'output_type': \
        ('png',
         'type of output ("png","geotiff")'),
        'what': \
        ('shadow',
         """
         what to compute

         choices: "shadow", "incidence"

         the incidence map always produces geotiff
         shadow produces a binary shadow map""")})
    res.update(_timestr_argument())
    return res


def _osm_defaults():
    res = _raster_defaults()
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


def _irradiance_defaults():
    res = _shadow_defaults()
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


def _upload_defaults():
    return {'data': \
            ('NA',
             """a local path for a file to upload

             Must be specified as a form element.

             For example, for a local file '/files/example.txt', say
             curl -F data=@/files/example.txt <site>/api/upload
             """)}


def _ssdp_defaults():
    res = _raster_defaults()
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
    res.update(_timestr_argument())
    return res


def _route_defaults():
    res = _ssdp_defaults()
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
    return res


@cache_fn_results(link = True,
                  ignore = lambda x: isinstance(x,dict))
def _call_task(task, args):
    try:
        job = task(**args).delay()
    except Exception as e:
        return _return_exception(e)

    return _get_job_results(job)


def _call_method(args, defaults_func, run_func):
    try:
        args = _parse_args(data = args,
                           defaults = defaults_func())
    except Exception as e:
        return _return_exception(e)

    return _serve(_call_task(run_func, args))


@bottle.route('/api/help', method=['GET'])
def get_help():
    return {'results': """
    Query geo-spatial data

    /api/help            print this page
    /api/help/<what>     print help for <what>

    /api/datasets        list available datasets

    /api/upload          upload a file to a storage

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

    /api/osm             render binary images of rendered from OSM

    /api/status          print current active and scheduled jobs

    """}


@bottle.route('/api/help/<what:path>', method=['GET'])
def get_what_help(what):
    if 'raster' == what:
        res = _raster_defaults()
    elif 'shadow' == what:
        res = _shadow_defaults()
    elif 'irradiance' == what:
        res = _ssdp_defaults()
    elif 'irradiance/ssdp' == what:
        res = _ssdp_defaults()
    elif 'irradiance/grass' == what:
        res = _irradiance_defaults()
    elif 'osm' == what:
        res = _osm_defaults()
    elif 'ssdp' == what:
        res = _ssdp_defaults()
    elif 'upload' == what:
        res = _upload_defaults()
    elif 'route' == what:
        res = _route_defaults()
    else:
        return error404('no help for the %s available' % what)
    return {'results': _format_help(res)}


@bottle.route('/api/<method:path>', method=['GET','POST'])
def do_method(method):
    if 'GET' == bottle.request.method:
        args = bottle.request.query
    elif 'POST' == bottle.request.method:
        args = bottle.request.json
    else:
        return error404('%s access method is not implemented' \
                        % str(bottle.request.method))

    if 'raster' == method:
        defaults = _raster_defaults
        run = tasks.sample_raster
    elif 'shadow' == method:
        defaults = _shadow_defaults
        run = tasks.shadow
    elif 'osm' == method:
        defaults = _osm_defaults
        run = tasks.osm_render
    elif 'irradiance' == method:
        defaults = _ssdp_defaults
        run = tasks.ssdp_irradiance
    elif 'irradiance/grass' == method:
        defaults = _irradiance_defaults
        run = tasks.irradiance
    elif 'irradiance/ssdp' == method:
        defaults = _ssdp_defaults
        run = tasks.ssdp_irradiance
    elif 'status' == method:
        return {'results': tasks.status()}
    elif 'datasets' == method:
        SPATIAL_DATA = get_SPATIAL_DATA()
        return {'results': SPATIAL_DATA.get_datasets()}
    elif 'upload' == method:
        try:
            data = bottle.request.files.data
        except Exception as e:
            return _return_exception(e)

        return _serve(upload(data))
    elif 'route' == method:
        if 'tsvfn_uploaded' == 'NA':
            return _return_exception\
                (RuntimeError\
                 ('tsvfn_uploaded must be provided!'))

        defaults = _route_defaults
        run = tasks.ssdp_route
    else:
        return error404('method is not implemented')

    return _call_method(args = args,
                        defaults_func = defaults,
                        run_func = run)


@bottle.error(404)
def error404(error):
    return {'results': str(error)}


bottle.run(host='0.0.0.0', port=8080,
           server='gunicorn',
           workers=int(PVGRIP_CONFIGS['server']['server_workers']),
           max_requests=int(PVGRIP_CONFIGS['server']['max_requests']),
           timeout=int(PVGRIP_CONFIGS['server']['timeout']))
