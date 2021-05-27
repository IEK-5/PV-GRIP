import os
import json
import traceback
import logging

from open_elevation.globals \
    import LOGGING_LEVEL

logging.basicConfig(filename = 'data/server.log',
                    level = LOGGING_LEVEL,
                    format = "[%(asctime)s] %(levelname)s: %(filename)s::%(funcName)s %(message)s")

from bottle import route, run, request

from celery.exceptions import TimeoutError

import open_elevation.celery_tasks as tasks
import open_elevation.celery_status as celery_status

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


def _lookup_defaults():
    return {'location': \
            ([50.87,6.12],
             '[latitude, longitude] of the desired location'),
            'data_re': \
            (r'.*',
             'regular expression matching the dataset'),
            'stat': \
            ('max',
             'statistic to compute for LAS files ("max","min","mean","idw","count","stdev")')}


def _raster_defaults():
    res = _lookup_defaults()
    del res['location']
    res.update({'box': \
                ([50.865,7.119,50.867,7.121],
                 'bounding box of desired locations, [lat_min, lon_min, lat_max, lon_max]'),
                'step': \
                (float(1),
                 'resolution of the sampling mesh in meters'),
                'mesh_type': \
                ('metric',
                 'type of mesh_type ("metric","wgs84")'),
                'output_type': \
                ('pickle',
                 'type of output ("pickle","geotiff","pnghillshade","png","pngnormalize").' +\
                 '"png" does not normalise data')})
    return res


def _shadow_defaults():
    res = _raster_defaults()
    res.update({
        'output_type': \
        ('png',
         'type of output ("png","geotiff")'),
        'what': \
        ('shadow',
         'what to compute: either incidence map (always geotiff) or binary shadow map'),
        'timestr': \
        ("2020-07-01_06:00:00",
         "UTC time of the shadow")})
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
             curl -F file=@/files/example.txt <site>/api/upload
             """)}


def _format_help(data):
    res = []
    for key, item in data.items():
        res += ["""%15s=%s
        %s
        """ % ((key,) + item)]

    return '\n'.join(res)


@route('/api/help', method=['GET'])
def get_help():
    return {'results': """
    Query geo-spatial data

    /api/help            print help
    /api/datasets        list available datasets
    /api/raster          download a raster image of a region
    /api/shadow          compute a shadow at a time of a region
    /api/osm             render binary images of rendered from OSM
    /api/irradiance      compute irradiance rasters
    /api/status          print current active and scheduled jobs
    /api/<what>/help     print help for <what>
    """}


@route('/api/datasets', method=['GET'])
def get_datasets():
    SPATIAL_DATA = get_SPATIAL_DATA()
    return {'results': SPATIAL_DATA.get_datasets()}


@route('/api/status', method=['GET'])
def get_status():
    return {'results': celery_status.status()}


@cache_fn_results(link = True,
                  ignore = lambda x: isinstance(x,dict))
def _raster(args):
    try:
        job = tasks.sample_raster(**args).delay()
    except Exception as e:
        return _return_exception(e)

    return _get_job_results(job)


@cache_fn_results(link = True,
                  ignore = lambda x: isinstance(x,dict))
def _shadow(args):
    try:
        job = tasks.shadow(**args).delay()
    except Exception as e:
        return _return_exception(e)

    return _get_job_results(job)


@cache_fn_results(link = True,
                  ignore = lambda x: isinstance(x,dict))
def _osm(args):
    try:
        job = tasks.osm_render(**args).delay()
    except Exception as e:
        return _return_exception(e)

    return _get_job_results(job)


@cache_fn_results(link = True,
                  ignore = lambda x: isinstance(x,dict))
def _irradiance(args):
    try:
        job = tasks.irradiance(**args).delay()
    except Exception as e:
        return _return_exception(e)

    return _get_job_results(job)


@route('/api/raster', method=['GET'])
def get_raster():
    try:
        args = _parse_args(data = request.query,
                           defaults = _raster_defaults())
    except Exception as e:
        return _return_exception(e)

    return _serve(_raster(args))


@route('/api/raster', method=['POST'])
def post_raster():
    try:
        args = _parse_args(data = request.json,
                           defaults = _raster_defaults())
    except Exception as e:
        return _return_exception(e)

    return _serve(_raster(args))


@route('/api/shadow', method=['GET'])
def get_shadow():
    try:
        args = _parse_args(data = request.query,
                           defaults = _shadow_defaults())
    except Exception as e:
        return _return_exception(e)

    return _serve(_shadow(args))


@route('/api/shadow', method=['POST'])
def post_shadow():
    try:
        args = _parse_args(data = request.json,
                           defaults = _shadow_defaults())
    except Exception as e:
        return _return_exception(e)

    return _serve(_shadow(args))


@route('/api/osm', method=['GET'])
def get_osm():
    try:
        args = _parse_args(data = request.query,
                           defaults = _osm_defaults())
    except Exception as e:
        return _return_exception(e)

    return _serve(_osm(args))


@route('/api/osm', method=['POST'])
def post_osm():
    try:
        args = _parse_args(data = request.json,
                           defaults = _osm_defaults())
    except Exception as e:
        return _return_exception(e)

    return _serve(_osm(args))


@route('/api/irradiance', method=['GET'])
def get_irradiance():
    try:
        args = _parse_args(data = request.query,
                           defaults = _irradiance_defaults())
    except Exception as e:
        return _return_exception(e)

    return _serve(_irradiance(args))


@route('/api/irradiance', method=['POST'])
def post_irradiance():
    try:
        args = _parse_args(data = request.json,
                           defaults = _irradiance_defaults())
    except Exception as e:
        return _return_exception(e)

    return _serve(_irradiance(args))


@route('/api/upload', method=['POST'])
def post_upload():
    try:
        data = request.files.data
    except Exception as e:
        return _return_exception(e)

    return _serve(upload(data))


@route('/api/raster/help', method=['GET'])
def get_raster_help():
    return {'results': _format_help(_raster_defaults())}


@route('/api/shadow/help', method=['GET'])
def get_shadow_help():
    return {'results': _format_help(_shadow_defaults())}


@route('/api/osm/help', method=['GET'])
def get_osm_help():
    return {'results': _format_help(_osm_defaults())}


@route('/api/irradiance/help', method=['GET'])
def get_irradiance_help():
    return {'results': _format_help(_irradiance_defaults())}


@route('/api/upload/help', method=['GET'])
def get_upload_help():
    return {'results': _format_help(_upload_defaults())}


run(host='0.0.0.0', port=8080,
    server='gunicorn',
    workers=int(PVGRIP_CONFIGS['server']['server_workers']),
    max_requests=int(PVGRIP_CONFIGS['server']['max_requests']),
    timeout=int(PVGRIP_CONFIGS['server']['timeout']))
