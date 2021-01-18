import os
import json
import logging
logging.basicConfig(filename = 'data/server.log',
                    level = logging.DEBUG,
                    format = "[%(asctime)s] %(levelname)s: %(filename)s::%(funcName)s %(message)s")

import datetime

import bottle
from bottle import route, run, request, response, hook

from celery.result import AsyncResult
from celery.exceptions import TimeoutError

import open_elevation.gdal_interfaces as gdal
import open_elevation.utils as utils
import open_elevation.celery_tasks as tasks
import open_elevation.celery_tasks.app as app


class InternalException(ValueError):
    """
    Utility exception class to handle errors internally and return error codes to the client
    """
    pass


"""
Initialize a global interface. This can grow quite large, because it has a cache.
"""
interface = gdal.GDALTileInterface('data/current','data/index.json',9)
logging.info("Amount of RESULTS_CACHE: %.2f" \
             % (app.RESULTS_CACHE.size()/(1024**3),))


def get_elevation(lat, lng, data_re):
    """
    Get the elevation at point (lat,lng) using the currently opened interface
    :param lat:
    :param lng:
    :return:
    """
    try:
        res = interface.lookup(lat = lat,
                               lon = lng,
                               data_re = data_re)
        elevation = res['elevation']
        resolution = res['resolution']
    except utils.TASK_RUNNING:
        return {
            'message': 'task is running'
        }
    except Exception as e:
        return {
            'latitude': lat,
            'longitude': lng,
            'error':
            """
            Cannot process request!
                Coordinate: (%s, %s)
                     Error: %s
            """ % (lat, lng,
                   type(e).__name__ + ": " + str(e))
        }

    return {
        'latitude': lat,
        'longitude': lng,
        'elevation': elevation,
        'resolution': resolution
    }


@hook('after_request')
def enable_cors():
    """
    Enable CORS support.
    :return:
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'


def lat_lng_from_location(location_with_comma):
    """
    Parse the latitude and longitude of a location in the format "xx.xxx,yy.yyy" (which we accept as a query string)
    :param location_with_comma:
    :return:
    """
    try:
        lat, lng = [float(i) for i in location_with_comma.split(',')]
        return lat, lng
    except:
        raise InternalException(json.dumps({'error': 'Bad parameter format "%s".' % location_with_comma}))


def query_to_locations():
    """
    Grab a list of locations from the query and turn them into [(lat,lng),(lat,lng),...]
    :return:
    """
    locations = request.query.locations
    if not locations:
        raise InternalException(json.dumps({'error': '"Locations" is required.'}))

    data_re = request.query.data_re

    return {'locations': [lat_lng_from_location(l) \
                          for l in locations.split('|')],
            'data_re': data_re}


def body_to_locations():
    """
    Grab a list of locations from the body and turn them into [(lat,lng),(lat,lng),...]
    :return:
    """
    try:
        locations = request.json.get('locations', None)
        data_re = request.json.get('data_re', None)
    except Exception:
        raise InternalException(json.dumps({'error': 'Invalid JSON.'}))

    if not locations:
        raise InternalException(json.dumps({'error': '"Locations" is required in the body.'}))

    latlng = []
    for l in locations:
        try:
            latlng += [ (l['latitude'],l['longitude']) ]
        except KeyError:
            raise InternalException(json.dumps({'error': '"%s" is not in a valid format.' % l}))

    return {'locations': latlng, 'data_re': data_re}


def do_lookup(get_locations_func):
    """
    Generic method which gets the locations in [(lat,lng),(lat,lng),...] format by calling get_locations_func
    and returns an answer ready to go to the client.
    :return:
    """
    try:
        inp = get_locations_func()
        return {'results': [get_elevation(lat, lng, data_re = inp['data_re']) \
                            for (lat, lng) in inp['locations']]}
    except InternalException as e:
        response.status = 400
        response.content_type = 'application/json'
        return e.args[0]

# Base Endpoint
URL_ENDPOINT = '/api/v1/lookup'

# For CORS
@route(URL_ENDPOINT, method=['OPTIONS'])
def cors_handler():
    return {}

@route(URL_ENDPOINT, method=['GET'])
def get_lookup():
    """
    GET method. Uses query_to_locations.
    :return:
    """
    return do_lookup(query_to_locations)


@route(URL_ENDPOINT, method=['POST'])
def post_lookup():
    """
    GET method. Uses body_to_locations.
    :return:
    """
    return do_lookup(body_to_locations)


@route('/api/v1/datasets', method=['GET'])
def get_datasets():
    return {'results': interface.get_directories()}


def _serve(data):
    if isinstance(data, dict):
        return data

    if os.path.exists(data):
        with open(data,'rb') as f:
            return f.read()

    return data


def _parse_args(data, defaults):
    res = {}

    for key, _ in data.items():
        if key not in defaults:
            raise RuntimeError("Unknown argument: '%s'" % key)


    for key, item in defaults.items():
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


def _get_job_results(job, timeout = 30):
    try:
        if 'SUCCESS' == job.state:
            fn = job.result

        if 'FAILURE' == job.state:
            raise job.result

        if 'PENDING' == job.state:
            fn = job.wait(timeout = timeout)

        return fn
    except utils.TASK_RUNNING:
        return {'results': {'message': 'task is running'}}
    except TimeoutError:
        return {'results': {'message': 'task is running',
                            'jobid': job.id}}
    except Exception as e:
        return {'results':
                {'error': type(e).__name__ + ": " + str(e)}}


@app.cache_fn_results(link = True,
                      ignore = lambda x: isinstance(x,dict))
def _get_raster(args):
    try:
        job = tasks.sample_raster\
            (gdal = interface, **args).delay()
    except Exception as e:
        return {'results':
                {'error': type(e).__name__ + ": " + str(e),
                 'what': '_get_raster',
                 'args': args}}

    return _get_job_results(job)


def _raster_defaults():
    return {'box': [50.865,7.119,50.867,7.121],
            'data_re': r'.*',
            'stat': 'max',
            'step': float(1),
            'mesh_type': 'metric',
            'output_type': 'pickle'}


@route('/api/v1/raster', method=['GET'])
def get_raster():
    try:
        args = _parse_args(data = request.query,
                           defaults = _raster_defaults())
    except Exception as e:
        return {'results':
                {'error': type(e).__name__ + ": " + str(e)}}

    return _serve(_get_raster(args))


@route('/api/v1/raster', method=['POST'])
def get_raster():
    try:
        args = _parse_args(data = request.json,
                           defaults = _raster_defaults())
    except Exception as e:
        return {'results':
                {'error': type(e).__name__ + ": " + str(e)}}

    return _serve(_get_raster(args))


@route('/api/v1/raster/help', method=['GET'])
def get_raster_help():
    return {'results': _raster_defaults()}


def _shadow_defaults():
    res = _raster_defaults()
    res.update({
        'output_type': 'png',
        'what': 'shadow',
        'timestr': "2020-07-01_06:00:00"
    })
    return res


@app.cache_fn_results(link = True,
                      ignore = lambda x: isinstance(x,dict))
def _get_shadow(args):
    try:
        job = tasks.shadow\
            (gdal = interface, **args).delay()
    except Exception as e:
        return {'results':
                {'error': type(e).__name__ + ": " + str(e),
                 'what': '_get_raster',
                 'args': args}}


    return _get_job_results(job)


@route('/api/v1/shadow', method=['GET'])
def get_shadow():
    try:
        args = _parse_args(data = request.query,
                           defaults = _shadow_defaults())
    except Exception as e:
        return {'results':
                {'error': type(e).__name__ + ": " + str(e)}}

    return _serve(_get_shadow(args))


@route('/api/v1/shadow', method=['POST'])
def get_shadow():
    try:
        args = _parse_args(data = request.json,
                           defaults = _shadow_defaults())
    except Exception as e:
        return {'results':
                {'error': type(e).__name__ + ": " + str(e)}}

    return _serve(_get_shadow(args))


@route('/api/v1/shadow/help', method=['GET'])
def get_shadow_help():
    return {'results': _shadow_defaults()}


run(host='0.0.0.0', port=8080,
    server='gunicorn',
    workers=8, timeout=60)
