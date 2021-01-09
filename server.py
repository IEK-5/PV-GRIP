import os
import json
import logging
logging.basicConfig(filename = 'data/server.log',
                    level = logging.DEBUG,
                    format = "[%(asctime)s] %(levelname)s: %(filename)s::%(funcName)s %(message)s")

import bottle
from bottle import route, run, request, response, hook

from celery.result import AsyncResult

import open_elevation.gdal_interfaces as gdal
import open_elevation.utils as utils
import open_elevation.nrw_las as nrw_las
import open_elevation.celery_tasks as tasks


class InternalException(ValueError):
    """
    Utility exception class to handle errors internally and return error codes to the client
    """
    pass


"""
Initialize a global interface. This can grow quite large, because it has a cache.
"""
interface = gdal.GDALTileInterface('data/','data/index.json',9)
logging.info(interface.print_used_las_space())

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


def _parse_args(data, defaults):
    res = {}
    for key, item in defaults.items():
        if key not in data:
            res[key] = item
        else:
            new = data[key]
            if type(item) != type(new) \
               and isinstance(new, str):
                new = json.loads(new)
            res[key] = type(item)(new)

    return res


def _get_raster(args):
    try:
        fn = tasks.start_sample_from_box\
            (gdal = interface, **args)
        with open(fn,'rb') as f:
            return f.read()
    except utils.TASK_RUNNING:
        return {'results': {'message': 'task is running'}}
    except Exception as e:
        return {'results': {
            'error':
            """
            Task failed!
            Task: get_raster
            Arguments: %s
            Error: %s
            """ % (str(args),
                   type(e).__name__ + ": " + str(e))
        }}



def _raster_defaults():
    return {'box': [50.865,7.119,50.867,7.121],
            'data_re': r'.*',
            'step': float(1),
            'mesh_type': 'metric'}


@route('/api/v1/raster', method=['GET'])
def get_raster():
    args = _parse_args(data = request.query,
                       defaults = _raster_defaults())

    return _get_raster(args)


@route('/api/v1/raster', method=['POST'])
def get_raster():
    args = _parse_args(data = request.json,
                       defaults = _raster_defaults())

    return _get_raster(args)


run(host='0.0.0.0', port=8080,
    server='gunicorn',
    workers=8, timeout=60)
