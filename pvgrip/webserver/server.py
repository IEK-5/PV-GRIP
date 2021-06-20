import types
import bottle
import logging

from pvgrip.globals \
    import LOGGING_LEVEL, PVGRIP_CONFIGS

logging.basicConfig(filename = 'data/server.log',
                    level = LOGGING_LEVEL,
                    format = "[%(asctime)s] %(levelname)s: %(filename)s::%(funcName)s %(message)s")

from pvgrip.webserver.utils \
    import format_help, return_exception, \
    call_method, parse_args

from pvgrip.webserver.defaults \
    import call_defaults, calls_help

from pvgrip.webserver.tasks \
    import get_task


@bottle.route('/api/help', method=['GET'])
def get_help():
    return {'results': calls_help()}


@bottle.route('/api/help/<what:path>', method=['GET'])
def get_what_help(what):
    res = call_defaults(method = what)

    if not res:
        return error404('no help for the %s available' % what)

    return {'results': format_help(res)}


@bottle.route('/api/<method:path>', method=['GET','POST'])
def do_method(method):
    if 'GET' == bottle.request.method:
        args = bottle.request.query
    elif 'POST' == bottle.request.method:
        args = bottle.request.json
    else:
        return error404('%s access method is not implemented' \
                        % str(bottle.request.method))

    try:
        defaults = call_defaults(method = method)
        args = parse_args(data = args, defaults = defaults)
        task = get_task(method = method, args = args)
    except Exception as e:
        return return_exception(e)

    if not isinstance(task, types.FunctionType):
        return task

    return call_method(task=task, args=args)


@bottle.error(404)
def error404(error):
    return {'results': str(error)}


bottle.run(host='0.0.0.0', port=8080,
           server='gunicorn',
           workers=int(
               PVGRIP_CONFIGS['server']['server_workers']),
           max_requests=int(
               PVGRIP_CONFIGS['server']['max_requests']),
           timeout=int(
               PVGRIP_CONFIGS['server']['timeout']))
