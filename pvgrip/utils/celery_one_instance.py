import os
import diskcache

from functools \
    import wraps

from redis.lock \
    import Lock

from redis \
    import StrictRedis

from urllib.parse \
    import urlparse, parse_qsl

from pvgrip.globals \
    import RESULTS_PATH, REDIS_URL

from pvgrip.utils.exceptions \
    import TASK_RUNNING

from pvgrip.utils.float_hash \
    import float_hash


def parse_url(url):
    """
    Parse the argument url and return a redis connection.
    Three patterns of url are supported:
        * redis://host:port[/db][?options]
        * redis+socket:///path/to/redis.sock[?options]
        * rediss://host:port[/db][?options]
    A ValueError is raised if the URL is not recognized.

    Taken from: https://github.com/cameronmaske/celery-once/blob/master/celery_once/backends/redis.py

    """
    parsed = urlparse(url)
    kwargs = parse_qsl(parsed.query)

    # TCP redis connection
    if parsed.scheme in ['redis', 'rediss']:
        details = {'host': parsed.hostname}
        if parsed.port:
            details['port'] = parsed.port
        if parsed.password:
            details['password'] = parsed.password
        db = parsed.path.lstrip('/')
        if db and db.isdigit():
            details['db'] = db
        if parsed.scheme == 'rediss':
            details['ssl'] = True

    # Unix socket redis connection
    elif parsed.scheme == 'redis+socket':
        details = {'unix_socket_path': parsed.path}
    else:
        raise ValueError('Unsupported protocol %s' % (parsed.scheme))

    # Add kwargs to the details and convert them to the appropriate type, if needed
    details.update(kwargs)
    if 'socket_timeout' in details:
        details['socket_timeout'] = float(details['socket_timeout'])
    if 'db' in details:
        details['db'] = int(details['db'])

    return details


def one_instance(expire=60):
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            REDIS = StrictRedis(**parse_url(REDIS_URL))
            key = float_hash(("one_instance_lock",
                              fun.__name__, args, kwargs))
            acquired = Lock\
                (REDIS,
                 key,
                 timeout = expire,
                 blocking = 1,
                 blocking_timeout = False)\
                 .acquire()

            if not acquired:
                raise TASK_RUNNING()

            try:
                return fun(*args, **kwargs)
            finally:
                REDIS.delete(key)
        return wrap
    return wrapper
