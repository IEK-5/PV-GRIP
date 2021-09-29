from functools \
    import wraps

from redis.lock \
    import Lock

from redis \
    import StrictRedis

from pvgrip.utils.exceptions \
    import TASK_RUNNING

from pvgrip.globals \
    import REDIS_URL

from pvgrip.utils.float_hash \
    import float_hash

from pvgrip.utils.redis.parseurl \
    import parse_url


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
