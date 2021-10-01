import logging

from functools \
    import wraps

from pvgrip.utils.exceptions \
    import TASK_RUNNING

from pvgrip.globals \
    import REDIS_URL

from pvgrip.utils.float_hash \
    import float_hash

from pvgrip.utils.redis.lock \
    import RedisLock, Locked


def one_instance(expire=60):
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            key = float_hash\
                (("one_instance_lock",
                  fun.__name__, args, kwargs))
            try:
                with RedisLock\
                     (redis_url = REDIS_URL,
                      key = key,
                      timeout = expire):
                    return fun(*args, **kwargs)
            except Locked:
                logging.debug("one_instance: {} is locked!"\
                              .format(key))
            raise TASK_RUNNING()
        return wrap
    return wrapper
