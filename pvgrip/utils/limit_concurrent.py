import time
import redis
import logging

from functools \
    import wraps

from pvgrip.globals \
    import REDIS_URL

from pvgrip.utils.float_hash \
    import float_hash

from pvgrip.utils.redis.parse_url \
    import parse_url

from pvgrip.utils.redis.lock \
    import RedisLock


def limit_concurrent(maxtimes = 2,
                     sleep = 3,
                     locksleep = 0.5,
                     lockexpire = 60,
                     keys = None):
    def wrapper(fun):
        @wraps(fun)
        def wrap(*args, **kwargs):
            REDIS = redis.StrictRedis(**parse_url(REDIS_URL))
            if keys is None:
                key = float_hash(("limit_concurrent_list",
                                  fun.__name__, args, kwargs))
            else:
                key = float_hash(("limit_concurrent_list",
                                  fun.__name__, args,
                                  [k for k in keys
                                   if k in kwargs]))

            while True:
                with RedisLock\
                     (redis_url = REDIS_URL,
                      key = float_hash\
                      (("limit_concurrent_lock", key)),
                      sleep = locksleep,
                      timeout = lockexpire):
                    if REDIS.llen(key) <= maxtimes:
                        REDIS.rpush(key,0)
                        break
                logging.debug\
                    ("limit_concurrent sleeping...")
                time.sleep(sleep)

            try:
                return fun(*args, **kwargs)
            finally:
                REDIS.lpop(key)
        return wrap
    return wrapper
