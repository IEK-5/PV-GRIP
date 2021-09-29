import time
import redis

from pvgrip.utils.redis.parse_url \
    import parse_url


class Locked(Exception):
    pass


class RedisLock:
    """Distributed lock

    """

    def __init__(self, redis_url, key, sleep=-1, timeout=None):
        """init

        :redis_url: how to connect to redis

        :key: a name for the lock

        :sleep: if positive locks sleeps before another attempt

        :timeout: expire time for the lock

        """
        self.key = key
        self.sleep = sleep
        self._redis = redis.StrictRedis\
            (**parse_url(redis_url))
        self._lock = redis.lock.Lock\
            (self._redis, self.key, timeout = timeout,
             blocking = 1,
             blocking_timeout = False)



    def __enter__(self):
        while not self._lock.acquire():
            if self.sleep < 0:
                raise Locked()

            time.sleep(self.sleep)
        return True


    def __exit__(self, type, value, traceback):
        self._redis.delete(self.key)
