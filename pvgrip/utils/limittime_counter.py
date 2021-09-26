import re
import time
import redis

from datetime import datetime

from pvgrip.utils.redis.parse_url \
    import parse_url

from pvgrip.utils.redis.lock \
    import RedisLock


limit_re = re.compile(r'([0-9]+)([mhdM])(P?)')


class FullQueue(Exception):
    pass


class Limit_Counter:
    """Keep track on some limit in time counters

    """

    def __init__(self, name, limit, redis_url):
        """init

        :name: name by which this limit counter goes by

        :limit: definition of a limit in the format

        <integer><unit>P?

        where unit is "m" (minutely), "h" (hourly) "d" daily, or "M"
        (monthly).

        the last character 'P' is a flag: if set then the counter
        expires by the end of a unit. For example, '1dP', the resets
        at a start of a new day.

        :redis_url: how to connect to Redis and where to store the
        counter.

        """
        if not limit_re.match(limit):
            raise RuntimeError\
                ("limit={} does not match required format"\
                 .format(limit))

        self.limit, self.time_period, self.limit_in_period = \
            limit_re.findall(limit)[0]
        self.limit_in_period = self.limit_in_period == 'P'
        self.limit = int(self.limit)

        self.name = "limit_counter_{}_{}_{}"\
            .format(name, limit, self.limit_in_period)

        self._client = redis.StrictRedis\
            (**parse_url(redis_url))
        self._lock = RedisLock(redis_url = redis_url,
                               key = "lock_{}"\
                               .format(self.name),
                               sleep = 1, timeout = 60)

        self._init_delay_dformat()


    def _init_delay_dformat(self):
        if "m" == self.time_period:
            self._delay = 60
            self._dformat = "%Y.%m.%dT%H:%M"
        elif "h" == self.time_period:
            self._delay = 60*60
            self._dformat = "%Y.%m.%dT%H"
        elif "d" == self.time_period :
            self._delay = 60*60*24
            self._dformat = "%Y.%m.%d"
        elif "M" == self.time_period:
            self._delay = 60*60*24*30
            self._dformat = "%Y.%m"
        else:
            raise RuntimeError("invalid time_period = {}"\
                               .format(self._time_period))


    def _waittime(self):
        """Identifies how old is the first entry in the list

        returns 0, if the first entry is older than the time_period.

        Otherwise, return number of seconds to wait for the first
        entry to expire (or become older than the time_period).
        """
        if 0 == self._client.llen(self.name):
            return -1

        now = time.time()
        car = float(self._client.lindex(self.name,0))

        if self.limit_in_period:
            fnow = datetime.strftime(datetime.fromtimestamp(now),
                                     self._dformat)
            fcar = datetime.strftime(datetime.fromtimestamp(car),
                                     self._dformat)
            if fnow != fcar:
                return 0
            next_period = datetime.strftime\
                (datetime.fromtimestamp(now + self._delay),
                 self._dformat)
            next_period = datetime.strptime\
                (next_period, self._dformat)

            return next_period.timestamp() - now

        if now - car > self._delay:
            return 0

        return self._delay - (now - car)


    def waittime(self):
        """Compute wait time

        Returns 0 if queue is free or number of seconds to wait
        """
        with self._lock:
            while not self._waittime():
                self._client.lpop(self.name)

            if self._client.llen(self.name) < self.limit:
                return 0

            return self._waittime()


    def increment(self):
        with self._lock:
            while not self._waittime():
                self._client.lpop(self.name)

            if self._client.llen(self.name) >= self.limit \
               and self._waittime() > 0:
                raise FullQueue()

            self._client.rpush(self.name,time.time())


    def __len__(self):
        return self._client.llen(self.name)


    def usage(self):
        return "{:2.0f}% usage of {}{}{}"\
            .format(100*len(self)/self.limit,
                    self.limit, self.time_period,
                    "P" if self.limit_in_period else "")


    def __print__(self):
        return self.usage()
