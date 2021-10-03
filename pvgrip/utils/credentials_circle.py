import re
import time
import random
import logging

from collections import defaultdict

from datetime import datetime

from pvgrip.utils.limittime_counter \
    import Limit_Counter, FullQueue
from pvgrip.utils.circle \
    import Circle
from pvgrip.utils.get_configs \
    import get_configs

from pvgrip.utils.redis.lock \
    import RedisLock


class Credentials_Circle:


    def __init__(self, config_fn, redis_url, maxwait = 180):
        """Init class

        :config_fn: a filename with configs
        the file has a format

        [name who own credentials]
        circle_limit = <number><units><period_flag>,<number><units>,...
        other_fields = ...
        # other fields contain whatever data
        # needed for the credentials

        :redis_url: how to connect to redis

        :maxwait: maximum wait for credentials to clear up
        """
        self.config_fn = config_fn
        self.redis_url = redis_url
        self.maxwait = maxwait
        self._circle = Circle()
        self._limits = {}
        self._data = {}
        self._waittime_cache = {}
        self._locks = {}
        self._init()


    def _form_limits(self, name, limits):
        return [Limit_Counter(name=name, limit = limit,
                              redis_url = self.redis_url)
                for limit in limits.split(',')]


    def _init(self):
        config = list(get_configs(self.config_fn)._sections.items())
        random.shuffle(config)
        for name, item in config:
            if 'circle_limit' not in item:
                logging.warning\
                    ("circle_limit is missing in the '{}' section!"\
                     .format(name))
                continue

            if name in self._limits:
                logging.warning\
                    ("name={} is duplicated!".format(name))
                continue

            self._circle.add(name)
            self._limits[name] = \
                self._form_limits("{}::{}"\
                                  .format(self.config_fn, name),
                                  item['circle_limit'])
            del item['circle_limit']
            self._data[name] = item
            self._locks[name] = RedisLock\
                (redis_url = self.redis_url,
                 key = "lock_credentials_circle_{}"\
                 .format(name),
                 sleep = 1, timeout = 60)
            self._waittime_cache[name] = 0


    def _check_limits(self, name):
        if self._waittime_cache[name] - time.time() > self.maxwait:
            return False

        wait = [0]
        for l in self._limits[name]:
            w = l.waittime()
            if not w:
                continue
            wait += [w]

        wait = max(wait)
        self._waittime_cache[name] = time.time() + wait

        if wait > self.maxwait:
            return False

        logging.debug("_check_limits, waiting for {} seconds"\
                      .format(wait))
        time.sleep(wait)
        return True


    def __call__(self):
        tried = defaultdict(lambda: 0)
        while True:
            name = self._circle()

            if tried[name] > 0:
                if min(tried.values()) > 0:
                    break
                continue

            if not self._check_limits(name):
                tried[name] = 1
                continue

            with self._locks[name]:
                try:
                    for l in self._limits[name]:
                        l.increment()
                except FullQueue:
                    logging.debug("credential_circle: {} hit the limit!"\
                                  .format(name))
                    continue

            logging.info("""
            selecting credentials from {}
            current usage = {}
            """.format(name, [l.usage() \
                              for l in self._limits[name]]))
            return (name, self._data[name])

        waittill = max([x for _, x in \
                        self._waittime_cache.items()])
        waittill = datetime.strftime\
            (datetime.fromtimestamp(waittill),'%c')

        raise RuntimeError\
            ("""All credentials hit a limit!
            Wait until: {}
            """.format(waittill))
