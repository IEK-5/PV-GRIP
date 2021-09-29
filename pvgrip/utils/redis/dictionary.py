import json
import redis

from pvgrip.utils.float_hash \
    import float_hash

from pvgrip.utils.redis.parse_url \
    import parse_url


class Redis_Dictionary:


    def __init__(self, name, redis_url,
                 hash_function = float_hash,
                 expire_time = 7200):
        """A distributed dictionary with redis

        :name: name of the set in redis

        :redis_url: how to connect to redis

        :hash_function: function that produces a hash for keys

        :expire_time: time to expire for dictionary items
        """
        self._name = name
        self._client = redis.StrictRedis(**parse_url(redis_url))
        self._hash = hash_function
        self._expire = expire_time


    def __contains__(self, key):
        hkey = self._hash(key)
        return self._client.exists(self._name + hkey)


    def __setitem__(self, key, item):
        hkey = self._hash(key)
        sitem = json.dumps(item)
        self._client.set(self._name + hkey, sitem,
                         ex = self._expire)


    def __getitem__(self, key):
        hkey = self._hash(key)

        if not self._client.exists(self._name + hkey):
            raise KeyError\
                ("'{key}' is not in hset".format(key=key))

        sitem = self._client.get(self._name + hkey)
        return json.loads(sitem)


    def __delitem__(self, key):
        hkey = self._hash(key)
        self._client.delete(self._name + hkey)
