import json
import redis

from pvgrip.utils.float_hash \
    import float_hash


class Redis_Dictionary:


    def __init__(self, name, host, port, db,
                 hash_function = float_hash,
                 expire_time = 7200):
        """A distributed dictionary with redis

        :name: name of the hset in redis

        :host: redis hostname

        :port: redis port

        :db: redis db to use

        :hash_function: function that produces a hash for keys

        :expire_time: time to expire for dictionary items
        """
        self._hash = hash_function
        self._client = redis.StrictRedis(host = host, port = port, db = db)
        self._name = name
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
