import json
import redis

from pvgrip.utils.float_hash \
    import float_hash


class Redis_Dictionary:


    def __init__(self, name, host, port, db,
                 hash_function = float_hash):
        """A distributed dictionary with redis

        :name: name of the hset in redis

        :host: redis hostname

        :port: redis port

        :db: redis db to use

        :hash_function: function that produces a hash for keys
        """
        self._hash = hash_function
        self._client = redis.StrictRedis(host = host, port = port, db = db)
        self._name = name


    def __contains__(self, key):
        hkey = self._hash(key)
        return self._client.hexists(self._name, hkey)


    def __setitem__(self, key, item):
        hkey = self._hash(key)
        sitem = json.dumps(item)
        self._client.hset(self._name, hkey, sitem)


    def __getitem__(self, key):
        hkey = self._hash(key)

        if not self._client.hexists(self._name, hkey):
            raise KeyError\
                ("'{key}' is not in hset".format(key=key))

        sitem = self._client.hget(self._name, hkey)
        return json.loads(sitem)


    def __delitem__(self, key):
        hkey = self._hash(key)
        self._client.hdel(self._name, hkey)
