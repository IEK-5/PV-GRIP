import os
import json
import logging
import diskcache

from open_elevation.utils \
    import git_root
from cassandra_io.files \
    import Cassandra_Files
from open_elevation.spatial_data \
    import Spatial_Data
from open_elevation.files_lrucache \
    import Files_LRUCache
from open_elevation.utils \
    import get_configs

GIT_ROOT = git_root()

PVGRIP_CONFIGS = get_configs\
    (os.path.join(GIT_ROOT,'configs','pvgrip.conf'))

RESULTS_PATH = os.path.join(GIT_ROOT,'data','results_cache')


_CASSANDRA_STORAGE_IP = \
    PVGRIP_CONFIGS['cassandra']['ip']
_CASSANDRA_STORAGE_CHUNKSIZE = \
    int(PVGRIP_CONFIGS['cassandra']['chunksize'])
_CASSANDRA_STORAGE_KEYSPACE_SUFFIX = \
    PVGRIP_CONFIGS['cassandra']['keyspace_suffix']
_CASSANDRA_REPLICATION = \
    PVGRIP_CONFIGS['cassandra']['replication']
_CASSANDRA_REPLICATION_ARGS = \
    json.loads(PVGRIP_CONFIGS['cassandra']['replication_args'])
_CASSANDRA_SPATIAL_INDEX_HASH_MIN = \
    int(PVGRIP_CONFIGS['cassandra']['spatial_index_hash_min'])
_CASSANDRA_SPATIAL_INDEX_DEPTH = \
    int(PVGRIP_CONFIGS['cassandra']['spatial_index_depth'])


REDIS_URL = 'redis://' + \
    PVGRIP_CONFIGS['redis']['ip'] + ':6379/0'


GRASS=PVGRIP_CONFIGS['grass']['executable']
GRASS_NJOBS = int(PVGRIP_CONFIGS['grass']['njobs'])


_LOGGING = {'INFO': logging.INFO,
            'DEBUG': logging.DEBUG,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL}
LOGGING_LEVEL = _LOGGING\
    [PVGRIP_CONFIGS['server']['logging_level']]


def get_RESULTS_CACHE():
    return Files_LRUCache\
        (path = RESULTS_PATH,
         maxsize = int(PVGRIP_CONFIGS['cache']['limit_worker']))


def get_CASSANDRA_STORAGE():
    return  Cassandra_Files\
        (cluster_ips = [_CASSANDRA_STORAGE_IP],
         keyspace_suffix = _CASSANDRA_STORAGE_KEYSPACE_SUFFIX,
         chunk_size = _CASSANDRA_STORAGE_CHUNKSIZE,
         replication = _CASSANDRA_REPLICATION,
         replication_args = _CASSANDRA_REPLICATION_ARGS)


def get_SPATIAL_DATA():
    CASSANDRA_STORAGE = get_CASSANDRA_STORAGE()
    return Spatial_Data\
        (cfs = CASSANDRA_STORAGE,
         index_args = \
         {'hash_min': _CASSANDRA_SPATIAL_INDEX_HASH_MIN,
          'depth': _CASSANDRA_SPATIAL_INDEX_DEPTH},
         base_args = \
         {'replication': _CASSANDRA_REPLICATION,
          'replication_args': _CASSANDRA_REPLICATION_ARGS})
