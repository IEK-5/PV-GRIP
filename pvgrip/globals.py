import os
import json
import celery
import logging

from cassandra_io.files \
    import Cassandra_Files
from ipfs_io.files \
    import IPFS_Files

from pvgrip.utils.git \
    import git_root
from pvgrip.storage.spatial_data \
    import Spatial_Data
from pvgrip.storage.files_lrucache \
    import Files_LRUCache
from pvgrip.utils.redis.dictionary \
    import Redis_Dictionary
from pvgrip.utils.get_configs \
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

_IPFS_STORAGE_IP = \
    PVGRIP_CONFIGS['ipfs']['ip']
_IPFS_STORAGE_KEYSPACE_SUFFIX = \
    PVGRIP_CONFIGS['ipfs']['keyspace_suffix']

REDIS_URL = 'redis://{ip}:{port}/{db}'.format\
    (ip=PVGRIP_CONFIGS['redis']['ip'],port=6379,db=0)

ALLOWED_REMOTE = \
    json.loads(PVGRIP_CONFIGS['storage']['use_remotes'])
DEFAULT_REMOTE = PVGRIP_CONFIGS['storage']['default']

GRASS=PVGRIP_CONFIGS['grass']['executable']
GRASS_NJOBS = int(PVGRIP_CONFIGS['grass']['njobs'])


SSDP=PVGRIP_CONFIGS['ssdp']['executable']
SSDP_NJOBS = int(PVGRIP_CONFIGS['ssdp']['njobs'])

COPERNICUS_CDS_HASH_LENGTH = \
    int(PVGRIP_CONFIGS['copernicus']['cds_hash_length'])
COPERNICUS_ADS_HASH_LENGTH = \
    int(PVGRIP_CONFIGS['copernicus']['ads_hash_length'])
COPERNICUS_CDS = {
    'url': PVGRIP_CONFIGS['copernicus']['cds_url'],
    'key': PVGRIP_CONFIGS['copernicus']['cds_key']}
COPERNICUS_ADS = {
    'url': PVGRIP_CONFIGS['copernicus']['ads_url'],
    'key': PVGRIP_CONFIGS['copernicus']['ads_key']}

_LOGGING = {'INFO': logging.INFO,
            'DEBUG': logging.DEBUG,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL}
LOGGING_LEVEL = _LOGGING\
    [PVGRIP_CONFIGS['server']['logging_level']]


AUTODISCOVER_TASKS = [
    'pvgrip.integrate',
    'pvgrip.irradiance',
    'pvgrip.lidar',
    'pvgrip.osm',
    'pvgrip.raster',
    'pvgrip.route',
    'pvgrip.shadow',
    'pvgrip.status',
    'pvgrip.storage',
    'pvgrip.utils',
    'pvgrip.webserver',
]


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
         replication_args = _CASSANDRA_REPLICATION_ARGS,
         protocol_version = 4,
         connect_timeout = 60,
         idle_heartbeat_timeout = 300,
         control_connection_timeout = 30)


def get_IPFS_STORAGE():
    return IPFS_Files\
        (ipfs_ip = _IPFS_STORAGE_IP,
         cluster_ips = [_CASSANDRA_STORAGE_IP],
         keyspace_suffix = _IPFS_STORAGE_KEYSPACE_SUFFIX,
         replication = _CASSANDRA_REPLICATION,
         replication_args = _CASSANDRA_REPLICATION_ARGS,
         protocol_version = 4,
         connect_timeout = 60,
         idle_heartbeat_timeout = 300,
         control_connection_timeout = 30)


def get_SPATIAL_DATA():
    if 'ipfs_path' == DEFAULT_REMOTE:
        storage = get_IPFS_STORAGE()
    elif 'cassandra_path' == DEFAULT_REMOTE:
        storage = get_CASSANDRA_STORAGE()
    else:
        raise RuntimeError("unknown storage type")

    return Spatial_Data\
        (cassandra_ips = [_CASSANDRA_STORAGE_IP],
         storage = storage,
         index_args = \
         {'hash_min': _CASSANDRA_SPATIAL_INDEX_HASH_MIN,
          'depth': _CASSANDRA_SPATIAL_INDEX_DEPTH},
         base_args = \
         {'replication': _CASSANDRA_REPLICATION,
          'replication_args': _CASSANDRA_REPLICATION_ARGS})


def get_Tasks_Queues():
    return Redis_Dictionary(name = 'pvgrip_tasks_queue',
                            redis_url = REDIS_URL)
