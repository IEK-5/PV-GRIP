import os
import re
import json
import celery
import logging

from cassandra_io.files \
    import Cassandra_Files
from ipfs_io.files \
    import IPFS_Files
from pvgrip.storage.local_io.files \
    import LOCALIO_Files

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

from pvgrip.utils.credentials_circle \
    import Credentials_Circle


def _set_localmount(configs, allowed_remote):
    if 'localmount_' not in allowed_remote:
        return allowed_remote, None

    res = {'localmount_{}'.format(x): \
           configs['localmount_'][x]\
           for x in list(configs['localmount_'])}

    remotes = [x for x in allowed_remote if x != 'localmount_']
    remotes += list(res.keys())

    return remotes, res


GIT_ROOT = git_root()

PVGRIP_CONFIGS = get_configs\
    (os.path.join(GIT_ROOT,'configs','pvgrip.conf'))

RESULTS_PATH = os.path.join(GIT_ROOT,'data','results_cache')


_CASSANDRA_STORAGE_IP = \
    PVGRIP_CONFIGS['cassandra']['ip']
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
_IPFS_TIMEOUT = int(PVGRIP_CONFIGS['ipfs']['ipfs_timeout'])


REDIS_URL = 'redis://{ip}:{port}/{db}'.format\
    (ip=PVGRIP_CONFIGS['redis']['ip'],port=6379,db=0)
REDIS_EXPIRES = int(PVGRIP_CONFIGS['redis']['expires'])


ALLOWED_REMOTE = \
    json.loads(PVGRIP_CONFIGS['storage']['use_remotes'])
ALLOWED_REMOTE, _LOCAL_STORAGE_ROOTS = \
    _set_localmount(PVGRIP_CONFIGS, ALLOWED_REMOTE)
DEFAULT_REMOTE = PVGRIP_CONFIGS['storage']['default']


GRASS=PVGRIP_CONFIGS['grass']['executable']
GRASS_NJOBS = int(PVGRIP_CONFIGS['grass']['njobs'])


SSDP=PVGRIP_CONFIGS['ssdp']['executable']
SSDP_NJOBS = int(PVGRIP_CONFIGS['ssdp']['njobs'])

COPERNICUS_ADS_CREDENTIALS = Credentials_Circle\
    (config_fn=PVGRIP_CONFIGS['copernicus']['credentials_ads'],
     redis_url = REDIS_URL)
COPERNICUS_CDS_CREDENTIALS = Credentials_Circle\
    (config_fn=PVGRIP_CONFIGS['copernicus']['credentials_cds'],
     redis_url = REDIS_URL)
COPERNICUS_CDS_HASH_LENGTH = \
    int(PVGRIP_CONFIGS['copernicus']['cds_hash_length'])
COPERNICUS_ADS_HASH_LENGTH = \
    int(PVGRIP_CONFIGS['copernicus']['ads_hash_length'])

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
    'pvgrip.weather',
    'pvgrip.webserver',
]


def get_RESULTS_CACHE():
    return Files_LRUCache\
        (path = RESULTS_PATH,
         maxsize = int(PVGRIP_CONFIGS['cache']['limit_worker']))


def get_IPFS_STORAGE():
    return IPFS_Files\
        (ipfs_ip = _IPFS_STORAGE_IP,
         ipfs_timeout = _IPFS_TIMEOUT,
         cluster_ips = [_CASSANDRA_STORAGE_IP],
         keyspace_suffix = _IPFS_STORAGE_KEYSPACE_SUFFIX,
         replication = _CASSANDRA_REPLICATION,
         replication_args = _CASSANDRA_REPLICATION_ARGS,
         protocol_version = 4,
         connect_timeout = 60,
         idle_heartbeat_timeout = 300,
         control_connection_timeout = 30)


def get_LOCAL_STORAGE(remotetype):
    if not re.match('localmount_.*', remotetype):
        raise RuntimeError\
            ("remotetype = {} does not match localmount_*")

    return LOCALIO_Files(root = _LOCAL_STORAGE_ROOTS[remotetype],
                         redis_url = REDIS_URL)


def get_SPATIAL_DATA():
    if 'ipfs_path' == DEFAULT_REMOTE:
        storage = get_IPFS_STORAGE()
    elif re.match('localmount_.*', DEFAULT_REMOTE):
        storage = get_LOCAL_STORAGE(DEFAULT_REMOTE)
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
                            redis_url = REDIS_URL,
                            expire_time = REDIS_EXPIRES)
