import os
import diskcache

from open_elevation.utils \
    import git_root
from cassandra_io.files \
    import Cassandra_Files
from open_elevation.spatial_data \
    import Spatial_Data
from open_elevation.files_lrucache \
    import Files_LRUCache


RESULTS_PATH = os.path.join(git_root(),'data','results_cache')


_CASSANDRA_STORAGE_IP = '172.17.0.2'
_CASSANDRA_STORAGE_CHUNKSIZE = 1048576
_CASSANDRA_STORAGE_KEYSPACE_SUFFIX = '_open_elevation'
_CASSANDRA_REPLICATION = 'SimpleStrategy'
_CASSANDRA_REPLICATION_ARGS = {'replication_factor': 1}


GRASS="grass78"


def get_RESULTS_CACHE():
    return Files_LRUCache(path = RESULTS_PATH,
                          maxsize = 150)


def get_CASSANDRA_STORAGE():
    return  Cassandra_Files\
        (cluster_ips = [_CASSANDRA_STORAGE_IP],
         keyspace_suffix = _CASSANDRA_STORAGE_KEYSPACE_SUFFIX,
         chunk_size = _CASSANDRA_STORAGE_CHUNKSIZE,
         replication = _CASSANDRA_REPLICATION,
         replication_args = _CASSANDRA_REPLICATION_ARGS)


def get_SPATIAL_DATA():
    CASSANDRA_STORAGE = get_CASSANDRA_STORAGE()
    return Spatial_Data(CASSANDRA_STORAGE)
