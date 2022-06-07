import celery

from ipfs_io.exceptions \
    import FAILED_METADATA

from pvgrip.utils.exceptions \
    import TASK_RUNNING

from pvgrip.utils.timeout \
    import TIMEOUT

from pvgrip.storage.remotestorage_path \
    import NotInStorage

from cassandra.cluster \
    import NoHostAvailable

from redis.exceptions \
    import BusyLoadingError, \
    ConnectionError

from pvgrip.osm.overpass_error import OverpassAPIError

class WithRetry(celery.Task):
    autoretry_for = (FAILED_METADATA,TASK_RUNNING, TIMEOUT,
                     NoHostAvailable, BusyLoadingError,
                     ConnectionError, NotInStorage, OverpassAPIError)
    retry_backoff = True
    max_retries = 10
