import celery

from ipfs_io.exceptions \
    import FAILED_METADATA

from pvgrip.utils.exceptions \
    import TASK_RUNNING

from cassandra.cluster \
    import NoHostAvailable


class WithRetry(celery.Task):
    autoretry_for = (FAILED_METADATA,TASK_RUNNING,
                     NoHostAvailable,)
    retry_backoff = True
    max_retries = 10
