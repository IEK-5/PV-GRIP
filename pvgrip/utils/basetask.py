import celery

from ipfs_io.exceptions \
    import FAILED_METADATA

from pvgrip.utils.exceptions \
    import TASK_RUNNING


class WithRetry(celery.Task):
    autoretry_for = (FAILED_METADATA,TASK_RUNNING,)
    retry_backoff = True
    max_retries = 10
