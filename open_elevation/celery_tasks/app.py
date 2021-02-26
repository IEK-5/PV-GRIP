import celery

from open_elevation.globals \
    import REDIS_URL

CELERY_APP = celery.Celery(broker=REDIS_URL,
                           backend=REDIS_URL,
                           task_track_started=True)
