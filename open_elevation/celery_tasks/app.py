import celery

REDIS_URL = 'redis://localhost:6379/0'

CELERY_APP = celery.Celery(broker=REDIS_URL,
                           backend=REDIS_URL,
                           task_track_started=True)
