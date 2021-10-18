import celery

from pvgrip.globals \
    import REDIS_URL, REDIS_EXPIRES, AUTODISCOVER_TASKS


CELERY_APP = celery.Celery(broker=REDIS_URL,
                           backend=REDIS_URL,
                           task_track_started=True,
                           result_expires = REDIS_EXPIRES,
                           enable_utc = True)
CELERY_APP.autodiscover_tasks(AUTODISCOVER_TASKS)
CELERY_APP.conf.task_routes = {'pvgrip.weather.tasks.retrieve_source':
                               {'queue': 'requests'}}
