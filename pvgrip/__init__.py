import celery

from pvgrip.globals \
    import REDIS_URL, AUTODISCOVER_TASKS


CELERY_APP = celery.Celery(broker=REDIS_URL,
                           backend=REDIS_URL,
                           task_track_started=True)
CELERY_APP.autodiscover_tasks(AUTODISCOVER_TASKS)
CELERY_APP.conf.task_routes = {'pvgrip.weather.tasks.retrieve_source':
                               {'queue': 'requests'}}
