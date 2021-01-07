import celery


CELERY_APP = celery.Celery(broker='redis://localhost:6379/0',
                           backend='redis://localhost:6379/0',
                           task_track_started=True)

CELERY_APP.conf.ONCE = {
    'backend': 'celery_once.backends.Redis',
    'settings': {
        'url': 'redis://localhost:6379/0',
        'default_timeout': 60*60
    }
}
