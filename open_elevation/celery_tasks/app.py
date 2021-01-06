import celery


CELERY_APP = celery.Celery(broker='amqp://guest:guest@127.0.0.1:5672//',
                           backend='cache+memcached://127.0.0.1:11211/',
                           task_track_started=True)
