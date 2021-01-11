import os
import celery
import subprocess, sys

from open_elevation.results_lrucache \
    import ResultFiles_LRUCache


def git_root():
    res = subprocess.run(["git","rev-parse","--show-toplevel"],
                         stdout=subprocess.PIPE).\
                         stdout.decode().split('\n')[0]
    return res


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

_RESULTS_PATH = os.path.join(git_root(),'data','results_cache')
RESULTS_CACHE = ResultFiles_LRUCache(path = _RESULTS_PATH,
                                     maxsize = 2)
