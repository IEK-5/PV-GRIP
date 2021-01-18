import re
import os
import time
import logging
import tempfile
import subprocess


class TASK_RUNNING(Exception):
    pass


def git_root():
    res = subprocess.run(["git","rev-parse","--show-toplevel"],
                         stdout=subprocess.PIPE).\
                         stdout.decode().split('\n')[0]
    return res


def if_in_celery():
    import open_elevation.celery_tasks.app \
        as app
    if not app.CELERY_APP.current_worker_task:
        return False
    return True


def list_files(path, regex):
    r = re.compile(regex)
    return [os.path.join(dp, f) \
            for dp, dn, filenames in \
            os.walk(path) \
            for f in filenames \
            if r.match(os.path.join(dp, f))]


def get_tempfile(path = os.path.join(git_root(),
                                     'data','tempfiles')):
    os.makedirs(path,exist_ok = True)
    fd = tempfile.NamedTemporaryFile(dir = path, delete = False)
    return os.path.join(path,fd.name)


def remove_file(fn):
    try:
        if fn:
            os.remove(fn)
    except:
        logging.error("cannot remove file: %s" % fn)
        pass
