import json
import subprocess

from celery.worker.control import inspect_command


def _call(what='active'):
    ps = subprocess\
        .Popen(['celery','-A',\
                'open_elevation.celery_tasks',\
                'inspect',what,'-j'],
               stdout = subprocess.PIPE)
    res = subprocess.run(['head','-n','-2'],
                         stdin = ps.stdout,
                         stdout = subprocess.PIPE)
    return json.loads(res.stdout.decode())


def status():
    return {'active': _call('active'),
            'scheduled': _call('scheduled'),
            # 'uptime': _call('inspect_uptime'),
            # 'free': _call('inspect_free')
            }


@inspect_command()
def inspect_uptime():
    res = subprocess.run(['uptime'],
                         stdout = subprocess.PIPE)
    return res.stdout.decode()


@inspect_command()
def inspect_free():
    res = subprocess.run(['free', '-h'],
                         stdout = subprocess.PIPE)
    return res.stdout.decode()
