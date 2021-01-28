import json
import subprocess


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


def _uptime():
    res = subprocess.run(['uptime'],
                         stdout = subprocess.PIPE)
    return res.stdout.decode()


def _free():
    res = subprocess.run(['free', '-h'],
                         stdout = subprocess.PIPE)
    return res.stdout.decode()


def status():
    return {'active': _call('active'),
            'scheduled': _call('scheduled'),
            'uptime': _uptime(),
            'free': _free()}
