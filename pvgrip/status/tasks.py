import subprocess

from celery.worker.control import inspect_command


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
