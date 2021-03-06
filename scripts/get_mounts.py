#!/usr/bin/env python3
import os
import sys
import subprocess
import configparser


def git_root():
    res = subprocess.run(["git","rev-parse","--show-toplevel"],
                         stdout=subprocess.PIPE).\
                         stdout.decode().split('\n')[0]
    return res


def get_configs(fn):
    config = configparser.ConfigParser()
    config.read(fn)
    return config


GIT_ROOT = git_root()
PVGRIP_CONFIGS = get_configs\
    (os.path.join(GIT_ROOT,'configs','pvgrip.conf'))
names = list(PVGRIP_CONFIGS['docker_localmount'])

print(' '.join(['-v {}'\
                .format(PVGRIP_CONFIGS['docker_localmount'][x]) \
                for x in names]))
