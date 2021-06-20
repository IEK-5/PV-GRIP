#!/bin/env python3

import os
import sys

from pvgrip.globals \
    import get_SPATIAL_DATA, GIT_ROOT


def print_usage(cmd):
    print("""
    %s <path>

    path  a path that must be a subdirectory of data/current
    """)


if __name__ == '__main__':
    if 2 != len(sys.argv):
        print_usage(sys.argv[0])
        sys.exit()

    path = os.path.realpath(sys.argv[1])
    data_path = os.path.join\
        (GIT_ROOT,'data','current')

    if os.path.commonprefix([path, data_path]) != data_path:
        print_usage(sys.argv[0])
        sys.exit()

    data = get_SPATIAL_DATA()
    data.upload(path)
