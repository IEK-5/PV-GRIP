#!/bin/env python3
import sys

from open_elevation.globals \
    import PVGRIP_CONFIGS

print(PVGRIP_CONFIGS[sys.argv[1]][sys.argv[2]])
