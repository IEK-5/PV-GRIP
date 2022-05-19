#!/bin/bash

docker kill pvgrip-broker pvgrip-webserver pvgrip-worker pvgrip-worker_requests pvgrip-flower

./pvgrip.sh --what=broker
./pvgrip.sh --what=worker --registry="" --ifmntcode="yes"
./pvgrip.sh --what=worker_requests --registry="" --ifmntcode="yes"
./pvgrip.sh --what=webserver --registry="" --ifmntcode="yes"
./pvgrip.sh --what=flower --registry="" --ifmntcode="yes"
