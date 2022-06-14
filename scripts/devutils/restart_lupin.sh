#!/bin/bash

git push --force

ssh lupin <<'ENDSSH'
cd pvgrip

cd $(git rev-parse --show-toplevel)

echo "Update repository"
git stash
git fetch
git reset --hard @{u}
git stash pop
docker kill pvgrip-broker pvgrip-webserver pvgrip-worker pvgrip-worker_requests pvgrip-flower

./pvgrip.sh --what=broker
./pvgrip.sh --what=worker --registry="" --ifmntcode="yes"
./pvgrip.sh --what=worker_requests --registry="" --ifmntcode="yes"
./pvgrip.sh --what=webserver --registry="" --ifmntcode="yes"
./pvgrip.sh --what=flower --registry="" --ifmntcode="yes"
ENDSSH