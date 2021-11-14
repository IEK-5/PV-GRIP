#!/bin/bash

cd $(git rev-parse --show-toplevel)

webserver="no"
worker="no"
flower="no"
broker="no"

for i in "$@"
do
    case "${i}" in
        webserver)
            webserver="yes"
            ;;
        worker)
            worker="yes"
            ;;
        flower)
            flower="yes"
            ;;
        broker)
            broker="yes"
            ;;
        *)
            echo "unknown argument!"
            exit
            ;;
    esac
done


echo "Update repository"
git stash
git fetch
git reset --hard origin/dev
git stash pop
git submodule sync
git submodule update --init configs/secret

echo "Pull latest"
./pvgrip.sh --what=pull -d
./pvgrip.sh --what=pull

echo "Prune older images"
./pvgrip.sh --what=prune -d
./pvgrip.sh --what=prune

if [ "${worker}" = "yes" ]
then
    docker kill pvgrip-worker
    ./pvgrip.sh --what=worker
    docker kill pvgrip-worker_requests
    ./pvgrip.sh --what=worker_requests
fi

if [ "${webserver}" = "yes" ]
then
    docker kill pvgrip-webserver
    ./pvgrip.sh --what=webserver
fi

if [ "${flower}" = "yes" ]
then
    docker kill pvgrip-flower
    ./pvgrip.sh --what=flower
fi

if [ "${broker}" = "yes" ]
then
    docker kill pvgrip-broker
    ./pvgrip.sh --what=broker
fi
