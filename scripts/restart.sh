#!/bin/bash

cd $(git rev-parse --show-toplevel)

webserver="no"
worker="no"

for i in "$@"
do
    case "${i}" in
        webserver)
            webserver="yes"
            ;;
        worker)
            worker="yes"
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
fi

if [ "${webserver}" = "yes" ]
then
    docker kill pvgrip-webserver
    ./pvgrip.sh --what=webserver
fi
