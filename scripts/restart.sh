#!/bin/bash

cd $(git rev-parse --show-toplevel)

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

read -p "Restart worker? (y/n)" choice
case "${choice}" in
    y|Y)
        docker kill pvgrip-worker
        ./pvgrip.sh --what=worker
        ;;
    *)
        ;;
esac

read -p "Restart webserver? (y/n)" choice
case "${choice}" in
    y|Y)
        docker kill pvgrip-webserver
        ./pvgrip.sh --what=webserver
        ;;
    *)
        ;;
esac
