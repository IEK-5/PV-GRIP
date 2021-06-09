#!/bin/bash

cd $(git rev-parse --show-toplevel)

echo "Update repository"
git stash
git fetch
git reset --hard origin/dev
git stash pop

echo "Pull latest"
./pvgrip --what=pull -d
./pvgrip --what=pull

echo "Prune older images"
./pvgrip --what=prune -d
./pvgrip --what=prune

read -p "Restart worker? (y/n)" choice
case "${choice}" in
    y|Y)
        docker kill pvgrip-worker
        ./pvgrip --what=worker
        ;;
    *)
        ;;
esac

read -p "Restart webserver? (y/n)" choice
case "${choice}" in
    y|Y)
        docker kill pvgrip-webserver
        ./pvgrip --what=webserver
        ;;
    *)
        ;;
esac
