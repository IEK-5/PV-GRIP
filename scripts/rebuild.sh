#!/bin/bash

cd $(git rev-parse --show-toplevel)

echo "Update repository"
git stash
git fetch
git reset --hard origin/dev
git stash pop

echo "Building image"
./pvgrip.sh --what=build -d
./pvgrip.sh --what=build

echo "Tagging a release"
./pvgrip.sh --what=tag -d
./pvgrip.sh --what=tag
./pvgrip.sh --what=tag --registry=esovetkin -d
./pvgrip.sh --what=tag --registry=esovetkin

echo "Push to a remote registry"
./pvgrip.sh --what=push -d
./pvgrip.sh --what=push
./pvgrip.sh --what=push --registry=esovetkin -d
./pvgrip.sh --what=push --registry=esovetkin

echo "Prune older images"
./pvgrip.sh --what=prune -d
./pvgrip.sh --what=prune
./pvgrip.sh --what=prune --registry=esovetkin -d
./pvgrip.sh --what=prune --registry=esovetkin

read -p "Label the release as 'latest'? (y/n)" choice
case "${choice}" in
    y|Y)
        echo "Tagging a release"
        ./pvgrip.sh --what=tag --next-tag=latest -d
        ./pvgrip.sh --what=tag --next-tag=latest
        ./pvgrip.sh --what=tag --next-tag=latest --registry=esovetkin -d
        ./pvgrip.sh --what=tag --next-tag=latest --registry=esovetkin

        echo "Push to a remote registry"
        ./pvgrip.sh --what=push --next-tag=latest -d
        ./pvgrip.sh --what=push --next-tag=latest
        ./pvgrip.sh --what=push --next-tag=latest --registry=esovetkin -d
        ./pvgrip.sh --what=push --next-tag=latest --registry=esovetkin
        ;;
    *)
        exit
        ;;
esac
