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

echo "Push to a remote registry"
./pvgrip.sh --what=push -d
./pvgrip.sh --what=push

echo "Prune older images"
./pvgrip.sh --what=prune -d
./pvgrip.sh --what=prune

read -p "Label the release as 'latest'? (y/n)" choice
case "${choice}" in
    y|Y)
        echo "Tagging a release"
        ./pvgrip.sh --what=tag --next-tag=latest -d
        ./pvgrip.sh --what=tag --next-tag=latest

        echo "Push to a remote registry"
        ./pvgrip.sh --what=push --next-tag=latest -d
        ./pvgrip.sh --what=push --next-tag=latest
        ;;
    *)
        exit
        ;;
esac
