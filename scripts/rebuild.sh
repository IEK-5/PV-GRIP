#!/bin/bash

cd $(git rev-parse --show-toplevel)

echo "Update repository"
git stash
git fetch
git reset --hard origin/dev
git stash pop

echo "Building image"
./pvgrip --what=build -d
./pvgrip --what=build

echo "Tagging a release"
./pvgrip --what=tag -d
./pvgrip --what=tag

echo "Push to a remote registry"
./pvgrip --what=push -d
./pvgrip --what=push

echo "Prune older images"
./pvgrip --what=prune -d
./pvgrip --what=prune

read -p "Label the release as 'latest'? (y/n)" choice
case "${choice}" in
    y|Y)
        echo "Tagging a release"
        ./pvgrip --what=tag --next-tag=latest -d
        ./pvgrip --what=tag --next-tag=latest

        echo "Push to a remote registry"
        ./pvgrip --what=push --next-tag=latest -d
        ./pvgrip --what=push --next-tag=latest
        ;;
    *)
        exit
        ;;
esac
