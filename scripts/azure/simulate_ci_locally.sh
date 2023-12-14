#!/usr/bin/env bash

echo "Retrieving last commit has"
GIT_HASH=`git rev-parse --short HEAD`

GIT_REMOTE=`git remote -v | cut -f2 | head -n 1 | cut -f1 -d ' '`
GIT_REMOTE="https://github.com/$(git remote get-url origin | sed 's/https:\/\/github.com\///' | sed 's/git@github.com://')"
echo "Building docker image for hash ${GIT_HASH}, remote ${GIT_REMOTE}"
docker build --no-cache -t tuplex/azure --build-arg GIT_HASH=${GIT_HASH} --build-arg GIT_REMOTE=${GIT_REMOTE} .

