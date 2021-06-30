#!/bin/bash
# (c) L.Spiegelberg
# helper script that copies source code folder to docker container ($1)

if [ ${#@} -ne 1 ]; then
    echo "Usage: $0 <container>"
    echo "* <containerid>: the hash to identify the container where things should get copied to, e.g. 687c7386ce33."
    exit 1
fi

docker cp adapters $1:/code
docker cp codegen $1:/code
docker cp core $1:/code
docker cp experiments $1:/code
docker cp io $1:/code
docker cp main $1:/code
docker cp python $1:/code
docker cp runtime $1:/code
docker cp scripts $1:/code
docker cp test $1:/code
docker cp utils $1:/code
docker cp CMakeLists.txt $1:/code
docker cp cmake $1:/code
docker cp doc $1:/code
docker cp docker $1:/code
