#!/usr/bin/env bash
# (c) L.Spiegelberg 2020
# create docker image

#!/usr/bin/env bash
# (c) 2021 Tuplex contributors
# builds benchmark image

while :; do
    case $1 in
        -u|--upload) UPLOAD="SET"
        ;;
        *) break
    esac
    shift
done

# build benchmark docker image
# copy from scripts to current dir because docker doesn't understand files
# outside the build context
cp ../../ubuntu1804/install_reqs.sh .
docker build -t tuplex/ubuntu1804 . || exit 1

# is upload set?
if [[ "${UPLOAD}" == 'SET' ]]; then
 docker login
 docker push tuplex/ubuntu1804
fi
