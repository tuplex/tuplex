# Docker images for Tuplex
This folder contains different subfolders which each hold scripts/data to create the different tuplex docker images.

- `tuplex`: This image holds a preinstalled tuplex version together with jupyter notebooks - ready to be used (Ubuntu 20.04 based).
- `benchmark`: This image holds all dependencies + other frameworks that are used to benchmark against in the SIGMOD'21 paper (Ubuntu 20.04 based).
- `ci`: Image based on manylinux2014 image to build against multiple python versions for different linux distros (CentOS/RedHat based).

To create the images, each folder has a file `create-image.sh` which may copy in additional scripts from the scripts folder, name the image if `--upload` is given, upload it to the tuplex organization (you need to be part of it and logged in to perform a successful upload).
