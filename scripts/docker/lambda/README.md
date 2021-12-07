# Lambda container
This directory contains a docker image based on https://github.com/JetBrains/clion-remote/blob/master/Dockerfile.remote-cpp-env to facilitate local development of the Lambda runner

In order to build the docker file,
use

docker build -t tuplex/lambda .


To use the container, either startup via docker compose or use following snippet:

```
docker run -d --cap-add sys_ptrace -p127.0.0.1:2222:22 --name clion_remote_env tuplex/lambda
```

Then use the guide from https://www.jetbrains.com/help/clion/clion-toolchains-in-docker.html#create-docker-toolchain to set everything up. You need CLion 2021.3 for this to work!
