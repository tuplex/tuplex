#!/usr/bin/env bash
#(c) 2017-2023 Tuplex team

set -euxo pipefail
apt-get update && apt-get install -y curl gnupg \
    && curl -fsSL https://www.mongodb.org/static/pgp/server-5.0.asc | apt-key add - \
    && echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-5.0.list \
    && apt update \
    && apt install -y mongodb-org