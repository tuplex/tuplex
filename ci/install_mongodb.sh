#!/usr/bin/env bash
#(c) 2017-2023 Tuplex team

set -euxo pipefail

apt install -y wget curl gnupg2 software-properties-common apt-transport-https ca-certificates lsb-release

curl -fsSL https://www.mongodb.org/static/pgp/server-6.0.asc| gpg --dearmor -o /etc/apt/trusted.gpg.d/mongodb-6.gpg

echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/6.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list

apt update -y

apt install -y mongodb-org
