#!/usr/bin/env bash
# installs MongoDB instance
# adapted from https://www.digitalocean.com/community/tutorials/how-to-install-mongodb-on-ubuntu-18-04-source
# and https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/
# needs sudo

curl -fsSL https://www.mongodb.org/static/pgp/server-5.0.asc | apt-key add -
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/5.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-5.0.list
apt update
apt install -y mongodb-org
