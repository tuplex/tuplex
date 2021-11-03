#!/usr/bin/env bash
# helper script to start a local mongodb instance

mkdir -p db
mkdir -p db/data
mkdir -p db/logs
echo "Starting MongoDB daemon"
mongod --fork --dbpath db/data --logpath db/logs/mongod.log --port 27017 
echo "done!"
