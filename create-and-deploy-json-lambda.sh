#!/usr/bin/env bash

echo "Creating (JSON serializastion) based Lambda"
./scripts/create_lambda_zip.sh

echo "running deploy script..."
python3.6 deploy.py
echo "done!"
