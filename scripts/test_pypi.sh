#!/usr/bin/env bash
# checks out tuplex from test.pypi
python3 --version
cat ./scripts/dev.version
echo "tuplex==$(cat ./scripts/dev.version)"
python3 -m pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple "tuplex==$(cat ./scripts/dev.version)"
python3 -c 'import tuplex; c = tuplex.Context()'