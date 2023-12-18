#!/usr/bin/env bash
# (c) 2017 - 2023 L.Spiegelberg
# runs all command to package website

rm -rf build

# sphinx build
make html

# reorg files, delete doctree
rm -rf build/doctrees
mv build/html/* build/
rm -rf build/html

# manual fix for svg file
cp -r source/img/* build/_images/

# remove unnecessary files that are auto-generated
rm build/search.html
rm build/searchindex.js
rm build/genindex.html
rm -rf build/_sources
rm build/objects.inv
rm build/contents.html
