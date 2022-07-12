#!/usr/bin/env bash
# script to use BBSN00 to build lambda executor, download it and store it in build-lambda/tplxlam.zip

if [[ -z "${BBSN00_USER}" ]]; then
	echo "please specify BBSN00_USER variable"
	exit 1
fi

# go to home dir, check out current remote & branch and start build
LOCAL_BRANCH=`git name-rev --name-only HEAD`
TRACKING_BRANCH=`git config branch.$LOCAL_BRANCH.merge`
TRACKING_REMOTE=`git config branch.$LOCAL_BRANCH.remote`
REMOTE_URL=`git config remote.$TRACKING_REMOTE.url`

HOSTNAME=10.116.60.19
HOSTNAME=bbsn00

echo "Building Lambda executor for branch ${LOCAL_BRANCH} (${TRACKING_BRANCH})"

ssh ${BBSN00_USER}@${HOSTNAME} bash -c 'cd /home/'${BBSN00_USER}'/tuplex-public && pwd && ls'

iscp lspiegel@bbsn00:/home/lspiegel/tuplex-public/build-lambda/*.zip build-lambda/

