#!/usr/bin/env bash

# TODO: CentOS/RHEL does not support AWS SDK. It's triggering a bug in NSS which is the SSL lib used in CentOS/RHEL.
# cf. https://github.com/aws/aws-sdk-cpp/issues/1491

# Steps to solve:
# 1.) install recent OpenSSL
# 2.) build Curl against it
# 3.) Compile AWS SDK with this curl version.
# cf. https://geekflare.com/curl-installation/ for install guide

# other mentions of the NSS problem:
# https://curl.se/mail/lib-2016-08/0119.html
# https://bugzilla.mozilla.org/show_bug.cgi?id=1297397

# select here which curl version to use
CURL_VERSION=7.80.0

# Alternative could be to also just install via cmake, i.e. from repo https://github.com/curl/curl.

# Main issue is, that on CentOS an old curl compiled with NSS is preinstalled.
# ==> remove!
# i.e., via rm -rf /usr/lib64/libcurl*

NUM_PROCS=$(( 1 * $( egrep '^processor[[:space:]]+:' /proc/cpuinfo | wc -l ) ))

cd /tmp && apt-get update && apt-get install wget gcc -y && rm -rf /usr/lib64/libcurl* && rm -rf /usr/lib/libcurl* && \
wget --no-check-certificate https://curl.se/download/curl-${CURL_VERSION}.tar.gz && tar xf curl-${CURL_VERSION}.tar.gz && \
cd curl-${CURL_VERSION} && ./configure --with-openssl --without-nss --prefix=/usr/ --libdir=/usr/lib64 && make -j ${NUM_PROCS} && make install && ldconfig
