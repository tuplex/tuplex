#!/usr/bin/env bash

# TODO: CentOS/RHEL does not support AWS SDK. It's triggering a bug in NSS which is the SSL lib used in CentOS/RHEL. Therefore, use a m
# cf. https://github.com/aws/aws-sdk-cpp/issues/1491

# Steps to solve:
# 1.) install recent OpenSSL
# 2.) build Curl against it
# 3.) Compile AWS SDK with this curl version.
#cf. https://geekflare.com/curl-installation/ for install guide


# other mentions of the NSS problem:
# https://curl.se/mail/lib-2016-08/0119.html
# https://bugzilla.mozilla.org/show_bug.cgi?id=1297397

CURL_VERSION=7.80.0

#cd /tmp && yum update -y && yum install wget gcc openssl-devel -y && \
#wget --no-check-certificate https://curl.se/download/curl-${CURL_VERSION}.tar.gz && tar xf curl-${CURL_VERSION}.tar.gz && \
#cd curl-${CURL_VERSION} && ./configure --with-openssl --without-nss && make -j 16 && make install && ldconfig


#could also just install via cmake... https://github.com/curl/curl

# on CentOS, an old curl compiled with NSS is preinstalled.
# ==> remove!
# rm -rf /usr/lib64/libcurl*


cd /tmp && yum update -y && yum install wget gcc openssl-devel -y && rm -rf /usr/lib64/libcurl* && \
wget --no-check-certificate https://curl.se/download/curl-${CURL_VERSION}.tar.gz && tar xf curl-${CURL_VERSION}.tar.gz && \
cd curl-${CURL_VERSION} && ./configure --with-openssl --without-nss --prefix=/usr/ --libdir=/usr/lib64 && make -j 16 && make install && ldconfig

## remove centos curl/libssl/nss
#rpm -e --nodeps libcurl curl nss && ldconfig

