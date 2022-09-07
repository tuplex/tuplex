
FROM ubuntu:18.04
    
# build using docker build -t tuplex/ubuntu:1804 .

MAINTAINER Leonhard Spiegelberg "leonhard@brown.edu"

RUN mkdir -p /opt/sbin

ADD install_mongodb.sh /opt/sbin/install_mongodb.sh
RUN /opt/sbin/install_mongodb.sh

ADD install_requirements.sh /opt/sbin/install_requirements.sh
RUN /opt/sbin/install_requirements.sh
