
FROM ubuntu:18.04
    
# build using docker build -t tuplex/ubuntu:1804 .

MAINTAINER Leonhard Spiegelberg "leonhard@brown.edu"

RUN mkdir -p /opt/sbin


ENV PATH "/opt/bin:$PATH"
RUN echo "export PATH=/opt/bin:${PATH}" >> /root/.bashrc
RUN apt-get update && apt-get install -y python3

ADD install_mongodb.sh /opt/sbin/install_mongodb.sh
RUN /opt/sbin/install_mongodb.sh
ADD install_requirements.sh /opt/sbin/install_requirements.sh
RUN /opt/sbin/install_requirements.sh

RUN python3.6 -m pip install "cloudpickle<2.0" cython numpy

RUN python3.6 -m pip install "cloudpickle<2.0" cython numpy
