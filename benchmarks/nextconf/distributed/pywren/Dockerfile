FROM ubuntu:20.04

# invoke (from this dir) using

# PyWren container to run benchmarks

RUN DEBIAN_FRONTEND="noninteractive" apt-get update && apt-get -y install tzdata

RUN apt-get update && apt-get install -y git software-properties-common wget

RUN add-apt-repository ppa:deadsnakes/ppa && apt-get update && apt-get install -y python3.7

RUN cd /tmp && wget https://bootstrap.pypa.io/get-pip.py && python3.7 get-pip.py

WORKDIR /work

RUN git clone https://github.com/LeonhardFS/pywren.git && cd pywren && python3.7 setup.py install

RUN python3.7 -m pip install awscli pandas

# add .pywrenconfig file
ADD .pywren_config /root/.pywren_config

# add benchmark files
ADD benchmark.sh /work/benchmark.sh
ADD run_pywren.py /work/run_pywren.py
