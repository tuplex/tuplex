FROM ubuntu:20.04

# invoke (from this dir) using

# Tuplex container to run benchmarks compatible to pywren...

RUN DEBIAN_FRONTEND="noninteractive" apt-get update && apt-get -y install tzdata

RUN apt-get update && apt-get install -y git software-properties-common wget

RUN add-apt-repository ppa:deadsnakes/ppa && apt-get update && apt-get install -y python3.7

RUN cd /tmp && wget https://bootstrap.pypa.io/get-pip.py && python3.7 get-pip.py

WORKDIR /work

ADD tuplex-0.3.1-cp37-cp37m-manylinux_2_17_x86_64.manylinux2014_x86_64.whl /work/tuplex-0.3.1-cp37-cp37m-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

RUN python3.7 -m pip install /work/tuplex-0.3.1-cp37-cp37m-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

RUN python3.7 -m pip install awscli pandas

# add benchmark files
ADD benchmark.sh /work/benchmark.sh
ADD run_tuplex.py /work/run_tuplex.py
