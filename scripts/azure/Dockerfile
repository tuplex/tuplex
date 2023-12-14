FROM ubuntu:22.04

ARG GIT_HASH
ARG GIT_REMOTE

ENV PATH=/opt/bin:$PATH

RUN mkdir -p /opt/sbin

RUN echo "Building tuplex for commit ${GIT_HASH} / ${GIT_REMOTE}"

RUN echo "Install MongoDB"
ADD install_mongodb.sh /opt/sbin/install_mongodb.sh
RUN bash /opt/sbin/install_mongodb.sh

RUN echo "Install required packages"
ADD install_azure_ci_reqs.sh /opt/sbin/install_azure_ci_reqs.sh
RUN bash /opt/sbin/install_azure_ci_reqs.sh

RUN echo 'Install python dependencies'
RUN apt-get update -y && apt-get install -y python3 python3-pip python3-setuptools ninja-build && python3 -m pip install pytest pygments>=2.4.1 MarkupSafe==2.0 pexpect setuptools astor PyYAML jupyter nbformat pymongo eventlet==0.30.0 gunicorn pymongo && jupyter --version

RUN echo "Clone tuplex and checkout"
# Install git & add github to known hosts
RUN apt-get install -y git && mkdir -p /root/.ssh/ && touch /root/.ssh/known_hosts && ssh-keyscan github.com >>/root/.ssh/known_hosts
RUN mkdir -p /code && cd /code && git clone "${GIT_REMOTE}" tuplex && cd tuplex && git checkout "${GIT_HASH}"


RUN echo 'Test local MongoDB'
RUN cd /code/tuplex/python && python3 -m pip install -r requirements.txt && python3 mongodb_test.py && pkill mongod || true

RUN echo "Build Tuplex"
RUN cd /code/tuplex && TUPLEX_BUILD_ALL=1 CMAKE_ARGS="-DBUILD_WITH_ORC=ON -DLLVM_ROOT_DIR=/usr/lib/llvm-16 -DCMAKE_BUILD_TYPE=Release -DBUILD_FOR_CI=ON" python3 setup.py install --user

RUN echo "C++ tests"
RUN cd /code/tuplex && cd build/temp.linux-x86_64-3.10 && ctest --timeout 180 --output-on-failure --repeat until-pass:3 -j 2

RUN echo "Python tests"
RUN cd /code/tuplex && cd build/temp.linux-x86_64-3.10/dist/python && python3.10 -m pytest -x --full-trace -l --log-cli-level=DEBUG --capture=tee-sys
