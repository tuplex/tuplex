#!/usr/bin/env bash

set -ex

# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

# Install Rust (without the stupid prompt)
apt-get update && apt-get install -y curl git build-essential

curl https://sh.rustup.rs -sSf | sh -s -- -y &&
echo "export PATH=\$HOME/.cargo/bin:\$PATH" >> $HOME/.bashrc &&
export PATH="$HOME/.cargo/bin:$PATH" && source $HOME/.cargo/env

export LLVM_SYS_60_PREFIX=/opt/llvm-6.0
export PATH=/opt/llvm-6.0/bin/:$PATH

pushd /tmp
git clone https://github.com/weld-project/weld.git && cd weld \
&& export PATH=$HOME/.cargo/bin:$PATH && rustup default nightly \
&& export WELD_HOME=`pwd` && export PATH=/opt/llvm-6.0/bin/:$PATH \
&& cargo build --release && cd .. && mv weld /opt/weld
popd

# Install Weld/Grizzly. Only python2 supported for this, hence use python2
# need to fix Makefile -> copy Makefile fix
export PATH=/opt/llvm-6.0/bin/:$PATH
export WELD_HOME=/opt/weld
cp $CWD/PatchedGrizzlyMakefile.make /opt/weld/python/grizzly/grizzly/Makefile

apt-get update && apt install -y python2 python2-dev && curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py && python2 get-pip.py && pip2 install pandas numpy
cd /opt/weld/python && cd pyweld && python2 setup.py install && cd .. && cd grizzly && python2 setup.py install
