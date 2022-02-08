#!/usr/bin/env bash
# Install Rust (without the stupid prompt)

curl https://sh.rustup.rs -sSf | sh -s -- -y &&
echo "export PATH=\$HOME/.cargo/bin:\$PATH" >> $HOME/.bashrc &&
export PATH="$HOME/.cargo/bin:$PATH" && source $HOME/.cargo/env

export LLVM_SYS_60_PREFIX=/opt/llvm-6.0
export PATH=/opt/llvm-6.0/bin/:$PATH

cd /usr/src &&
git clone https://github.com/LeonhardFS/weld.git && git checkout --track origin/py3fixes && cd weld \
&& export PATH=$HOME/.cargo/bin:$PATH && rustup default nightly \
&& export WELD_HOME=`pwd` && export PATH=/opt/llvm-6.0/bin/:$PATH \
&& cargo build --release && cd .. && mv weld /opt/weld

# install grizzly
export PATH=/opt/llvm-6.0/bin/:$PATH
export LLVM_SYS_60_PREFIX=/opt/llvm-6.0
python3.6 -m pip install setuptools_rust
cd /opt/weld/weld-python && python3.6 setup.py install

cd /opt/weld/python/grizzly && python3.6 setup.py install


# Install Weld/Grizzly. Only python2 supported for this, hence use python2
# need to fix Makefile -> copy Makefile fix
cp PatchedGrizzlyMakefile.make /opt/weld/python/grizzly/grizzly/Makefile

apt-get update && apt install -y python2 python2-dev && curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py && python2 get-pip.py && pip2 install pandas numpy
cd /opt/weld/python && cd pyweld && python2 setup.py install && cd .. && cd grizzly && python2 setup.py install 
