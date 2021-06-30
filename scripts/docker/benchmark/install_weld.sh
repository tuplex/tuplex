#!/usr/bin/env bash
# Install Rust (without the stupid prompt)

curl https://sh.rustup.rs -sSf | sh -s -- -y &&
echo "export PATH=\$HOME/.cargo/bin:\$PATH" >> $HOME/.bashrc &&
export PATH="$HOME/.cargo/bin:$PATH" && source $HOME/.cargo/env

export LLVM_SYS_60_PREFIX=/opt/llvm-6.0
export PATH=/opt/llvm-6.0/bin/:$PATH

cd /usr/src &&
git clone https://github.com/weld-project/weld.git && cd weld \
&& export PATH=$HOME/.cargo/bin:$PATH && rustup default nightly \
&& export WELD_HOME=`pwd` && export PATH=/opt/llvm-6.0/bin/:$PATH \
&& cargo build --release && cd .. && mv weld /opt/weld