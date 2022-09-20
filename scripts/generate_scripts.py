#!/usr/bin/env python3
# coding: utf-8
# (c) 2017 - 2022 Tuplex team
# this is a helper script to generate all the install scripts for various OSs but with the same versions.
# when upgrading, changing versions - change them here
import os
import glob
import json
import datetime
import logging

### CHANGE VERSIONS HERE ###

# configure here which versions to use for each platform
VERSIONS = {}
def configure_versions(osname):
    global VERSIONS

    VERSIONS['OS'] = osname

    VERSIONS['CMAKE_VERSION'] = '3.23.3'
    VERSIONS['YAMLCPP_VERSION'] = '0.6.3'
    VERSIONS['CELERO_VERSION'] = '2.8.3'
    VERSIONS['ANTLR_VERSION'] = '4.8'
    VERSIONS['AWSSDK_VERSION'] = '1.9.320'  # '1.9.133'
    VERSIONS['AWSLAMBDACPP_VERSION'] = '0.2.6'
    VERSIONS['PCRE2_VERSION'] = '10.39'
    VERSIONS['PROTOBUF_VERSION'] = '3.21.5'
    VERSIONS['CLOUDPICKLE_VERSION'] = '<2.0.0'
    VERSIONS['BOOST_VERSION'] = '1.79.0'
    VERSIONS['GCC_VERSION'] = '10'
    VERSIONS['LLVM_VERSION'] = '9.0.1'

    VERSIONS['MONGODB_VERSION'] = '5.0'

    VERSIONS['WORKDIR'] = '/tmp'
    VERSIONS['PREFIX'] = '/opt'

    # for ubuntu 18.04 use gcc7
    if '18.04' in osname:
        VERSIONS['GCC_VERSION'] = '7'

    # for 20.04 use gcc10
    if '20.04' in osname:
        VERSIONS['GCC_VERSION'] = '10'
    # for ubuntu 22.04 use gcc11
    if '22.04' in osname:
        VERSIONS['GCC_VERSION'] = '11.2'

    # derived vars
    VERSIONS['GCC_VERSION_MAJOR'] = VERSIONS['GCC_VERSION'].split('.')[0]
    CMAKE_VERSION = VERSIONS['CMAKE_VERSION']
    VERSIONS['CMAKE_VERSION_MAJOR'], VERSIONS['CMAKE_VERSION_MINOR'], VERSIONS['CMAKE_VERSION_PATCH'] = tuple(
        CMAKE_VERSION.split('.'))


### helper functions ###

def workdir_setup():
    WORKDIR=VERSIONS['WORKDIR']
    PREFIX=VERSIONS['PREFIX']
    return """PREFIX=${{PREFIX:-{}}}
WORKDIR=${{WORKDIR:-{}}}

echo ">> Installing packages into ${{PREFIX}}"
mkdir -p $PREFIX && chmod 0755 $PREFIX
mkdir -p $PREFIX/sbin
mkdir -p $PREFIX/bin
mkdir -p $PREFIX/share
mkdir -p $PREFIX/include
mkdir -p $PREFIX/lib

echo ">> Files will be downloaded to ${{WORKDIR}}/tuplex-downloads"
WORKDIR=$WORKDIR/tuplex-downloads
mkdir -p $WORKDIR""".format(PREFIX, WORKDIR)

def bash_header(install_prefix='/opt', workdir='/tmp'):
    current_year = datetime.datetime.now().year
    current_date = str(datetime.datetime.now())
    return '''#!/usr/bin/env bash
# (c) Tuplex team 2017-{}
# auto-generated on {}
# install all dependencies required to compile tuplex + whatever is needed for profiling
# everything will be installed to {} by default'''.format(current_year, current_date, install_prefix) + \
'\n' + '''
# need to run this with root privileges
if [[ $(id -u) -ne 0 ]]; then
  echo "Please run this script with root privileges"
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

""" + workdir_setup() + """

PYTHON_EXECUTABLE=${{PYTHON_EXECUTABLE:-python3}}
PYTHON_BASENAME="$(basename -- $PYTHON_EXECUTABLE)"
PYTHON_VERSION=$(${{PYTHON_EXECUTABLE}} --version)
echo ">> Building dependencies for ${{PYTHON_VERSION}}"

'''.format(install_prefix, workdir)

def install_mongodb(osname='ubuntu:22.04'):

    # cf. https://www.mongodb.com/docs/v5.0/tutorial/install-mongodb-on-ubuntu/

    MONGODB_VERSION=VERSIONS['MONGODB_VERSION']
    if osname == 'ubuntu:18.04':
        return  """apt-get update && apt-get install -y curl gnupg \\
&& curl -fsSL https://www.mongodb.org/static/pgp/server-{version}.asc | apt-key add - \\
&& echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/{version} multiverse" | tee /etc/apt/sources.list.d/mongodb-org-{version}.list \\
&& apt update \\
&& apt install -y mongodb-org""".format(version=MONGODB_VERSION)
    elif osname == 'ubuntu:20.04':
        return """apt-get update && apt-get install -y curl gnupg \\
    && curl -fsSL https://www.mongodb.org/static/pgp/server-{version}.asc | apt-key add - \\
    && echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/{version} multiverse" | tee /etc/apt/sources.list.d/mongodb-org-{version}.list \\
    && apt update \\
    && apt install -y mongodb-org""".format(version=MONGODB_VERSION)
    elif osname == 'ubuntu:22.04':
        # cf. guide at https://wiki.crowncloud.net/?How_to_Install_Latest_MongoDB_on_Ubuntu_22_04
        # and https://www.mongodb.com/community/forums/t/installing-mongodb-over-ubuntu-22-04/159931/35, there is no mongodb release for 22.04 yet...
        return "echo 'no support for Ubuntu 22.04 for MongoDB, watch out for future release.'"
    #     return """apt update && apt install -y curl gnupg dirmngr gnupg apt-transport-https ca-certificates software-properties-common \\
    # && curl -fsSL https://www.mongodb.org/static/pgp/server-{version}.asc | apt-key add - \\
    # && echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/{version} multiverse" | tee /etc/apt/sources.list.d/mongodb-org-{version}.list \\
    # && apt update \\
    # && apt install -y mongodb-org""".format(version=MONGODB_VERSION)
    else:
        raise Exception('unknown os {}'.format(osname))
   
def tiny_bash_header():
    current_year = datetime.datetime.now().year
    current_date = str(datetime.datetime.now())
    return '#!/usr/bin/env bash\n#(c) 2017-{} Tuplex team\n\n'.format(current_year)

def write_bash_file(path, content, header=tiny_bash_header()):
    with open(path, 'w') as fp:
        fp.write(header)
        fp.write(content)
    os.chmod(path, 0o700) # rwx------


def apt_dependencies(osname='ubuntu:22.04'):
    GCC_VERSION_MAJOR = VERSIONS['GCC_VERSION_MAJOR']

    packages_dict = {'ubuntu:22.04': '''apt-utils dh-autoreconf libmagic-dev curl libxml2-dev vim build-essential libssl-dev zlib1g-dev libncurses5-dev \\
    libncursesw5-dev libreadline-dev libsqlite3-dev libgdbm-dev libdb5.3-dev \\
    libbz2-dev libexpat1-dev liblzma-dev tk-dev libffi-dev wget git libcurl4-openssl-dev python3-dev python3-pip openjdk-8-jre-headless''',
                     'ubuntu:20.04': '''software-properties-common dh-autoreconf curl build-essential wget git libedit-dev libz-dev \\
                   python3-yaml python3-pip pkg-config libssl-dev libcurl4-openssl-dev curl \\
                   uuid-dev libffi-dev libmagic-dev \\
                   doxygen doxygen-doc doxygen-latex doxygen-gui graphviz \\
                   libgflags-dev libncurses-dev \\
                   openjdk-8-jdk libyaml-dev ninja-build gcc-{} g++-{} autoconf libtool m4
                     '''.format(GCC_VERSION_MAJOR, GCC_VERSION_MAJOR),
                     'ubuntu:18.04': '''build-essential apt-utils wget git dh-autoreconf libxml2-dev \\
 autoconf curl automake libtool software-properties-common wget libedit-dev libz-dev \\
  python3-yaml pkg-config libssl-dev libcurl4-openssl-dev curl \\
  uuid-dev git python3.7 python3.7-dev python3-pip libffi-dev \\
  doxygen doxygen-doc doxygen-latex doxygen-gui graphviz \\
  gcc-{} g++-{} libgflags-dev libncurses-dev \\
  awscli openjdk-8-jdk libyaml-dev libmagic-dev ninja-build
                    '''.format(GCC_VERSION_MAJOR, GCC_VERSION_MAJOR)}

    return 'apt update -y\n' + \
           '''
           apt-get install -y {}
           
           ldconfig
           export CC=gcc-{}
           export CXX=g++-{}
           '''.format(packages_dict[osname], GCC_VERSION_MAJOR, GCC_VERSION_MAJOR)

def yum_dependencies():
    return """yum install -y libedit-devel libzip-devel \
  pkgconfig openssl-devel libxml2-devel zlib-devel  \
  uuid libuuid-devel libffi-devel graphviz-devel \
  gflags-devel ncurses-devel \
  awscli java-1.8.0-openjdk-devel libyaml-devel file-devel ninja-build zip unzip ninja-build --skip-broken
"""

def github_to_known_hosts(home='/root'):
    return """# add github to known hosts
mkdir -p {home}/.ssh/ &&
  touch {home}/.ssh/known_hosts &&
  ssh-keyscan github.com >> {home}/.ssh/known_hosts""".format(home=home)


def install_cmake():
    CMAKE_VERSION_MAJOR = VERSIONS['CMAKE_VERSION_MAJOR']
    CMAKE_VERSION_MINOR = VERSIONS['CMAKE_VERSION_MINOR']
    CMAKE_VERSION_PATCH = VERSIONS['CMAKE_VERSION_PATCH']

    return '''
# fetch recent cmake & install
CMAKE_VER_MAJOR={}
CMAKE_VER_MINOR={}
CMAKE_VER_PATCH={}'''.format(CMAKE_VERSION_MAJOR, CMAKE_VERSION_MINOR, CMAKE_VERSION_PATCH) + '''
CMAKE_VER="${CMAKE_VER_MAJOR}.${CMAKE_VER_MINOR}"
CMAKE_VERSION="${CMAKE_VER}.${CMAKE_VER_PATCH}"
URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz
mkdir -p ${WORKDIR}/cmake && cd ${WORKDIR}/cmake &&
  curl -sSL $URL -o cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  tar -v -zxf cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  rm -f cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  cd cmake-${CMAKE_VERSION}-linux-x86_64 &&
  cp -rp bin/* ${PREFIX}/bin/ &&
  cp -rp share/* ${PREFIX}/share/ &&
  cd / && rm -rf ${WORKDIR}/cmake
    '''


def install_boost():
    # may need to include the following to work for boost python
    # # fix up for boost python a link
    # cd /usr/local/include/ && ln -s python3.6m python3.6 && cd - || exit

    BOOST_VERSION = VERSIONS['BOOST_VERSION']

    boost_reformatted = BOOST_VERSION.replace('.', '_')

    # boost has a good amount of issues, following will result in a bug for GCC
    # -> this line in $PREFIX/include/boost/thread/pthread/thread_data.hpp
    # #if PTHREAD_STACK_MIN > 0
    # should be replaced with
    # #ifdef PTHREAD_STACK_MIN

    boost_fix = " && sed -i 's/#if PTHREAD_STACK_MIN > 0/#ifdef PTHREAD_STACK_MIN/g' ${PREFIX}/include/boost/thread/pthread/thread_data.hpp"
    return '''    
mkdir -p ${WORKDIR}/boost

# build incl. boost python
pushd ${WORKDIR}/boost && ''' + \
           'wget https://boostorg.jfrog.io/artifactory/main/release/{}/source/boost_{}.tar.gz && tar xf boost_{}.tar.gz'.format(
               BOOST_VERSION, boost_reformatted, boost_reformatted, boost_reformatted) + \
           ' && cd ${{WORKDIR}}/boost/boost_{}'.format(boost_reformatted) + \
           ''' \\
           && ./bootstrap.sh --with-python=${PYTHON_EXECUTABLE} --prefix=${PREFIX} --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time" \\
            && ./b2 cxxflags="-fPIC" link=static -j "$(nproc)" \\
            && ./b2 cxxflags="-fPIC" link=static install''' + boost_fix


def install_protobuf():
    PROTOBUF_VERSION = VERSIONS['PROTOBUF_VERSION']

    code = 'mkdir -p ${WORKDIR}/protobuf && cd ${WORKDIR}/protobuf \\'

    code += '''\n&& curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v{minversion}/protobuf-cpp-{version}.tar.gz \\
&& tar xf protobuf-cpp-{version}.tar.gz \\
&& cd protobuf-{version} \\
&& ./autogen.sh && ./configure "CFLAGS=-fPIC" "CXXFLAGS=-fPIC" \\
&& make -j$(nproc) && make install && ldconfig'''.format(version=PROTOBUF_VERSION,
                                                         minversion=PROTOBUF_VERSION.replace('3.', ''))

    return code


def install_pcre2():
    PCRE2_VERSION = VERSIONS['PCRE2_VERSION']

    code = 'mkdir -p ${WORKDIR}/pcre2 && cd ${WORKDIR}/pcre2 \\'

    code += '''\n&& curl -LO https://github.com/PhilipHazel/pcre2/releases/download/pcre2-{version}/pcre2-{version}.zip \\
&& unzip pcre2-{version}.zip \\
&& rm pcre2-{version}.zip \\
&& cd pcre2-{version} \\
&& ./configure CFLAGS="-O2 -fPIC" --prefix=${{PREFIX}} --enable-jit=auto --disable-shared \\
&& make -j$(nproc) && make install'''.format(version=PCRE2_VERSION)

    return code

def install_aws_sdk():
    # install BOTH aws sdk and LAMBDA runtime
    
    AWSSDK_VERSION=VERSIONS['AWSSDK_VERSION']
    AWSLAMBDACPP_VERSION=VERSIONS['AWSLAMBDACPP_VERSION']
    
    code = 'mkdir -p ${WORKDIR}/aws && cd ${WORKDIR}/aws \\'
    
    code += '''\n&&  git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git \\
&& cd aws-sdk-cpp && git checkout tags/{version} && mkdir build && cd build \\
&& cmake -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=14 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" -DCMAKE_INSTALL_PREFIX=${{PREFIX}} .. \\
&& make -j$(nproc) \\
&& make install'''.format(version=AWSSDK_VERSION)

    code += '\n\n#installing AWS Lambda C++ runtime\n'
    code += '\ncd ${WORKDIR}/aws \\\n'
    code += '''&& git clone https://github.com/awslabs/aws-lambda-cpp.git \\
&& cd aws-lambda-cpp \\
&& git fetch && git fetch --tags \\
&& git checkout v{version} \\
&& mkdir build \\
&& cd build \\
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${{PREFIX}} .. \\
&& make -j$(nproc) && make install'''.format(version=AWSLAMBDACPP_VERSION)
    
    return code


def install_aws_sdk_macos():

    AWSSDK_VERSION=VERSIONS['AWSSDK_VERSION']
    AWSLAMBDACPP_VERSION=VERSIONS['AWSLAMBDACPP_VERSION']

    code = '''#!/usr/bin/env bash

echo ">> installing AWS SDK from source"
CPU_CORES=$(sysctl -n hw.physicalcpu)

# if macOS is 10.x -> use this as minimum
MINIMUM_TARGET="-DCMAKE_OSX_DEPLOYMENT_TARGET=10.13"

MACOS_VERSION=$(sw_vers -productVersion)
echo "-- processing on MacOS ${MACOS_VERSION}"
function version { echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }'; }

if [ $(version $VAR) -ge $(version "11.0.0") ]; then
    echo "Newer MacOS detected, all good."
    MINIMUM_TARGET=""
else
    # keep as is
    echo "defaulting build to use as minimum target ${MINIMUM_TARGET}"
fi

cd /tmp &&
  git clone --recurse-submodules https://github.com/aws/aws-sdk-cpp.git &&
  cd aws-sdk-cpp && git checkout tags/''' + AWSSDK_VERSION + ''' && mkdir build && pushd build &&
  cmake ${MINIMUM_TARGET} -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=14 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;core;lambda;transfer" .. &&
  make -j${CPU_CORES} &&
  make install &&
  popd &&
  cd - || echo ">> error: AWS SDK failed"
'''
    return code

def install_antlr():
    # install both ANTLR tool and C++ runtime
    ANTLR_VERSION=VERSIONS['ANTLR_VERSION']
    
    ANTLR_MAJMIN = '.'.join(ANTLR_VERSION.split('.')[:2])
    
    code = 'mkdir -p ${WORKDIR}/antlr && cd ${WORKDIR}/antlr \\\n'
    
    code += '''&& curl -O https://www.antlr.org/download/antlr-{majmin}-complete.jar \\
&& cp antlr-{majmin}-complete.jar ${{PREFIX}}/lib/ \\\n'''.format(majmin=ANTLR_MAJMIN)
    
    # install runtime
    code += '''&& curl -O https://www.antlr.org/download/antlr4-cpp-runtime-{majmin}-source.zip \\
&& unzip antlr4-cpp-runtime-{majmin}-source.zip -d antlr4-cpp-runtime \\
&& rm antlr4-cpp-runtime-{majmin}-source.zip \\
&& cd antlr4-cpp-runtime \\
&& mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${{PREFIX}} .. \\
&& make -j$(nproc) && make install'''.format(majmin=ANTLR_MAJMIN)
    
    return code

def install_yaml():
    YAMLCPP_VERSION=VERSIONS['YAMLCPP_VERSION']
    
    # install yaml cpp
    code = 'mkdir -p ${WORKDIR}/yamlcpp && cd ${WORKDIR}/yamlcpp \\'
    
    code += '''\n&& git clone https://github.com/jbeder/yaml-cpp.git yaml-cpp \\
&& cd yaml-cpp \\
&& git checkout tags/yaml-cpp-{version} \\
&& mkdir build && cd build \\
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${{prefix}} -DYAML_CPP_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC" .. \\
&& make -j$(nproc) && make install'''.format(version=YAMLCPP_VERSION)
    
    return code

def install_celero():
    CELERO_VERSION=VERSIONS['CELERO_VERSION']
    
    # install ycelero
    code = 'mkdir -p ${WORKDIR}/celero && cd ${WORKDIR}/celero \\'
    
    code += '''\n&&  git clone https://github.com/DigitalInBlue/Celero.git celero && cd celero \\
&& git checkout tags/v{version} \\
&& mkdir build && cd build \\
&& cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${{PREFIX}} -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC -std=c++11" .. \\
&& make -j$(nproc) && make install'''.format(version=CELERO_VERSION)
    
    return code

def bashrc_helper():
    ANTLR_VERSION=VERSIONS['ANTLR_VERSION']
    PREFIX=VERSIONS['PREFIX']
    
    code = 'echo "Consider adding the following lines to your ~/.bash_profile, ~/.bashrc or other shell config file:"\n\n'
    # could print out the following hints for bashrc
    
    # # setup bash aliases
    # echo "alias antlr='java -jar /opt/lib/antlr-4.8-complete.jar'" >>"$HOME/.bashrc"
    # echo "alias grun='java org.antlr.v4.gui.TestRig'" >>"$HOME/.bashrc"

    # # update include/link paths to /opt
    # echo "export CPLUS_INCLUDE_PATH=/opt/include:\${CPLUS_INCLUDE_PATH}" >> "$HOME/.bashrc"
    # echo "export C_INCLUDE_PATH=/opt/include:\${C_INCLUDE_PATH}" >> "$HOME/.bashrc"
    # echo "export LD_LIBRARY_PATH=/opt/lib:\${LD_LIBRARY_PATH}" >> "$HOME/.bashrc"
    
    ANTLR_MAJMIN = '.'.join(ANTLR_VERSION.split('.')[:2])
    ANTLR_MAJ=ANTLR_VERSION.split('.')[0]
    
    code += """
alias antlr='java -jar /opt/lib/antlr-{}-complete.jar'
alias grun='java org.antlr.v{}.gui.TestRig'

export PATH={prefix}/bin:$PATH
export CPLUS_INCLUDE_PATH={prefix}/include:${{CPLUS_INCLUDE_PATH}}
export C_INCLUDE_PATH={prefix}/include:${{C_INCLUDE_PATH}}
export LD_LIBRARY_PATH={prefix}/lib:${{LD_LIBRARY_PATH}}
    """.format(ANTLR_MAJMIN, ANTLR_MAJ, prefix=PREFIX)
    return code

def install_llvm():
    
    GCC_VERSION_MAJOR=VERSIONS['GCC_VERSION_MAJOR']
    LLVM_VERSION=VERSIONS['LLVM_VERSION']
    
    cxx_flags = '-std=c++11'
    
    # GCC11+ requires explicit include of limits etc. cf.https://stackoverflow.com/questions/4798936/numeric-limits-was-not-declared-in-this-scope-no-matching-function-for-call-t
    if int(GCC_VERSION_MAJOR) >= 11:
        cxx_flags += ' -include /usr/include/c++/11/limits'
    
    
    LLVM_VERSION_MAJOR=LLVM_VERSION.split('.')[0]
    LLVM_VERSION_MINOR=LLVM_VERSION.split('.')[1]
    LLVM_MAJMIN = '{}.{}'.format(LLVM_VERSION_MAJOR, LLVM_VERSION_MINOR)
    workdir='${WORKDIR}/llvm'
    code = 'mkdir -p {} && cd {} && '.format(workdir, workdir)  + \
'''wget https://github.com/llvm/llvm-project/releases/download/llvmorg-{version}/llvm-{version}.src.tar.xz \\
&& wget https://github.com/llvm/llvm-project/releases/download/llvmorg-{version}/clang-{version}.src.tar.xz \\
&& tar xf llvm-{version}.src.tar.xz && tar xf clang-{version}.src.tar.xz \\'''.format(version=LLVM_VERSION)

    cmake_base_flags = '''-DLLVM_ENABLE_RTTI=ON -DLLVM_ENABLE_EH=ON \\
        -DLLVM_ENABLE_PROJECTS="clang" \\
         -DLLVM_TARGETS_TO_BUILD="X86" -DCMAKE_BUILD_TYPE=Release'''
    
    if int(LLVM_VERSION_MAJOR) < 10:
    
        # LLVM version < 10
        code += '''\n&& mkdir llvm{maj} && mv clang-{version}.src llvm{maj}/clang \\
    && mv llvm-{version}.src llvm{maj}/llvm-{version}.src \\
    && cd llvm{maj}'''.format(version=LLVM_VERSION, maj=LLVM_VERSION_MAJOR)
        
        cmake_flags = cmake_base_flags
        
    else:
        # LLVM version >= 10
        code += '''\n&& mkdir llvm{maj} && mv clang-{version}.src llvm{maj}/clang && mv cmake llvm{maj}/cmake \\
    && mv llvm-{version}.src llvm{maj}/llvm-{version}.src \\
    && cd llvm{maj}'''.format(version=LLVM_VERSION, maj=LLVM_VERSION_MAJOR)

        cmake_flags = cmake_base_flags + ' -DLLVM_INCLUDE_BENCHMARKS=OFF'
        
        
    code += ''' && mkdir build && cd build \\
&& cmake {cmake_flags} -DCMAKE_CXX_FLAGS="{cxx_flags}" \\
         -DCMAKE_INSTALL_PREFIX=/opt/llvm-{majmin} ../llvm-{version}.src \\
         && make -j "$(nproc)" && make install'''.format(cmake_flags=cmake_flags, majmin=LLVM_MAJMIN, version=LLVM_VERSION, cxx_flags=cxx_flags)
    
    return code


def make_ubuntu_req_file(path, osname='ubuntu:18.04'):
    
    configure_versions(osname)
    
    WORKDIR=VERSIONS['WORKDIR']
    PREFIX=VERSIONS['PREFIX']

    os_tag = osname.split(':')[-1]
    # create Ubuntu req file
    with open(path, 'w') as fp:
        fp.write(bash_header().strip())
        
        fp.write('\necho ">> Installing all build dependencies for Tuplex under Ubuntu {}"\n'.format(os_tag))

        fp.write('\necho ">> Installing apt dependencies"\n')
        fp.write(apt_dependencies(osname).strip())

        fp.write('\n\necho ">> Installing recent cmake"\n')
        fp.write(install_cmake().strip())

        # use for following commands, newly installed cmake!
        fp.write('\n\nexport PATH=$PREFIX/bin:$PATH\ncmake --version\n')

        fp.write('\n\necho ">> Installing Boost"\n')
        fp.write(install_boost().strip())

        fp.write('\n\necho ">> Installing LLVM"\n')
        fp.write(install_llvm().strip())

        fp.write('\n\necho ">> Installing PCRE2"\n')
        fp.write(install_pcre2().strip())

        fp.write('\n\necho ">> Installing Celero"\n')
        fp.write(install_celero().strip())

        fp.write('\n\necho ">> Installing YAMLCPP"\n')
        fp.write(install_yaml().strip())

        fp.write('\n\necho ">> Installing ANTLR"\n')
        fp.write(install_antlr().strip())

        fp.write('\n\necho ">> Installing protobuf"\n')
        fp.write(install_protobuf().strip())

        fp.write('\n\necho ">> Installing AWS SDK"\n')
        fp.write(install_aws_sdk().strip())

        fp.write('\n\necho ">> Cleaning/removing workdir {}"'.format(WORKDIR))
        fp.write('\nrm -rf ${WORKDIR}\n')
        fp.write('\necho "-- Done, all Tuplex requirements installed to {} --"\n'.format(PREFIX))
        
    os.chmod(path, 0o700)



def generate_ubuntu1804(root_folder):
    # write ubuntu 18.04 files
    osname = 'ubuntu:18.04'
    configure_versions(osname)
    os.makedirs(root_folder, exist_ok=True)
    write_bash_file(os.path.join(root_folder, 'install_mongodb.sh'), install_mongodb(osname))

    # write reqs file
    make_ubuntu_req_file(os.path.join(root_folder, 'install_requirements.sh'), osname)

    # write corresponding docker file
    docker_content = '\nFROM ubuntu:18.04\n    \n' \
                     '# build using docker build -t tuplex/ubuntu:18.04 .\n\n' \
                     'MAINTAINER Leonhard Spiegelberg "leonhard@brown.edu"\n\n' \
                     'ENV DEBIAN_FRONTEND=noninteractive\n' \
                     'RUN mkdir -p /opt/sbin\n\n' \
                     '\nENV PATH "/opt/bin:$PATH"\n' \
                     'RUN echo "export PATH=/opt/bin:${PATH}" >> /root/.bashrc\n' \
                     'RUN apt-get update && apt-get install -y python3\n\n' \
                     'ADD install_mongodb.sh /opt/sbin/install_mongodb.sh\n' \
                     'RUN /opt/sbin/install_mongodb.sh\n' \
                     'ADD install_requirements.sh /opt/sbin/install_requirements.sh\n' \
                     'RUN /opt/sbin/install_requirements.sh\n'

    # install cloudpickle < 2.0 & numpy
    # python3.6 -m pip install 'cloudpickle<2.0' cython snumpy
    # python3.7 -m pip install 'cloudpickle<2.0' cython numpy
    docker_content += '\nRUN python3.6 -m pip install "cloudpickle<2.0" cython numpy\n' \
                      '\nRUN python3.6 -m pip install "cloudpickle<2.0" cython numpy\n'

    with open(os.path.join(root_folder, "Dockerfile"), 'w') as fp:
        fp.write(docker_content)

def generate_ubuntu2004(root_folder):
    # write ubuntu 20.04 files
    osname = 'ubuntu:20.04'
    configure_versions(osname)
    os.makedirs(root_folder, exist_ok=True)
    write_bash_file(os.path.join(root_folder, 'install_mongodb.sh'), install_mongodb(osname))

    # write reqs file
    make_ubuntu_req_file(os.path.join(root_folder, 'install_requirements.sh'), osname)

    # write corresponding docker file
    docker_content = '\nFROM ubuntu:20.04\n    \n' \
                     '# build using docker build -t tuplex/ubuntu:20.04 .\n\n' \
                     'MAINTAINER Leonhard Spiegelberg "leonhard@brown.edu"\n\n' \
                     'ENV DEBIAN_FRONTEND=noninteractive\n' \
                     'RUN mkdir -p /opt/sbin\n\n' \
                     '\nENV PATH "/opt/bin:$PATH"\n' \
                     'RUN echo "export PATH=/opt/bin:${PATH}" >> /root/.bashrc\n' \
                     'RUN apt-get update && apt-get install -y python3\n\n' \
                     'ADD install_mongodb.sh /opt/sbin/install_mongodb.sh\n' \
                     'RUN /opt/sbin/install_mongodb.sh\n' \
                     'ADD install_requirements.sh /opt/sbin/install_requirements.sh\n' \
                     'RUN /opt/sbin/install_requirements.sh\n'

    # install cloudpickle < 2.0 & numpy
    # python3.6 -m pip install 'cloudpickle<2.0' cython snumpy
    # python3.7 -m pip install 'cloudpickle<2.0' cython numpy
    docker_content += '\nRUN python3.8 -m pip install "cloudpickle<2.0" cython numpy\n'

    with open(os.path.join(root_folder, "Dockerfile"), 'w') as fp:
        fp.write(docker_content)

def generate_ubuntu2204(root_folder):
    # write ubuntu 22.04 files
    osname = 'ubuntu:22.04'
    configure_versions(osname)
    os.makedirs(root_folder, exist_ok=True)
    write_bash_file(os.path.join(root_folder, 'install_mongodb.sh'), install_mongodb(osname))

    # write reqs file
    make_ubuntu_req_file(os.path.join(root_folder, 'install_requirements.sh'), osname)

    # write corresponding docker file
    docker_content = '\nFROM ubuntu:22.04\n    \n' \
                     '# build using docker build -t tuplex/ubuntu:22.04 .\n\n' \
                     'MAINTAINER Leonhard Spiegelberg "leonhard@brown.edu"\n\n' \
                     'ENV DEBIAN_FRONTEND=noninteractive\n' \
                     'RUN mkdir -p /opt/sbin\n\n' \
                     '\nENV PATH "/opt/bin:$PATH"\n' \
                     'RUN echo "export PATH=/opt/bin:${PATH}" >> /root/.bashrc\n' \
                     'RUN apt-get update && apt-get install -y python3\n\n' \
                     'ADD install_mongodb.sh /opt/sbin/install_mongodb.sh\n' \
                     'RUN /opt/sbin/install_mongodb.sh\n' \
                     'ADD install_requirements.sh /opt/sbin/install_requirements.sh\n' \
                     'RUN /opt/sbin/install_requirements.sh\n'

    # install cloudpickle >2.0 & numpy
    docker_content += '\nRUN python3 -m pip install "cloudpickle>2.0" cython numpy\n'

    with open(os.path.join(root_folder, "Dockerfile"), 'w') as fp:
        fp.write(docker_content)

def create_lambda_python(PYTHON_VERSION):
    # currently it is 3.9.13 -> could change anytime, AWS updates it.

    code = '''#!/usr/bin/env bash
# to build the lambda executor need to embed python, therefore create full version below

export CFLAGS=-I/usr/include/openssl

# use Python 3.9 runtime
PYTHON3_VERSION={version}
PYTHON3_MAJMIN=${{PYTHON3_VERSION%.*}}'''.format(version=PYTHON_VERSION) + \
'''
# from https://bugs.python.org/issue36044
# change tasks, because hangs at test_faulthandler...
export PROFILE_TASK=-m test.regrtest --pgo \
        test_collections \
        test_dataclasses \
        test_difflib \
        test_embed \
        test_float \
        test_functools \
        test_generators \
        test_int \
        test_itertools \
        test_json \
        test_logging \
        test_long \
        test_ordered_dict \
        test_pickle \
        test_pprint \
        test_re \
        test_set \
        test_statistics \
        test_struct \
        test_tabnanny \
        test_xml_etree

set -ex && cd /tmp && wget https://www.python.org/ftp/python/${PYTHON3_VERSION}/Python-${PYTHON3_VERSION}.tgz && tar xf Python-${PYTHON3_VERSION}.tgz \
    && cd Python-${PYTHON3_VERSION} && ./configure --with-lto --prefix=/opt/lambda-python --enable-optimizations --enable-shared \
    && make -j $(( 1 * $( egrep '^processor[[:space:]]+:' /proc/cpuinfo | wc -l ) )) \
    && make altinstall

# install cloudpickle numpy for Lambda python
export LD_LIBRARY_PATH=/opt/lambda-python/lib:$LD_LIBRARY_PATH
/opt/lambda-python/bin/python${PYTHON3_MAJMIN} -m pip install 'cloudpickle<2.0.0' numpy tqdm'''

    return code


def generate_manylinux_files(root_folder):
    os.makedirs(root_folder, exist_ok=True)
    configure_versions('centos')

    # write install_boost.sh script
    with open(os.path.join(root_folder, 'install_boost.sh'), 'w') as fp:
        header = tiny_bash_header() + """# this a script to install boost for specific python version to some folder
PYTHON_EXECUTABLE=$1
PREFIX=$2
PYTHON_VERSION="$(basename -- $PYTHON_EXECUTABLE)"
echo ">>> building boost for ${PYTHON_VERSION}"
echo " -- boost will be installed to ${PREFIX}"

mkdir -p $DEST_PATH

# fix up for boost python a link
INCLUDE_DIR=$(echo $PYTHON_EXECUTABLE | sed 's|/bin/.*||')
INCLUDE_DIR=${INCLUDE_DIR}/include
cd $INCLUDE_DIR && ln -s ${PYTHON_VERSION}m ${PYTHON_VERSION} && cd - || exit 1
"""
        fp.write(header)
        fp.write('\n')
        fp.write(install_boost())

    # write install_lambda_python.sh
    with open(os.path.join(root_folder, 'install_lambda_python.sh'), 'w') as fp:
        fp.write(create_lambda_python('3.9.13'))

    # write install_tuplex_reqs.sh
    with open(os.path.join(root_folder, 'install_tuplex_reqs.sh'), 'w') as fp:
        fp.write(tiny_bash_header())
        fp.write('\n# install all build dependencies for tuplex (CentOS)\n')
        
        fp.write(workdir_setup() + '\n')

        fp.write(yum_dependencies() + '\n')
        fp.write(github_to_known_hosts('/root') + '\n')

        # yaml cpp
        fp.write('\n\necho ">> Installing YAMLCPP"\n')
        fp.write(install_yaml().strip())

        # celero
        fp.write('\n\necho ">> Installing Celero"\n')
        fp.write(install_celero().strip())

        # antlr4 + cpp runtime
        fp.write('\n\necho ">> Installing ANTLR"\n')
        fp.write(install_antlr().strip())

        # aws sdk
        fp.write('\n\necho ">> Installing AWS SDK"\n')
        fp.write(install_aws_sdk().strip())

        # pcre2
        fp.write('\n\necho ">> Installing PCRE2"\n')
        fp.write(install_pcre2().strip())

        # protobuf
        fp.write('\n\necho ">> Installing protobuf"\n')
        fp.write(install_protobuf().strip())

    # write install_llvm9.sh yum edition
    with open(os.path.join(root_folder, 'install_llvm.sh'), 'w') as fp:

        LLVM_VERSION=VERSIONS['LLVM_VERSION']
        LLVM_MAJMIN_VERSION='{}.{}'.format(LLVM_VERSION.split('.')[0], LLVM_VERSION.split('.')[1])

        fp.write(tiny_bash_header())
        fp.write('\n# install LLVM {} to use for building wheels\n'.format(LLVM_VERSION))
        fp.write('\n' + workdir_setup() + '\n')
        fp.write('\nyum update && yum install -y wget libxml2-devel\n')
        fp.write(install_llvm())
        fp.write('\ncd ${{PREFIX}}/llvm-{majmin}/bin && ln -s clang++ clang++-{majmin}'.format(majmin=LLVM_MAJMIN_VERSION))

    # keep the docker file as is there...


# create Ubunti 22.04 req file
def create_llvm14_file(path, llvm_version='14.0.6'):
    configure_versions('ubuntu:22.04')
    WORKDIR=VERSIONS['WORKDIR']
    PREFIX=VERSIONS['PREFIX']
    with open(path, 'w') as fp:
        fp.write(bash_header().strip())

        LLVM_VERSION=llvm_version

        fp.write('\n\necho ">> Installing LLVM"\n')
        fp.write(install_llvm().strip())

        fp.write('\n\necho ">> Cleaning/removing workdir {}"'.format(WORKDIR))
        fp.write('\nrm -rf ${WORKDIR}\n')
        fp.write('\necho "-- Done, all Tuplex requirements installed to {} --"\n'.format(PREFIX))


def generate_yaml_req_file(path, osname='ubuntu:18.04'):
    configure_versions(osname)

    with open(path, 'w') as fp:
        fp.write(bash_header() + '\n')

        fp.write(workdir_setup() + '\n')

        fp.write('\nexport DEBIAN_FRONTEND=noninteractive\n')

        # use python3.7 from deadsnakes for yaml
        py37_install = """# add recent python3.7 package, confer https://linuxize.com/post/how-to-install-python-3-7-on-ubuntu-18-04/
apt install -y software-properties-common \\
&& add-apt-repository -y ppa:deadsnakes/ppa \\
&& apt-get update\n"""
        fp.write(py37_install)

        # apt install
        apt_install = """apt-get install -y build-essential autoconf automake libtool software-properties-common wget libedit-dev libz-dev \\
  python3-yaml pkg-config libssl-dev libcurl4-openssl-dev curl \\
  uuid-dev git python3.7 python3.7-dev python3-pip libffi-dev \\
  doxygen doxygen-doc doxygen-latex doxygen-gui graphviz \\
  gcc-7 g++-7 libgflags-dev libncurses-dev \\
  awscli openjdk-8-jdk libyaml-dev libmagic-dev ninja-build"""

        fp.write(apt_install + '\n')

        # faster llvm get
        llvm_install = """# LLVM 9 packages (prob not all of them needed, but here for complete install)
wget https://apt.llvm.org/llvm.sh && chmod +x llvm.sh \\
&& ./llvm.sh 9 && rm -rf llvm.sh"""

        fp.write(llvm_install + '\n')

        link_update = """# set gcc-7 as default
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-7 70 --slave /usr/bin/g++ g++ /usr/bin/g++-7
# set python3.7 as default
update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 70 --slave /usr/bin/python3m python3m /usr/bin/python3.7m
# upgrade pip
python3.7 -m pip install --upgrade pip\n"""
        fp.write(link_update + '\n')

        fp.write(install_cmake() + '\n')

        fp.write(github_to_known_hosts('/root') + '\n')

        # can we do this quicker?
        fp.write(install_boost() + '\n')

        fp.write(install_yaml() + '\n')

        fp.write(install_celero() + '\n')

        fp.write(install_antlr() + '\n')

        fp.write(install_aws_sdk() + '\n')

        fp.write(install_pcre2() + '\n')

        fp.write(install_protobuf() + '\n')

        fp.write("pip3 install 'cloudpickle<2.0.0' cython numpy\n")

        fp.write('echo ">>> installing reqs done."\n')

def generate_macos_awssdk_file(path):
    with open(path, 'w') as fp:
        fp.write(install_aws_sdk_macos() + '\n')

def main():
    """generates all scripts"""
    generate_ubuntu1804('ubuntu1804')
    generate_ubuntu2004('ubuntu2004')
    generate_ubuntu2204('ubuntu2204')

    generate_manylinux_files('docker/ci')
    generate_yaml_req_file('install_azure_ci_reqs.sh')

    generate_macos_awssdk_file('macos/install_aws-sdk-cpp.sh')


if __name__ == '__main__':
    main()





