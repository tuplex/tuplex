## Tuplex benchmarks

`sigmod21` contains the old SIGMOD'21 benchmarks.
`nextconf` contains new benchmarks for our next paper.



old README below.



## Benchmarks

measuring IO time:

to see what overhead IO has, simply do

```
du -sb ../flights/tuplex_output
14471659734	../flights/tuplex_output
```
Then run write speed
```
dd if=/dev/zero of=/tmp/out.bin bs=14471659734 count=1
```






# Building Pypy
within source folder
go to `pypy/goal`

use

```$xslt
pypy ../../rpython/bin/rpython --opt=jit --lto --listcompr --backendopt targetpypystandalone.py --objspace-std-methodcachesizeexp=32 \
                                                                                                --objspace-std-optimized_list_getitem \
                                                                                                --withmod-_csv \
                                                                                                --withmod-_pypyjson \
                                                                                                --withmod-_vmprof \
                                                                                                --withmod-_rawffi \
                                                                                                --withmod-_collections
```

then go to `pypy/tool/release` and use
```$xslt
python package.py --archive-name pypy-my-own-package-name
```





# starting docker container
docker run --name sigmod21 --rm -i -t tuplex/sigmod21-experiments:latest bash


Build command for docker image
```bash
cmake -DPYTHON3_VERSION=3.6 -DLLVM_ROOT_DIR=/usr/lib/llvm-9 -DCMAKE_BUILD_TYPE=Release ..
```


To install Tuplex, use
```bash
python3.6 setup.py install
```



For setup of docker on ubuntu 2004 read <https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04>


docker build -t tuplex/sigmod21-experiments:latest .



For running on AWS instance, create raid-0 array, then folders

/disk/data
/disk/benchmark_results
/disk/Tuplex <-- from git clone

get data via
aws s3 cp s3://tuplex/sigmod21-data/ /disk/data/ --recursive

start docker container with folders mounted as volumes via

docker run -v /disk/data:/data -v /disk/benchmark_results:/results -v /home/ubuntu/Tuplex:/code --name sigmod21 --rm -dit tuplex/sigmod21-experiments:latest 

In order to get a shell into the running docker container, use
docker exec -it sigmod21 bash

for aws, disable stupid hyper threading via https://aws.amazon.com/blogs/compute/disabling-intel-hyper-threading-technology-on-amazon-linux/




### Setting up AWS instance
Use a r5d.8xlarge instance with 80GB EBS memory
Pass in startup as described in https://aws.amazon.com/blogs/compute/disabling-intel-hyper-threading-technology-on-amazon-linux/ the script

```bash
#cloud-config
bootcmd:
 - for cpunum in $(cat /sys/devices/system/cpu/cpu*/topology/thread_siblings_list | cut -s -d, -f2- | tr ',' '\n' | sort -un); do echo 0 > /sys/devices/system/cpu/cpu$cpunum/online; done
```
then, when running `lscpu --extended` on the EC2 instance, it should show something like
```bash
ubuntu@ip-172-31-0-76:~$ lscpu --extended
CPU NODE SOCKET CORE L1d:L1i:L2:L3 ONLINE
  0    0      0    0 0:0:0:0          yes
  1    0      0    1 1:1:1:0          yes
  2    0      0    2 2:2:2:0          yes
  3    0      0    3 3:3:3:0          yes
  4    0      0    4 4:4:4:0          yes
  5    0      0    5 5:5:5:0          yes
  6    0      0    6 6:6:6:0          yes
  7    0      0    7 7:7:7:0          yes
  8    0      0    8 8:8:8:0          yes
  9    0      0    9 9:9:9:0          yes
 10    0      0   10 10:10:10:0       yes
 11    0      0   11 11:11:11:0       yes
 12    0      0   12 12:12:12:0       yes
 13    0      0   13 13:13:13:0       yes
 14    0      0   14 14:14:14:0       yes
 15    0      0   15 15:15:15:0       yes
 16    -      -    - :::               no
 17    -      -    - :::               no
 18    -      -    - :::               no
 19    -      -    - :::               no
 20    -      -    - :::               no
 21    -      -    - :::               no
 22    -      -    - :::               no
 23    -      -    - :::               no
 24    -      -    - :::               no
 25    -      -    - :::               no
 26    -      -    - :::               no
 27    -      -    - :::               no
 28    -      -    - :::               no
 29    -      -    - :::               no
 30    -      -    - :::               no
 31    -      -    - :::               no
```
i.e., hyper threading has been deactivated and only real cores are used. Now, let's set up the RAID-0 array using the following commands:
```bash
sudo mkdir -p /disk
sudo mdadm --create --verbose /dev/md0 --level=0 --raid-devices=2 /dev/nvme1n1 /dev/nvme2n1
cat /proc/mdstat # should show the raid array
sudo mkfs.ext4 -F /dev/md0
sudo mount /dev/md0 /disk
sudo chown $(whoami) /disk
```
As a next step, let's follow the guide from https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-20-04 on how to install docker on the machine.

```bash
sudo apt update
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
sudo apt update
apt-cache policy docker-ce # should print out apt details
sudo apt install -y docker-ce
sudo systemctl status docker # should print status out
sudo usermod -aG docker ${USER} # allows to run docker commands as non-sudo
#logout, login to make user group changes effective
```

Next, let's fetch the full SIGMOD-21 experimental data via S3. For this, you need to install the awscli and configure it.
```bash
sudo apt-get install -y python3-pip
pip3 install awscli
# logout, login to put aws on path or source .bashrc
# i.e. above command installs cli into /home/ubuntu/.local/bin/aws 
aws configure # enter here your AWS credentials (zone=us-east-1, output=json). If you have AWS configured on your local machine, simply run `env` to print out the credentials.
```
Now let's get the data via
```bash
aws s3 cp s3://tuplex/sigmod21-data/ /disk/data/ --recursive
```
this might take a while (~170GB of data, at 300MB/s around 10min). 

Next, let's either fetch the docker image or build it from scratch. In any case, check out the Tuplex repo and switch to the sigmod21 branch.
```bash
cd
git clone https://github.com/LeonhardFS/Tuplex.git
cd Tuplex
git checkout --track origin/sigmod21-experiments
```
To build the docker image, go to `/home/ubuntu/Tuplex/benchmark/docker` and execute
```bash
docker build -t tuplex/sigmod21-experiments:latest .
```
This might take a while (i.e. ~1h or so).
Now, let's start up the docker container on the machine via
```bash
docker run -v /disk/data:/data -v /disk/benchmark_results:/results -v /home/ubuntu/Tuplex:/code --name sigmod21 --rm -dit tuplex/sigmod21-experiments:latest 
```

To get a shell within the docker container, use
```bash
docker exec -it sigmod21 bash
```
Then, build Tuplex via
```bigquery
cd /code/
mkdir build
cd build
cmake -DPYTHON3_VERSION=3.6 -DLLVM_ROOT_DIR=/usr/lib/llvm-9 -DCMAKE_BUILD_TYPE=Release ..
make -j16
cd dist/python/
python3.6 setup.py install
```
This completes the setup. Now, we can run the benchmarks. To launch e.g. Zillow/Z1, go to `/code/benchmark/zillow/Z1` and invoke the benchmark script via `nohup bash runbenchmark.sh &`.

### Grizzly Setup Notes (should add to docker build)
```
apt install python-setuptools python2-dev # get setuptools, dev for python2
cd /opt/weld/python
export WELD_HOME=/opt/weld
```

Now, we need to change the install files a little bit:
```
cd pyweld
```

have to change setup.py install_requirements:
  - `pandas==0.23.4` - final pandas to have python2 support
  - `numpy==1.15.4` - final numpy to have python2 support

```
python2 setup.py install; cd ..
```
```
cd grizzly
```

have to change `grizzly/Makefile`:
  - make all the `python`/`python-config` calls to `python2.7`/`python2.7-config`, respectively.
  - make `llvm-config` into `/opt/llvm-6.0/bin/llvm-config`
  - prepend `clang`/`clang++` with `/opt/llvm-6.0/bin/` path

```
python2 setup.py install
```


### Notes:
To test:
does march=native make a difference?
```
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()
```

Use following config for flights, to get it fast...
```json
{"webui.enable": false,
"executorCount": 15,
"executorMemory": "6G",
"driverMemory": "10G",
"partitionSize": "32MB",
"inputSplitSize":"64MB",
"runTimeMemory": "8MB",
"useLLVMOptimizer": true,
"autoUpcast":true,
"optimizer.generateParser":true,
"optimizer.nullValueOptimization": true,
"csv.selectionPushdown": true,
"resolveWithInterpreterOnly":false}
```

--> this + march native yields 5s lower runtime!
