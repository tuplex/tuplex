This file contains instructions on how to reproduce the experiment:


1.startup m5.large EC2 instance



2. Install the following



3. install data into /data (requires ~100GB of free disk space!)
```
sudo apt-get update && sudo apt-get -y install python3-pip git clang-9
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-9 10
sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-9 10

python3 -m pip install awscli boto3
export PATH=/home/ubuntu/.local/bin:$PATH
aws configure
sudo mkdir -p /data && sudo chown -R ubuntu:ubuntu /data
aws s3 cp s3://tuplex-public/data/flights_all.tar.gz .
tar -xvf flights_all.tar.gz -C /data
cd /data && ls *.gz | xargs gunzip
```

cd && git clone https://github.com/LeonhardFS/tuplex-public.git && cd tuplex-public/ && git checkout --track origin/lambda-exp
cd benchmarks/nextconf/hyperspecialization/flights/





To get the assembly of the functions, use:

objdump -d agg_weather_specialized.so | awk -v RS= '/^[[:xdigit:]]+ <process_cells>/' > specialized.asm

objdump -d agg_weather_general.so | awk -v RS= '/^[[:xdigit:]]+ <process_cells>/' > general.asm