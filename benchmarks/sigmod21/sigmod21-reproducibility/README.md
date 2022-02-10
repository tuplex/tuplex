# Readme for reproducibility submission of paper ID [259]
* Paper name: [Tuplex: Data Science in Python at Native Code Speed](https://dl.acm.org/doi/10.1145/3448016.3457244)
* Conference: SIGMOD'21


**If you merely want to run the necessary commands, please skip A)-C) and head directly to section D).**

If any questions arise or things don't work, please do not hesitate to reach out to the authors or e-mail `tuplex@cs.brown.edu`.

### A) Source code info
* Repository: [official Github Repo](https://github.com/tuplex/tuplex)
* Programming Language: C/C++/Java/Python
* Additional Programming Language info: C++14, C11, Java1.8, Python 3.6.9
* Compiler Info: gcc 10
* Target architecture: x86_64
* Packages/Libraries Needed: We provide scripts to install requirements for various platforms (MacOS/Ubuntu 18.04/Ubuntu 20.04) as well as various Docker images to run Tuplex.

#### Retrieving the repo
Run the following commands to retrieve the Tuplex repo from within `/disk` in order to reproduce everything:

```
cd /disk
git clone https://github.com/LeonhardFS/tuplex-public.git
cd tuplex-public
git checkout --track origin/sigmod-repro
```

In the folder  `benchmarks/sigmod21-reproducibility` we provide a python script `tuplex.py` which acts as command line interface (CLI) to carry out both experiments and generate plots. Given running the experiments might take quite some time, we provide our original experimental results as zipped file (`r5d.8xlarge.tar.gz`). With this data you may regenerate the paper's original plots.
To run the script `python3` and several packages are required which can be installed via `pip3 install -r requirements.txt`. This script has various options for the different tasks, which can be listed via:

```
./tuplex.py --help
```

### B) Datasets info
For convenience we provide a CLI command to automatically download the data on a benchmark machine to `/disk` via 
```
./tuplex.py download
```

We host the data in both Google Drive and on S3. We ask the validator to NOT SHARE any of the data, as it contains privacy sensitive data (Zillow and logs from Brown University). For this reason, we password protected the 7zip file. The password will be made available via Microsoft CMT or can be retrieved by sending us an email to `tuplex@cs.brown.edu` or to one of the authors. The full data requires around `~180GB` of free disk space unpacked, the data file itself is compressed using 7zip resulting in `~12GB` to download.

| Host option: | Link:  | Description on how to retrieve:  |
|------------|---|---|
| Google Drive Link | https://drive.google.com/uc?id=1chJncLpuSOPUvlWwODg_a7A-sEbEORL1 |Most convenient way to retrieve is to use gdown (install via `pip3 install gdown`). |
| AWS S3 Link | s3://tuplex-public/data/sigmod21.7z | Most convenient way is to use the AWS CLI (install via `pip3 install awscli` and configure via `aws configure`). In order to download the file use e.g. `aws s3 cp s3://tuplex-public/data/sigmod21.7z . --request-payer requester` |

To unpack the file, go to `/disk` on your benchmark machine and run

```
7z x sigmod21.7z
```

This will store all data files in `/disk/data`.

### C) Hardware Info
We use a single [r5d.8xlarge EC2](https://aws.amazon.com/ec2/instance-types/r5/) instance in the us-east-1 zone on which we disable HyperThreading to carry out experiments.

The following is the output of `lscpu` on a r5d.8xlarge instance:

```
Architecture:                    x86_64
CPU op-mode(s):                  32-bit, 64-bit
Byte Order:                      Little Endian
Address sizes:                   46 bits physical, 48 bits virtual
CPU(s):                          32
On-line CPU(s) list:             0-31
Thread(s) per core:              2
Core(s) per socket:              16
Socket(s):                       1
NUMA node(s):                    1
Vendor ID:                       GenuineIntel
CPU family:                      6
Model:                           85
Model name:                      Intel(R) Xeon(R) Platinum 8259CL CPU @ 2.50GHz
Stepping:                        7
CPU MHz:                         2499.998
BogoMIPS:                        4999.99
Hypervisor vendor:               KVM
Virtualization type:             full
L1d cache:                       512 KiB
L1i cache:                       512 KiB
L2 cache:                        16 MiB
L3 cache:                        35.8 MiB
NUMA node0 CPU(s):               0-31
Vulnerability Itlb multihit:     KVM: Mitigation: VMX unsupported
Vulnerability L1tf:              Mitigation; PTE Inversion
Vulnerability Mds:               Vulnerable: Clear CPU buffers attempted, no microcode; SMT Host state unknown
Vulnerability Meltdown:          Mitigation; PTI
Vulnerability Spec store bypass: Vulnerable
Vulnerability Spectre v1:        Mitigation; usercopy/swapgs barriers and __user pointer sanitization
Vulnerability Spectre v2:        Mitigation; Full generic retpoline, STIBP disabled, RSB filling
Vulnerability Srbds:             Not affected
Vulnerability Tsx async abort:   Not affected
Flags:                           fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss ht syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology nonstop_tsc cpuid aperfmperf tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch invpcid_single pti fsgsbase tsc_adjust bmi1 avx2 smep bmi2 erms invpcid mpx avx512f avx512dq rdseed adx smap clflushopt clwb avx512cd avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves ida arat pku ospke
```

We provide in `AWS_Setup.md` commands to start up the type of EC2 instance we used for our experiments.
In `AWS_Configuration.md` we provide the commands we used to configure the machine. They have been also saved in the `config_r5d.sh` script, which can be run simply via `sudo bash config_r5d.sh`.

### D) Experimentation Info

For convenience we provide a top-level command-line interface `/tuplex.py` to carry out various tasks in order to reproduce the results. In order to carry out experiments, a couple steps to be performed after setting up a benchmark machine (as described in C) or `AWS_Setup.md` / `AWS_Configuration.md`), for which the CLI may be used:

1. download & extract data  
    `./tuplex.py download --password <PASSWORD HERE>`
2. Retrieve `tuplex/benchmark` image
    `docker pull tuplex/benchmark` or create the image from scratch by running `./create-image.sh` from within `/disk/tuplex-public/scripts/docker/benchmark` (may take ~1h to build on r5d.8xlarge)
3. build tuplex in the container
    `./tuplex.py build`
4. run experiments
    `./tuplex.py run <dataset>`
5. generate plots by specifying a result folder
    `./tuplex.py plot all --input-path /disk/benchmark_results`

The commands above are were tested on a r5d.8xlarge machine. However, if you'd like to use a different machie, please note that each command can be customized and we also do provide in the following paragraphs additional explanation what each step involves.


### E) Detailed experimentation steps if CLI fails:
Given our experiments benchmark against various setups, we packaged everything into one docker container. The Dockerfile corresponding to the benchmark container can be found in the repo in the folder `scripts/docker/benchmark`. A script `./create-image.sh` in the same folder is provided to create the docker image from scratch.


To startup the container, we use the following settings:

```
docker run -v /disk/data:/data \
           -v /disk/benchmark_results:/results \
           -v /disk/tuplex-public:/code \
           --name sigmod21 --rm -dit tuplex/benchmark
```

To get a console into the container, you may use

```
docker exec -it sigmod21 bash
```

Tuplex can be build from the `/code` directory using the following commands:

```
#!/usr/bin/env bash
# builds Tuplex within experimental container

export CC=gcc-10
export CXX=g++-10

TUPLEX_DIR=/code

cd $TUPLEX_DIR && cd tuplex && mkdir -p build && \
cd build && \
cmake -DBUILD_WITH_AWS=OFF -DBUILD_NATIVE=ON -DPYTHON3_VERSION=3.6 \
      -DLLVM_ROOT_DIR=/usr/lib/llvm-9 -DCMAKE_BUILD_TYPE=Release .. && \
make -j16 && \
cd dist/python/ && \
python3.6 setup.py install
```

#### Running experiments

All benchmarks are provided as source code in subfolders within the `./benchmarks` folder. In the CLI we provide a subcommand `run` to run key experiments over each dataset.

Within each subfolder, the scripts are usually structured as `run<framework>.py` and can be invoked separately in case. A `run<framework>.py` script thereby represents a single run for a single framework. `runbenchmark.sh` scripts run the runner scripts for each framework several times. Indeed, the CLI invokes this scripts from the docker container.

In our original setup, we ran each configuration 11 times (1 warmup run that's ignored and 10 runs).
Yet, users may want to select a different number of runs to save time.
This can be achieved by simply exporting an environment variable `NUM_RUNS`, i.e. to use 5 runs (1 warmup) use
```bash
export NUM_RUNS=5
```
or start the docker container with that setting by passing as parameter `-e NUM_RUNS=5`.

For convenience, the CLI also features a `start/stop` subcommand pair which allows to start/stop the docker container.

#### Lambda experiment
The experiment comparing Tuplex's Lambda backend vs. Spark is unfortunately not any longer reproducible due to AWS having changed their infrastructure recently. Yet, upon request we're happy to provide detailed instructions to produce table 4 as well.

The setup guide and source code for this experiment can be found in the `benchmarks/distributed` folder.

--
(c) 2021 Tuplex authors
