# Readme for reproducibility submission of paper ID [259]
* Paper name: [Tuplex: Data Science in Python at Native Code Speed](https://dl.acm.org/doi/10.1145/3448016.3457244)
* Conference: SIGMOD'21

### A) Source code info
* Repository: [official Github Repo](https://github.com/tuplex/tuplex)
* Programming Language: C/C++/Java/Python
* Additional Programming Language info: C++14, C11, Java1.8, Python 3.6.9
* Compiler Info: gcc 10
* Target architecture: x86_64
* Packages/Libraries Needed: We provide scripts to install requirements for various platforms (MacOS/Ubuntu 18.04/Ubuntu 20.04) as well as various Docker images to run Tuplex.


### B) Datasets info
* Repository: [url]
* Data generators: [url]

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

#### Retrieving the repo
Run the following commands to retrieve the Tuplex repo from within `/disk`:

```
cd /disk
git clone https://github.com/LeonhardFS/tuplex-public.git
cd tuplex-public
git checkout --track origin/sigmod-repro
```

Then, startup the container via

```
docker run -v /disk/data:/data -v /disk/benchmark_results:/results -v /disk/tuplex-public:/code --name sigmod21 --rm -dit tuplex/benchmark
```

Build and install Tuplex then via
```
```


#### Retrieving data files
We host the data in both Google Drive and on S3. We ask the validator to NOT SHARE any of the data, as it contains privacy sensitive data (Zillow and logs from Brown University). For this reason, we password protected the 7zip file. The password will be made available via Microsoft CMT or can be retrieved by sending an email to `tuplex@cs.brown.edu` or to one of the authors. The full data requires around `~180GB` of free disk space unpacked, the data file itself is compressed using 7zip resulting in `~12GB` to download.

| Host option: | Link:  | Description on how to retrieve:  |
|------------|---|---|
| Google Drive Link | https://drive.google.com/file/d/1chJncLpuSOPUvlWwODg_a7A-sEbEORL1 |Most convenient way to retrieve is to use gdown (install via `pip3 install gdown`). |
| AWS S3 Link | s3://tuplex-public/data/sigmod21.7z | Most convenient way is to use the AWS CLI (install via `pip3 install awscli` and configure via `aws configure`). In order to download the file use e.g. `aws s3 cp s3://tuplex-public/data/sigmod21.7z . --request-payer requester` |

To unpack the file, go to `/disk` on your benchmark machine and run

```
7z x sigmod21.7z
```

This will store all data files in `/disk/data`.


#### Docker container w. everything installed
Due to the amount of different frameworks being evaluated in the original paper, we ran all experiments using a single docker container containing all frameworks. The container can be either recreated, dowloaded from docker.hub or using a Google Drive Link or AWS S3 Link.

| Host option: | Link: | Description on how to retrieve: |
|--------------|-------|----|
| DockerHub | https://hub.docker.com/r/tuplex/sigmod21-experiments | docker pull tuplex/sigmod21-experiments |
| create from source | `./scripts/docker/benchmark` | Run the `./create-image.sh` script from within the `./scripts/docker/benchmark` folder, requires local docker installation |
| AWS S3 | |


#### Generating plots
In the folder we provide a python script `tuplex.py` which acts as command line interface (CLI) to carry out both experiments and generate plots. Given running the experiments might take quite some time, we provide our original experimental results as zipped file (r5d.8xlarge.tar.gz). Users can then regenerate the papers original plots
using

```
./tuplex.py plot all # plots all figures and saves them in plots/ folder

./tuplex.py plot figure3 # generate only figure3

./tuplex.py plot --help # list available plots.
```

#### Running experiments in docker
In our original setup, we ran each configuration 11 times (1 warmup run that's ignored and 10 runs).
Yet, users may want to select a different number of runs to save time.
This can be achieved by simply exporting an environment variable `NUM_RUNS`, i.e. to use 5 runs (1 warmup) use
```bash
export NUM_RUNS=5
```
or start the docker container with that setting by passing as parameter `-e NUM_RUNS=5`.


#### Lambda experiment
The experiment comparing Tuplex's Lambda backend vs. Spark is unfortunately not any longer reproducible due to AWS having changed their infrastructure recently. Yet, upon request we're happy to provide detailed instructions to produce table 4 as well.

commands for running experiments:


echo "docker exec -e NUM_RUNS=1 sigmod21 bash -c 'cd /code/benchmarks/zillow/Z1/ && bash runbenchmark.sh'"
docker exec -e NUM_RUNS=1 sigmod21 bash -c 'cd /code/benchmarks/zillow/Z1/ && bash runbenchmark.sh'
echo "docker exec -e NUM_RUNS=1 sigmod21 bash -c 'cd /code/benchmarks/zillow/Z2/ && bash runbenchmark.sh'"
docker exec -e NUM_RUNS=1 sigmod21 bash -c 'cd /code/benchmarks/zillow/Z2/ && bash runbenchmark.sh'


docker exec -e NUM_RUNS=1 sigmod21 bash -c 'cd /code/benchmarks/dirty_zillow && bash runbenchmark.sh

the others seem to work like this as well...
I.e. finish up documentation & then simply add to root driver file!


<< TODO: mention verification scripts >>

	- D1) Scripts and how-tos to generate all necessary data or locate datasets
	[Ideally, there is a script called: ./prepareData.sh]
	- D2) Scripts and how-tos to prepare the software for system
	[Ideally, there is a script called: ./prepareSoftware.sh]
	- D3) Scripts and how-tos for all experiments executed for the paper
	[Ideally, there is a script called: ./runExperiments.sh]

--
(c) 2021 Tuplex authors
