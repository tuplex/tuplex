# Readme for reproducibility submission of paper ID [259]
* Paper name: [Tuplex: Data Science in Python at Native Code Speed](https://dl.acm.org/doi/10.1145/3448016.3457244)
* Conference: SIGMOD'21

### A) Source code info
* Repository: [official Github Repo](https://github.com/tuplex/tuplex)
* Programming Language: C/C++/Java/Python
* Additional Programming Language info: C++14, C11, Java1.8, Python 3.6.9
* Compiler Info: gcc 10
* Packages/Libraries Needed: We provide scripts to install requirements for various platforms (MacOS/Ubuntu 18.04/Ubuntu 20.04) as well as various Docker images to run Tuplex.


### B) Datasets info
* Repository: [url]
* Data generators: [url]

### C) Hardware Info
We use a single [r5d.8xlarge EC2](https://aws.amazon.com/ec2/instance-types/r5/) instance to carry out experiments.
	
	- C1) Processor (architecture, type, and number of processors/sockets)
	- C2) Caches (number of levels, and size of each level)
	- C3) Memory (size and speed)
	- C4) Secondary Storage (type: SSD/HDD/other, size, performance: random read/sequnetial read/random write/sequnetial write)
	- C5) Network (if applicable: type and bandwidth)

### D) Experimentation Info
	- D1) Scripts and how-tos to generate all necessary data or locate datasets
	[Ideally, there is a script called: ./prepareData.sh]
	- D2) Scripts and how-tos to prepare the software for system
	[Ideally, there is a script called: ./prepareSoftware.sh]
	- D3) Scripts and how-tos for all experiments executed for the paper
	[Ideally, there is a script called: ./runExperiments.sh]

--
(c) 2021 Tuplex authors
