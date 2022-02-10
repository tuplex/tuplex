## AWS r5d.8xlarge configuration

In the following document the setup of the r5d.8xlarge instance as we used it for the SIGMOD21 paper is described.

All experiments were run using a docker container. Data is placed on a RAID-0 array which is mounted to `disk` with the following folders:

* `/disk/data` holds all experimental data
* `/disk/benchmark_results` stores benchmark results
* `/disk/tuplex` the tuplex repository.

### Setting up the (host) r5d.8xlarge machine
If you feel comfortable, simply run `sudo bash config_r5d.sh`. Else, here are the detailed steps:
```
# 1. set up RAID-0 array
sudo mkdir -p /disk
sudo mdadm --create --verbose /dev/md0 --level=0 --raid-devices=2 /dev/nvme1n1 /dev/nvme2n1
cat /proc/mdstat # should show the raid array
sudo mkfs.ext4 -F /dev/md0
sudo mount /dev/md0 /disk
sudo chown $(whoami) /disk

# 2. install docker
sudo apt update
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common p7zip-full
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
sudo apt update
apt-cache policy docker-ce # should print out apt details
sudo apt install -y docker-ce
sudo systemctl status docker # should print status out
sudo usermod -aG docker ${USER} # allows to run docker commands as non-sudo
#logout, login to make user group changes effective

# 3. install awscli
sudo apt-get install -y python3-pip
pip3 install awscli
# logout, login to put aws on path or source .bashrc
# i.e. above command installs cli into /home/ubuntu/.local/bin/aws

aws configure # enter here your AWS credentials (zone=us-east-1, output=json). If you have AWS configured on your local machine, simply run `env` to print out the credentials.

```
