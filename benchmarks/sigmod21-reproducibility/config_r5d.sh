#!/usr/bin/env bash

# check that script is run as sudo
if (( $EUID != 0 )); then
    echo "Please run as root"
    exit 1
fi

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
#logout, login to make user group changes effective, or use hack at end of script

# 3. install awscli
sudo apt-get install -y python3-pip
pip3 install awscli

# or run following: https://superuser.com/questions/272061/reload-a-linux-users-group-assignments-without-logging-out
exec su -l $USER
