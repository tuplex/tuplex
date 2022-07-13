Run following commands to configure r5d.8xlarge instance


```
sudo apt-get update

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
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io
sudo systemctl status docker # should print status out
sudo usermod -aG docker ubuntu # allows to run docker commands as non-sudo
#logout, login to make user group changes effective

# 3. retrieve the repo & check out resukts
cd /disk
git clone https://github.com/LeonhardFS/tuplex-public.git
cd tuplex-public
git checkout --track origin/inc-exp

# 4. build the benchmark docker image
cd scripts/docker/benchmark && ./create-image.sh

```

Then, install clearcache on the machine

```
// To build:
// g++ main.cc -o clearcache
// Then, to install (& set sid flag):
// sudo cp clearcache /usr/local/bin/
// sudo chown root:root /usr/local/bin/clearcache && sudo chmod 4655 /usr/local/bin/clearcache

#include <cstdio>
#include <iostream>
#include <cstdlib>
#include <string>
#include <sys/types.h>
#include <unistd.h>

int main() {
  using namespace std;
  setuid(0);
  cout<<"Currently used caches:"<<endl;
  system("free -h");
  cout<<"Clearing all OS caches..."<<endl;
  string clear_cmd = "sync && sh -c 'echo 3 > /proc/sys/vm/drop_caches' && swapoff -a && swapon -a && printf '\n%s\n' 'Ram-cache and Swap Cleared'";
  system(clear_cmd.c_str());
  cout<<"Caches after clearing:"<<endl;
  system("free -h");
  return 0;

}
```

sudo apt install g++ -y 