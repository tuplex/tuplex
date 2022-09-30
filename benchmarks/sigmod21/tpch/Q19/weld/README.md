To get this working, on MAC OS X install:

brew install llvm@6 ncurses

On Ubuntu, also install ncurses lib
apt-get install -y cmake libncurses5-dev libncursesw5-dev


To run in docker, use
docker run -ti -v /hot/data:/data -v $PWD:/experiments --name exp weld-experiments

to get a nother login shell into this container, use

docker exec -it exp bash


Build via 

```
WELD_HOME=/opt/weld/ cmake -DCMAKE_BUILD_TYPE=Release ..
```
