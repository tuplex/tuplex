## How to launch benchmark

Step 1: create docker image via ./create-image.sh

Step 2:

Run following to output everything to local result dir

```
mkdir -p ./results
docker run -v $PWD/results:/results -e NUM_RUNS=5 pydocker bash /code/benchmark.sh
```

