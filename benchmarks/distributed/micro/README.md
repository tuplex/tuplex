## Run how fast an invocation would take on Lambda vs. M5
in `experiments/singleinvocation` is code to run a protobuf message (within this folder)

i.e. run then ./singlelambda -p zillow.pb -i s3://tuplex/data/10GB/zillow_00001.csv -o s3://tuplex/scratch/bench.csv

to benchmark how fast an EC2 performs compared to tuplex.
