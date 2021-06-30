#!/usr/bin/env bash
# (c) L.Spiegelberg 2020
# install optional packages like perf, spark, dask for benchmarking and experiments

# need to run this with root privileges
if [[ $(id -u) -ne 0 ]]; then
  echo "Please run as root"
  exit 1
fi

# Tuplex done, here optional packages for profiling & Co
# Profiling, Spark, Dask
apt-get install -y valgrind linux-tools-common linux-tools-generic linux-tools-$(uname -r)

# install recent spark (2.4.5)
mkdir -p /tmp/downloads &&
  curl --url http://apache.cs.utah.edu/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz --output /tmp/downloads/spark-2.4.6.tgz &&
  curl -L --url https://downloads.lightbend.com/scala/2.12.10/scala-2.12.10.tgz --output /tmp/downloads/scala-2.12.10.tgz &&
  mkdir -p /opt/spark &&
  mkdir -p /opt/scala &&
  pushd /tmp/downloads &&
  tar xf scala-2.12.10.tgz &&
  tar xf spark-2.4.6.tgz &&
  mv spark-2.4.6-bin-hadoop2.7/* /opt/spark &&
  mv scala-2.12.10/* /opt/scala &&
  popd &&
  # update bashrc
  echo "export SCALA_HOME=/opt/scala" >> "$HOME/.bashrc" &&
  echo "export SPARK_HOME=/opt/spark" >> "$HOME/.bashrc" &&
  echo "export PATH=\$SPARK_HOME/bin:$JAVA_HOME/bin:\$SCALA_HOME/bin:\$PATH" >> "$HOME/.bashrc" &&
  echo "export PYSPARK_PYTHON=python3" >> "$HOME/.bashrc" &&
  echo "export PYSPARK_DRIVER_PYTHON=python3" >> "$HOME/.bashrc"

# install Dask
sudo python3 -m pip install "dask[complete]"
