#!/usr/bin/env bash/

# find out what JAVA_HOME is
JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 > /dev/null | grep 'java.home' | cut -d '=' -f2 | tr -d '[:blank:]')
echo "Found JAVA_HOME=$JAVA_HOME"

# install recent spark (2.4.7)
cd /usr/src &&
  curl --url https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz --output spark-2.4.7.tgz &&
  curl -L --url https://downloads.lightbend.com/scala/2.12.10/scala-2.12.10.tgz --output scala-2.12.10.tgz &&
  mkdir -p /opt/spark@2 &&
  mkdir -p /opt/scala &&
  tar xf scala-2.12.10.tgz &&
  tar xf spark-2.4.7.tgz &&
  mv spark-2.4.7-bin-hadoop2.7/* /opt/spark@2 &&
  mv scala-2.12.10/* /opt/scala &&
  # update bashrc
  echo "export JAVA_HOME=${JAVA_HOME}" >> "$HOME/.bashrc" &&
  echo "export SCALA_HOME=/opt/scala" >> "$HOME/.bashrc" &&
  echo "export SPARK_HOME=/opt/spark@2" >> "$HOME/.bashrc" &&
  echo "export PATH=\$SPARK_HOME/bin:$JAVA_HOME/bin:\$SCALA_HOME/bin:\$PATH" >> "$HOME/.bashrc" &&
  echo "export PYSPARK_PYTHON=python3" >> "$HOME/.bashrc" &&
  echo "export PYSPARK_DRIVER_PYTHON=python3" >> "$HOME/.bashrc"
