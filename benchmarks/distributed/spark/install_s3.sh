curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar &&
curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar &&
curl -O https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.0/jets3t-0.9.0.jar &&
mv hadoop-aws-2.7.3.jar /opt/spark/jars/ &&
mv aws-java-sdk-1.7.4.jar /opt/spark/jars/ &&
mv jets3t-0.9.0.jar /opt/spark/jars/