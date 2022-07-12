setup

1. install flintrock `pip3 install flintrock`
2. modify config.yaml with your AWS private key
2. launch spark cluster on EC2 `flintrock --config config.yaml launch sparkcluster`




Ansible:
ansible all -m ping --private-key "~/.ssh/tuplex.pem" --user ubuntu -i inventory.ini

ansible slaves -a 'sudo apt-get update' --private-key "~/.ssh/tuplex.pem" --user ubuntu -i inventory.ini

ansible slaves -a 'sudo apt-get install -y openjdk-8-jdk' --private-key "~/.ssh/tuplex.pem" --user ubuntu -i inventory.ini
use 8082 for master to slave communication

ansible slaves -a '/opt/spark/sbin/start-slave.sh spark://ip-172-31-29-237.ec2.internal:7077 -m 4G -p 8082' --private-key "~/.ssh/tuplex.pem" --user ubuntu -i inventory.ini
install s3 support + configure AWS credentials (for each node!)

create hadoop conf fill in credentials, or check <http://wrschneider.github.io/2019/02/02/spark-credentials-file.html> for other options

```
<configuration  xmlns:xi="http://www.w3.org/2001/XInclude">
    <property>
        <name>fs.s3.awsAccessKeyId</name>
        <value>xxxxx</value>
    </property>

    <property>
        <name>fs.s3.awsSecretAccessKey</name>
        <value>xxxxx</value>
    </property>
</configuration>
```



run via

ansible slaves -a '' --private-key "~/.ssh/tuplex.pem" --user ubuntu -i inventory.ini



copy following conf file on your spark cluster to
/opt/spark/conf/spark-defaults.conf

need to assign 
spark.driver.port
spark.blockManager.port

to fixed numbers (because Spark internally randomizes them.)
If on EC2 important to have them in security group.

make sure to install python3.7 on all nodes via

ansible slaves -a 'sudo apt-get install -y python3.7 python3.7-dev' --private-key "~/.ssh/tuplex.pem" --user ubuntu -i inventory.ini



Then launch on master spark script:

```
export PYSPARK_PYTHON=python3.7
export PYSPARK_DRIVER_PYTHON=python3.7
spark-submit --deploy-mode client --master spark://ip-172-31-29-237.ec2.internal:7077 --executor-memory 4G --executor-cores 2 runpyspark.py --path 's3n://tuplex/data/100GB/*.csv' --output-path 's3n://tuplex/spark/zillow100G'
```

(S3a and s3 APIs not stable with python)