# AWS setup
Following walks through the necessary commands to start up a fresh AWS EC2 instance as used in the Tuplex experiments:

```
# 0. Specify here names/settings
AWS_KEY_NAME='sigmod21.pem'
AWS_SG_NAME='sigmod21-sg'
# use this to authorize only private IP
AUTHORIZED_IPS=`curl https://checkip.amazonaws.com`/32
# use this to authorize all IPs
AUTHORIZED_IPS='0.0.0.0/0'
# Ubuntu 20.04 LTS
AWS_AMI_IMAGE=ami-083654bd07b5da81d

# 1. create new AWS key
aws ec2 create-key-pair \
--key-name "${AWS_KEY_NAME%.*}" \
--query "KeyMaterial" \
--output text > $AWS_KEY_NAME

chmod 400 $AWS_KEY_NAME
ssh-keygen -y -f $AWS_KEY_NAME > $AWS_KEY_NAME.pub

# 2. create security group allowing all sorts of access
aws ec2 create-security-group --group-name ${AWS_SG_NAME} --description "SIGMOD21 experiments security group"

AWS_SG_ID=$(aws ec2 describe-security-groups --group-names ${AWS_SG_NAME} --output text | cut -f3 | tr -d '[:space:]')

aws ec2 authorize-security-group-ingress --group-id ${AWS_SG_ID} --protocol tcp --port 22 --cidr $AUTHORIZED_IPS

# 3. startup EC2 instance (r5d.8xlarge) with 80GB root EBS volume
 aws ec2 run-instances --image-id ${AWS_AMI_IMAGE} --count 1 --instance-type r5d.8xlarge --key-name "${AWS_KEY_NAME%.*}" --security-groups ${AWS_SG_NAME} --user-data launch_script.txt --block-device-mappings "[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":80,\"DeleteOnTermination\":true}}]"

# get instance IP
INSTANCE_IP=$(aws ec2 describe-instances --filters "Name=instance-state-name,Values=running" --filters "Name=instance-type,Values=r5d.8xlarge" --filters "Name=image-id,Values=${AWS_AMI_IMAGE}" --query "Reservations[].Instances[].PublicIpAddress" --output text)

echo "Connect to instance via ssh -i ${AWS_KEY_NAME} ubuntu@${INSTANCE_IP}"

```