#!/usr/bin/env python3
# (c) L.Spiegelberg 2020
# use this file to start a tuplex compatible aws instance with all dependencies installed

import argparse
import os
import sys
import boto
import boto.ec2
import subprocess
import time
import json

def get_connection(aws_access_key, aws_secret_key, region=u'us-east-2'):
    return boto.ec2.connect_to_region(region,
                                      aws_access_key_id=aws_access_key,
                                      aws_secret_access_key=aws_secret_key)

def start_instance(conn, image_id='ami-0226c1fcd5f7f2898', instance_type='r5d.4xlarge'):
    cur_instances = []

    cache_file_path = 'tuplex_instances.json'

    if os.path.isfile(cache_file_path):
        for line in open(cache_file_path).readlines():
            inst = json.loads(line)
            if inst not in cur_instances:
                cur_instances.append(inst)

    instances_file = open(cache_file_path, 'w')

    instances = []
    spot_requests = conn.request_spot_instances(price='2.00', image_id=image_id, count=1, key_name='tuplex',
                                                security_groups=['allips'],
                                                instance_type=instance_type, user_data='')
    assert len(spot_requests) > 0
    spot_request = spot_requests[0]

    # wait until requests get fulfilled...
    open_request = True
    while open_request:
        time.sleep(5)
        open_request = False
        spot_requests = conn.get_all_spot_instance_requests(request_ids=[spot_request.id])
        for spot_request in spot_requests:
            print('Spot request status: {}'.format(spot_request.state))
            if spot_request.state == 'open':
                open_request = True

    # Get info about instances
    instance_num = 0
    reservations = conn.get_all_reservations()
    for reservation in reservations:
        for instance in reservation.instances:

            # skip updates for spot requests not beloning to the currently active ones
            if instance.spot_instance_request_id not in [spot_request.id]:
                continue

            instance.update()
            while instance.state == u'pending':
                time.sleep(1)
                instance.update()
            if instance.state == u'running':
                if instance.id not in cur_instances:
                    instance_num += 1
                    print('Started instance {}: {}'.format(instance_num, instance))
                    desc = {'public_dns_name': instance.public_dns_name, 'id': instance.id}
                    instances.append(desc)
                    instances_file.write(json.dumps(desc) + '\n')
            else:
                print('Could not start instance: {}'.format(instance))
    instances_file.close()
    return instances

def terminate_instances(conn, instances):
    print('Terminating instances: {}'.format([inst['id'] for inst in instances]))
    conn.terminate_instances(instance_ids=[inst['id'] for inst in instances])


def scp_to_instances(instances, path, dest_path=None, username='ubuntu', key='~/.ssh/tuplex.pem'):
    print('scp {} to {}'.format(path, ','.join([inst['public_dns_name'] for inst in instances])))
    if dest_path is None:
        dest_path = '/home/{}/'.format(username)
    for inst in instances:
        address = inst['public_dns_name']
        cmd = 'scp -i {}'.format(key) if key else 'scp'
        cmd += ' -o StrictHostKeyChecking=no {} {}@{}:{}'.format(path, username, address, dest_path)
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        out = out.decode()
        err = err.decode()

        if err:
            print('ERROR {}: {}'.format(address, err))
        else:
            print('SUCCESS')


def run_cmd_on_instances(instances, cmd, username='ubuntu', key='~/.ssh/tuplex.pem', quiet=False):
    if not quiet:
        print('{} on {}'.format(cmd, ','.join([inst['public_dns_name'] for inst in instances])))

    outarr = []
    errarr = []
    retcodes = []
    processes= []
    for inst in instances:
        address = inst['public_dns_name']
        sshcmd = 'ssh -i {}'.format(key) if key else 'ssh'
        sshcmd += ' -o StrictHostKeyChecking=no {}@{} \'{}\''.format(username, address, cmd)
        p = subprocess.Popen([sshcmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append(p)
        out, err = p.communicate()
        out = out.decode()
        err = err.decode()

        outarr.append(out)
        errarr.append(err)
        retcodes.append(p.returncode)

    # wait for processes to finish
    for p in processes:
        p.wait()

    if len(instances) == 1:
        return outarr[0], errarr[0], retcodes[0]
    else:
        return outarr, errarr, retcodes


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-r', '--region', dest='aws_region', default='us-east-2', help='region in which to launch EC2 instance.')
    parser.add_argument('-k', '--ssh-key', dest='sshkey', default='~/.ssh/tuplex.pem', help='SSH key to use with AWS, must be registered.')
    parser.add_argument('--instance-type', dest='aws_instance_type', default='r5d.4xlarge', help='instance type to launch')
    parser.add_argument('--image', dest='aws_image', default='ami-0226c1fcd5f7f2898', help='image type')
    parser.add_argument('--store-credentials', dest='store_credentials', default=False, action='store_true', help='whether to copy AWS credentials to EC2 instance or not')
    parser.add_argument('--download-data', dest='download_data', default=False, action='store_true', help='whether to download data from public s3://tuplex-public bucket. Need to copy aws credentials there.')
    args = parser.parse_args()

    # AWS args
    assert 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ, 'AWS credentials not set'

    aws_access_key = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']

    region = args.aws_region or u'us-east-2'

    print('>>> creating EC2 connection')
    conn = get_connection(aws_access_key, aws_secret_key, region=args.aws_region)

    # make new spot request
    print('>>> starting new instance')
    instances = start_instance(conn, image_id=args.aws_image, instance_type=args.aws_instance_type)
    for inst in instances:
        print('- instance {} available under {}'.format(inst['id'], inst['public_dns_name']))
        print('  login via: ssh -i {} ubuntu@{}'.format(args.sshkey, inst['public_dns_name']))

    # waiting for 5s till instance is up
    for i in range(5):
        print('sleep for 1s till instance is up...')
        time.sleep(1)

    # setup stuff
    # configuring the instance (SSDs RAID-0)
    print('>>> configuring RAID-0 array on instance')

    # create RAID-0 array
    # TODO: better via init script...
    run_cmd_on_instances(instances,
                         'sudo mdadm --create --verbose /dev/md0 --level=0 --raid-devices=2 /dev/nvme1n1 /dev/nvme0n1')

    # poll mdstat a couple times
    out = ''
    for i in range(5):
        out, _, _ = run_cmd_on_instances(instances, 'cat /proc/mdstat', key=args.sshkey)
        if 'md0 : active raid0' not in out:
            print('disk not yet initialized, sleep...')
            time.sleep(1)
        else:
            break
    assert 'md0 : active raid0' in out

    run_cmd_on_instances(instances, 'sudo mkdir -p /disk', key=args.sshkey)

    # create file system and mount disks
    run_cmd_on_instances(instances, 'sudo mkdir -p /disk', key=args.sshkey)
    run_cmd_on_instances(instances, 'sudo mkfs.ext4 -F /dev/md0', key=args.sshkey)

    # mount /dev/md0 under /disk
    run_cmd_on_instances(instances, 'sudo mount /dev/md0 /disk', key=args.sshkey)

    # you can check available disk space via `df -h -x devtmpfs -x tmpfs`

    # make /disk owned by current user (ubuntu/ec2-user)
    run_cmd_on_instances(instances, 'sudo chown $(whoami) /disk', key=args.sshkey)

    print(' -- done -- ')

    # copy over ssh keys
    # make sure the key repokey is added to github as deployment key (only pull access!)
    print('>>> setting up SSH keys for repo access on instance')
    run_cmd_on_instances(instances, 'mkdir -p ~/.ssh', key=args.sshkey)
    scp_to_instances(instances, '../keys/repokey.pem', '~/.ssh/repokey.pem', key=args.sshkey)
    run_cmd_on_instances(instances, 'chmod 400 ~/.ssh/repokey.pem', key=args.sshkey) # change permissions, important!


    # add config for github.com, disable host checking.
    github_fingerprint = '16:27:ac:a5:76:28:2d:36:63:1b:56:4d:eb:df:a6:48'

    ssh_config_content = '''host github.com
     HostName github.com
     IdentityFile ~/.ssh/repokey.pem
     StrictHostKeyChecking no
     User git
    '''

    with open('sshconfig.txt', 'w') as fp:
        fp.write(ssh_config_content)

    scp_to_instances(instances, 'sshconfig.txt', '~/.ssh/config', key=args.sshkey)
    os.remove('sshconfig.txt')

    if args.store_credentials:

        # setup AWS CLI
        print('>>> setting up AWS S3 access & Co. on instance')
        # set up aws cli on instance (needed because requester pays S3!)
        run_cmd_on_instances(instances, 'sudo snap install --classic aws-cli', key=args.sshkey)
        run_cmd_on_instances(instances, 'echo "export PATH=/snap/bin:\\$PATH" >> ~/.bashrc', key=args.sshkey)

        # add env vars to .bashrc
        # aws_access_key = os.environ['AWS_ACCESS_KEY_ID']
        # aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
        run_cmd_on_instances(instances, 'echo "export AWS_ACCESS_KEY_ID={}" >> ~/.bashrc'.format(aws_access_key),
                             quiet=True, key=args.sshkey)
        run_cmd_on_instances(instances, 'echo "export AWS_SECRET_ACCESS_KEY={}" >> ~/.bashrc'.format(aws_secret_key),
                             quiet=True, key=args.sshkey)

        # copy over credentials fils
        run_cmd_on_instances(instances, 'mkdir -p ~/.aws', key=args.sshkey)
        scp_to_instances(instances, '~/.aws/credentials', '~/.aws/credentials', key=args.sshkey)

        if args.download_data:
            # downloading datafiles
            print('>>> downloading data files')
            flight_data_uri = 's3://tuplex-public/data/flights.tar.gz'

            run_cmd_on_instances(instances,
                                 'mkdir -p /disk/data/flights && cd /disk/data/flights && /snap/bin/aws s3 cp {} .'.format(
                                     flight_data_uri), key=args.sshkey)
            run_cmd_on_instances(instances,
                                 'cd /disk/data/flights && tar xf flights.tar.gz && rm flights.tar.gz', key=args.sshkey)


    #  -- setup done --

    # # terminate all instances
    #terminate_instances(conn, instances)

if __name__ == '__main__':
    main()
