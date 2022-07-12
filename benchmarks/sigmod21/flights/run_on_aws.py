#!/usr/bin/env python3
# (c) L.Spiegelberg 2020
# use this file to run experiments on a AWS EC2 instance.

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


def create_tuplex_benchmark_script(branch, commit, num_runs=5, num_months=1, timeout=900,
                                   email='leonhard_spiegelberg@brown.edu'):
    s = '#!/usr/bin/env bash\n'
    s += '# auto generated benchmark script to run tuplex for {}, commit {}\n'.format(branch, commit)
    s += 'NUM_RUNS={}\n'.format(num_runs)
    s += 'TIMEOUT={}\n'.format(timeout)
    s += 'RESULT_DIR=results\n'
    s += '[ -d ${RESULT_DIR} ] && rm -rf ${RESULT_DIR}\n'
    s += 'mkdir -p ${RESULT_DIR}\n'
    s += 'date > ${RESULT_DIR}/memusage_tuplex.txt\n'  # initial memusage
    s += 'free -s 1 -b >> ${RESULT_DIR}/memusage_tuplex.txt 2>&1 &\n'
    s += 'FREEPID=$!\n'
    s += 'sleep 10\n'  # sleep for 10s to collect baseline memusage

    # codegen runs
    s += 'for i in $(seq 1 {}); do\n'.format(num_months)
    s += '\tv=$(printf %02d $i)\n'
    s += '\tfor ((r = 1; r <= NUM_RUNS; r++)); do\n'
    s += '\t\tLOG="${RESULT_DIR}/tuplex-01-$v-run-$r.txt"\n'
    s += '\t\techo $LOG\n'
    s += '\t\ttimeout $TIMEOUT python3 runtuplex.py --path /disk/data/flights/ --num-months $v >$LOG 2>$LOG.stderr\n'
    s += '\tdone\n'
    s += 'done\n'
    s += '\n'
    s += 'date >> memusage_tuplex.txt && kill $FREEPID\n'
    s += '\n'
    s += 'cp tuplex_config.json ${RESULT_DIR}/\n'  # include config file for tuplex!
    s += 'tar cvzf results.tar.gz ${RESULT_DIR}/'
    s += '\n'
    s += 'echo "Experiment on branch {}/{} done" >> email.txt\n'.format(branch, commit)
    s += 'date >> email.txt\n'
    s += 'python3 ../../scripts/sendmail.py -s "EC2 results ready" -b email.txt -e {} results.tar.gz\n'.format(email)
    return s


def create_spark_benchmark_script(branch, commit, num_runs=5,
                                  num_months=1, timeout=900, email='leonhard_spiegelberg@brown.edu'):
    s = '#!/usr/bin/env bash\n'
    s += '# auto generated benchmark script to run pyspark for {}, commit {}\n'.format(branch, commit)
    s += 'NUM_RUNS={}\n'.format(num_runs)
    s += 'TIMEOUT={}\n'.format(timeout)
    s += 'RESULT_DIR=results\n'
    s += 'source ~/.bashrc\n'
    s += '[ -d ${RESULT_DIR} ] && rm -rf ${RESULT_DIR}\n'
    s += 'mkdir -p ${RESULT_DIR}\n'
    s += 'date > ${RESULT_DIR}/memusage_spark.txt\n'  # initial memusage
    s += 'free -s 1 -b >> ${RESULT_DIR}/memusage_spark.txt 2>&1 &\n'
    s += 'FREEPID=$!\n'
    s += 'sleep 10\n'  # sleep for 10s to collect baseline memusage

    # codegen runs
    s += 'for i in $(seq 1 {}); do\n'.format(num_months)
    s += '\tv=$(printf %02d $i)\n'
    s += '\tfor ((r = 1; r <= NUM_RUNS; r++)); do\n'
    s += '\t\tLOG="${RESULT_DIR}/pyspark-01-$v-run-$r.txt"\n'
    s += '\t\techo $LOG\n'
    s += '\t\ttimeout $TIMEOUT spark-submit --master "local[*]" --driver-memory 100g runpyspark.py --path /disk/data/flights/ --num-months $v >$LOG 2>$LOG.stderr\n'
    s += '\tdone\n'
    s += 'done\n'
    s += '\n'
    s += 'date >> memusage_spark.txt && kill $FREEPID\n'
    s += 'spark-submit --version > ${RESULT_DIR}/spark_version.txt 2>&1\n'
    s += '\n'
    s += 'tar cvzf results.tar.gz ${RESULT_DIR}/\n'
    s += '\n'
    s += 'echo "Experiment on branch {}/{} done" >> email.txt\n'.format(branch, commit)
    s += 'date >> email.txt\n'
    s += 'python3 ../../scripts/sendmail.py -s "EC2 results ready" -b email.txt -e {} results.tar.gz\n'.format(email)
    return s


def create_dask_benchmark_script(branch, commit, num_runs=5,
                                 num_months=1, timeout=900, email='leonhard_spiegelberg@brown.edu'):
    s = '#!/usr/bin/env bash\n'
    s += '# auto generated benchmark script to run dask for {}, commit {}\n'.format(branch, commit)
    s += 'NUM_RUNS={}\n'.format(num_runs)
    s += 'TIMEOUT={}\n'.format(timeout)
    s += 'RESULT_DIR=results\n'
    s += 'source ~/.bashrc\n'
    s += '[ -d ${RESULT_DIR} ] && rm -rf ${RESULT_DIR}\n'
    s += 'mkdir -p ${RESULT_DIR}\n'
    s += 'date > ${RESULT_DIR}/memusage_dask.txt\n'  # initial memusage
    s += 'free -s 1 -b >> ${RESULT_DIR}/memusage_dask.txt 2>&1 &\n'
    s += 'FREEPID=$!\n'
    s += 'sleep 10\n'  # sleep for 10s to collect baseline memusage

    # codegen runs
    s += 'for i in $(seq 1 {}); do\n'.format(num_months)
    s += '\tv=$(printf %02d $i)\n'
    s += '\tfor ((r = 1; r <= NUM_RUNS; r++)); do\n'
    s += '\t\tLOG="${RESULT_DIR}/dask-01-$v-run-$r.txt"\n'
    s += '\t\techo $LOG\n'
    s += '\t\ttimeout $TIMEOUT python3 rundask.py --path /disk/data/flights/ --num-months $v >$LOG 2>$LOG.stderr\n'
    s += '\tdone\n'
    s += 'done\n'
    s += '\n'
    s += 'date >> memusage_dask.txt && kill $FREEPID\n'
    s += 'python3 -c "import dask;print(dask.__version__)" > ${RESULT_DIR}/dask_version.txt\n'
    s += '\n'
    s += 'tar cvzf results.tar.gz ${RESULT_DIR}/\n'
    s += '\n'
    s += 'echo "Experiment on branch {}/{} done" >> email.txt\n'.format(branch, commit)
    s += 'date >> email.txt\n'
    s += 'python3 ../../scripts/sendmail.py -s "EC2 results ready" -b email.txt -e {} results.tar.gz\n'.format(email)
    return s

def run_tuplex(instances, branch, enableLLVMOpt, enablePushdown, num_runs=5, num_months=10, email='leonhard_spiegelberg@brown.edu'):

    if enableLLVMOpt is None:
        enableLLVMOpt = False
    if enablePushdown is None:
        enablePushdown = False

    # switch to specific branch
    print('>>> cloning Tuplex repo')
    # checkout git repo if not exists
    run_cmd_on_instances(instances,
                         '[ ! -d /disk/Tuplex ] && git clone git@github.com:LeonhardFS/Tuplex.git /disk/Tuplex')
    out, err, _ = run_cmd_on_instances(instances,
                                       'cd /disk/Tuplex/ && git checkout origin/{} && cd -'.format(branch))
    gitmessage = err.splitlines()[-1]
    git_commit = gitmessage.split()[4]
    print(gitmessage)

    print('>>> building Tuplex')
    tstart = time.time()
    # remove build dir if it exists
    run_cmd_on_instances(instances, '[ -d /disk/Tuplex/build ] && rm -rf /disk/Tuplex/build')
    # create build dir and kick off release build for tuplex only!
    # note, need to source env vars from bashrc!
    out, err, retcode = run_cmd_on_instances(instances,
                                             'source ~/.bashrc && export PATH=/usr/local/bin:$PATH'
                                             ' && pushd /disk/Tuplex && mkdir -p build && cd build '
                                             '&& /snap/bin/cmake -DCMAKE_BUILD_TYPE=Release .. '
                                             '&& make -j32 tuplex && popd')
    # compilation successful?

    # ==> install tuplex in user mode!
    out, err, retcode = run_cmd_on_instances(instances,
                                             'cd /disk/Tuplex/build/dist/python && python3 setup.py install --user')
    print('>>> Build took: {:.2f}s'.format(time.time() - tstart))

    # creating run script for tuplex...
    bench_script = create_tuplex_benchmark_script(branch, git_commit, num_runs, num_months, 900, email)
    bench_path = 'benchmark_tuplex.sh'
    with open(bench_path, 'w') as fp:
        fp.write(bench_script)

    # add json with config TODO
    # add email.txt with text TODO
    config_file = 'tuplex_config.json'
    # conf for r5d.4xlarge (in total: 100GB, same as spark)
    conf = {"webui.enable": False,
            "executorMemory": "6G",
            "driverMemory": "10G",
            "partitionSize": "32MB",
            "runTimeMemory": "256MB",
            "useLLVMOptimizer": enableLLVMOpt,
            "csv.selectionPushdown": enablePushdown}
    with open(config_file, 'w') as fp:
        json.dump(conf, fp)

    assert os.path.exists(bench_path) and os.path.exists(config_file)

    # scp both files over to tuplex benchmark dir
    scp_to_instances(instances, bench_path, '/disk/Tuplex/benchmark/flights/')
    scp_to_instances(instances, config_file, '/disk/Tuplex/benchmark/flights/')
    scp_to_instances(instances, 'runtuplex.py', '/disk/Tuplex/benchmark/flights/') # local copy...

    os.remove(bench_path)
    os.remove(config_file)

    # create email.txt
    with open('email.txt', 'w') as f:
        f.write('Tuplex experiment on branch {}/{} with settings\n\n{}\n\n'
                'completed, please find results attached.\n'.format(branch, git_commit, conf))

    scp_to_instances(instances, 'email.txt', '/disk/Tuplex/benchmark/flights/')
    os.remove('email.txt')

    # create wrapper script for remote bg launch...
    with open('wrapper.sh', 'w') as f:
        f.write('#!/usr/bin/env bash\n')
        f.write('source ~/.bashrc && cd /disk/Tuplex/benchmark/flights'
                ' && bash {} > stdout.txt 2> stderr.txt &'.format(os.path.basename(bench_path)))

    scp_to_instances(instances, 'wrapper.sh', '/home/ubuntu/')
    os.remove('wrapper.sh')

    # all scripts are there, only remaining thing is to launch the wrapper and close inputs/outputs
    run_cmd_on_instances(instances, 'bash /home/ubuntu/wrapper.sh >&- 2>&- <&- &')
    print('>>> experiment started, wait for email with the results.')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tplx-enable-llvm-opt', dest='enable_llvm_opt', action='store_true', help='enable LLVM optimizers for Tuplex')
    parser.add_argument('--tplx-enable-pushdown', dest='enable_pushdown', action='store_true', help='enable projection pushdown for Tuplex')
    # TODO: null value opt, dict opt, ...
    parser.add_argument('-f', '--framework', dest='framework', default='tuplex', help='specify frameworks (tuplex, dask, spark) to run experiment for. E.g. --framework tuplex to run both tuplex and spark')
    parser.add_argument('--num-months', type=int, dest='num_months', default=None, help='specify on how many months to run the experiment on')
    parser.add_argument('-n', '--num-runs', type=int, dest='num_runs', default=5, help='how many times each configuration should run')
    parser.add_argument('--email', dest='email', default='leonhard_spiegelberg@brown.edu', help='specify SES registered email to send results to.')
    parser.add_argument('-b', '--branch', dest='branch', default='tplx179', help='specify which branch to use from tuplex repo')
    args = parser.parse_args()

    print(args)

    assert args.framework
    assert args.enable_llvm_opt is not None and args.enable_pushdown is not None

    # AWS args
    assert 'AWS_ACCESS_KEY_ID' in os.environ and 'AWS_SECRET_ACCESS_KEY' in os.environ, 'AWS credentials not set'

    aws_access_key = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']

    smtp_user = None
    smtp_password = None

    if args.email:
        assert 'SMTP_USER_NAME' in os.environ and 'SMTP_PASSWORD' in os.environ
        smtp_user = os.environ['SMTP_USER_NAME']
        smtp_password = os.environ['SMTP_PASSWORD']
        print('>>> found email, sending results to {}'.format(args.email))

    region = u'us-east-2'

    print('>>> creating EC2 connection')
    conn = get_connection(aws_access_key, aws_secret_key)

    # make new spot request
    print('>>> starting new instance')
    instances = start_instance(conn)
    for inst in instances:
        print('- instance {} available under {}'.format(inst['id'], inst['public_dns_name']))
        print('  login via: ssh -i ~/.ssh/tuplex.pem ubuntu@{}'.format(inst['public_dns_name']))

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
        out, _, _ = run_cmd_on_instances(instances, 'cat /proc/mdstat')
        if 'md0 : active raid0' not in out:
            print('disk not yet initialized, sleep...')
            time.sleep(1)
        else:
            break
    assert 'md0 : active raid0' in out

    run_cmd_on_instances(instances, 'sudo mkdir -p /disk')

    # create file system and mount disks
    run_cmd_on_instances(instances, 'sudo mkdir -p /disk')
    run_cmd_on_instances(instances, 'sudo mkfs.ext4 -F /dev/md0')

    # mount /dev/md0 under /disk
    run_cmd_on_instances(instances, 'sudo mount /dev/md0 /disk')

    # you can check available disk space via `df -h -x devtmpfs -x tmpfs`

    # make /disk owned by current user (ubuntu/ec2-user)
    run_cmd_on_instances(instances, 'sudo chown $(whoami) /disk')

    print(' -- done -- ')

    # copy over ssh keys
    # make sure the key repokey is added to github as deployment key (only pull access!)
    print('>>> setting up SSH keys for repo access on instance')
    run_cmd_on_instances(instances, 'mkdir -p ~/.ssh')
    scp_to_instances(instances, '../keys/repokey.pem', '~/.ssh/repokey.pem')
    run_cmd_on_instances(instances, 'chmod 400 ~/.ssh/repokey.pem') # change permissions, important!


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

    scp_to_instances(instances, 'sshconfig.txt', '~/.ssh/config')
    os.remove('sshconfig.txt')

    # setup AWS CLI
    print('>>> setting up AWS S3 access & Co. on instance')
    # set up aws cli on instance (needed because requester pays S3!)
    run_cmd_on_instances(instances, 'sudo snap install --classic aws-cli')
    run_cmd_on_instances(instances, 'echo "export PATH=/snap/bin:\\$PATH" >> ~/.bashrc')

    # add env vars to .bashrc
    # aws_access_key = os.environ['AWS_ACCESS_KEY_ID']
    # aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
    run_cmd_on_instances(instances, 'echo "export AWS_ACCESS_KEY_ID={}" >> ~/.bashrc'.format(aws_access_key),
                         quiet=True)
    run_cmd_on_instances(instances, 'echo "export AWS_SECRET_ACCESS_KEY={}" >> ~/.bashrc'.format(aws_secret_key),
                         quiet=True)
    if args.email:
        run_cmd_on_instances(instances, 'echo "export SMTP_USER_NAME={}" >> ~/.bashrc'.format(smtp_user),
                             quiet=True)
        run_cmd_on_instances(instances, 'echo "export SMTP_PASSWORD={}" >> ~/.bashrc'.format(smtp_password),
                             quiet=True)

    # copy over credentials fils
    print('>>> downloading data files')
    run_cmd_on_instances(instances, 'mkdir -p ~/.aws')
    scp_to_instances(instances, '~/.aws/credentials', '~/.aws/credentials')

    # downloading datafiles
    flight_data_uri = 's3://tuplex-public/data/flights2018-2019.tar.gz'

    run_cmd_on_instances(instances,
                         'mkdir -p /disk/data/flights && cd /disk/data/flights && /snap/bin/aws s3 cp {} .'.format(
                             flight_data_uri))
    run_cmd_on_instances(instances,
                         'cd /disk/data/flights && tar xf flights2018-2019.tar.gz && rm flights2018-2019.tar.gz')

    # TODO: 2018_01 is faulty, remove
    run_cmd_on_instances(instances, 'rm /disk/data/flights/flights_on_time_performance_2018_01.csv')


    #  -- setup done --
    # running experiments for frameworks
    fw = args.framework

    if fw == 'tuplex':
        run_tuplex(instances, args.branch, args.enable_llvm_opt, args.enable_pushdown)
    # elif fw == 'spark':
    #     run_spark()
    # elif fw == 'dask':
    #     run_dask()
    else:
        raise Exception('Unknown framework {} encountered!'.format(fw))

    # # terminate all instances
    #terminate_instances(conn, instances)

if __name__ == '__main__':
    main()
