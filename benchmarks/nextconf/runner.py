#!/usr/bin/env python3

# CLI to run experiments/reproduce easily
try:
    import os.path
    import click
    import logging
    import subprocess
    import docker
    import gdown
    import sys
    import os
    import boto3
    import tarfile
    import io
    import time
    import pathlib
except ModuleNotFoundError as e:
    logging.error("Module not found: {}".format(e))
    logging.info("Install missing modules via {} -m pip install -r requirements.txt".format(sys.executable))
    sys.exit(1)

# subtargets can be done via /
experiment_targets = ['all',
                      'flights/sampling',
                      'flights/hyper',
                      'flights/years',
                      'flights/stratified',
                      'github/promo',
                      'github/global']

experiment_targets_description = {'all':'a meta target to run all experiments',
                                  'flights/sampling': 'runs the flights query using different sampling schemes',
                                  'flights/hyper': 'runs the flights query in general sampling mode and hyperspecialized sampling on the lambdas',
                                  'flights/years': 'runs flights query with varying filter predicate',
                                  'flights/stratified': 'runs flights query with varying samples per strata',
                                  'github/promo': 'runs github query with filter promotion off/on',
                                  'github/global': 'runs github query, but only the global mode'}

experiments_path_dict = {'flights/hyper': {'script': 'benchmark-filter.sh', 'wd': '/code/benchmarks/nextconf/hyperspecialization/flights', 'result_dir': '/code/benchmarks/nextconf/hyperspecialization/flights/experimental_results/filter'},
                         'flights/years': {'script': 'benchmark-all-years.sh', 'wd': '/code/benchmarks/nextconf/hyperspecialization/flights', 'result_dir': '/code/benchmarks/nextconf/hyperspecialization/flights/experimental_results/years'},
                         'flights/stratified': {'script': 'benchmark-hyper-stratified-sampling.sh', 'wd': '/code/benchmarks/nextconf/hyperspecialization/flights', 'result_dir': '/code/benchmarks/nextconf/hyperspecialization/flights/experimental_results/stratified-sampling'},
                         'github/promo': {'script': 'benchmark.sh', 'wd': '/code/benchmarks/nextconf/hyperspecialization/github', 'result_dir': '/code/benchmarks/nextconf/hyperspecialization/github/experimental_results/github'},
                         'github/global': {'script': 'benchmark-only-global.sh', 'wd': '/code/benchmarks/nextconf/hyperspecialization/github', 'result_dir': '/code/benchmarks/nextconf/hyperspecialization/github/experimental_results/github-global'},
                         }


# make sure every target has a description!
for target in experiment_targets:
    assert target in experiment_targets_description.keys(), 'missing description for target {}'.format(target)

def make_meta_targets(targets):
    m = []

    for name in targets:
        parts = name.split('/')
        n = len(parts)
        for i in range(1, n):
            m.append('/'.join(parts[:i]))

    m.append('all')
    return sorted(list(set(m)))

#  meta targets, i.e. targets that are comprised of other targets
meta_targets = make_meta_targets(experiment_targets)

plot_targets = []

# default paths
DEFAULT_RESULT_PATH = 'r5d.8xlarge'
DEFAULT_OUTPUT_PATH = 'plots'
DOCKER_IMAGE_TAG = 'tuplex/benchmarkii'
DOCKER_CONTAINER_NAME = 'vldb23'

# hidden config file to check compatibility of settings
CONFIG_FILE='.config.json'

BUILD_CACHE='build_cache'
def build_cache():
   os.makedirs(BUILD_CACHE, exist_ok=True)
   return BUILD_CACHE


def get_container():
    # docker client
    dc = docker.from_env()

    # check if it's already running, if not start!
    containers = [c for c in dc.containers.list() if c.name == DOCKER_CONTAINER_NAME]
    container = None
    if len(containers) >= 1:
        logging.info('Docker container {} already running.'.format(DOCKER_CONTAINER_NAME))
        container = containers[0]
    else:
        logging.info('Docker container not running yet, starting...')
        start_container()
        containers = [c for c in dc.containers.list() if c.name == DOCKER_CONTAINER_NAME]
        assert len(containers) >= 1, 'Failed to start docker container...'
        container = containers[0]
        logging.info('Docker container started.')

    return container

def retrieve_targets_to_run(name):
    # decode meta targets
    name = name.lower()
    if name == 'all':
        return experiment_targets

    # prefix!
    targets = []
    for ename in experiment_targets:
        if ename.lower() == name or ename.lower().startswith(name + '/'):
            targets.append(ename)
    return sorted(targets)

def build_docker_image():
    """build docker image, this may take a while"""
    # --> could either download or build from scratch
    assert DOCKER_IMAGE_TAG == 'tuplex/benchmarkii', 'support only for image ' + DOCKER_IMAGE_TAG
    image_script_path = 'scripts/docker/benchmarkII/create-image.sh'

    logging.info('Building docker image {} from scratch, invoking {}.'.format(DOCKER_IMAGE_TAG, image_script_path))
    tstart_build = time.time()
    script_abs_path = os.path.join('tuplex', image_script_path)

    assert os.path.isfile(script_abs_path), 'Could not find script under ./{}'.format(script_abs_path)

    cmd = ['bash', script_abs_path]
    env = os.environ.copy()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, env=env)
    for line in iter(p.stdout.readline, b''):
        logging.info(line.decode().strip())
    p.stdout.close()
    p.wait()
    build_time_in_s = time.time() - tstart_build
    if 0 == p.returncode:
        logging.info('Docker image successfully built in {:.1f}s'.format(build_time_in_s))
    else:
        logging.info(f'Building docker image failed with rc={p.returncode} in {build_time_in_s:.1f}s')
        raise Exception('Building docker image failed')

@click.command()
@click.argument('target', type=click.Choice(sorted(list(set(meta_targets + experiment_targets))), case_sensitive=False))
@click.option('--num-runs', type=int, default=1, #11,
              help='How many runs to run experiment with (default=11 for 10 runs + 1 warmup run')
@click.option('--detach/--no-detach', default=False, help='whether to launch command in detached mode (non-blocking)')
@click.option('--help/--no-help', default=False, help='display help about target')
@click.pass_context
def run(ctx, target, num_runs, detach, help):
    """ run benchmarks for specific dataset. THIS MIGHT TAKE A WHILE! """

    logging.info("Retrieving AWS credentials")
    session = boto3.Session()
    credentials = session.get_credentials()

    assert credentials, 'No AWS credentials found'

    # Credentials are refreshable, so accessing your access key / secret key
    # separately can lead to a race condition. Use this to get an actual matched
    # set.
    credentials = credentials.get_frozen_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    token = credentials.token
    region = session.region_name
    logging.info('AWS credentials found access key [{}] secret key [{}] token [{}].'.format('x' if access_key else ' ',
                                                                'x' if secret_key else ' ',
                                                                'x' if token else ' ',))
    logging.info('Running for AWS region={}.'.format(region))
    logging.info('Running experiments for target {}'.format(target))
    targets_to_run = retrieve_targets_to_run(target)
    if len(targets_to_run) > 1:
        logging.info('requires running targets {}'.format(targets_to_run))

    if help:
        print('Detailed descriptions of experimental targets:\n')
        # display help and exit
        for target in targets_to_run:
            print('\t{}:\n\t{}'.format(target, '-' * ( 1+ len(target))))
            desc = experiment_targets_description[target]
            if not desc.endswith('.'):
                desc += '.'
            print('\t{}\n'.format(desc))
        print('Running experiments requires docker daemon running.')

        sys.exit(0)

    # docker client
    dc = docker.from_env()

    # check if it's already running, if not start!
    containers = [c for c in dc.containers.list() if c.name == DOCKER_CONTAINER_NAME]
    container = None
    if len(containers) >= 1:
        logging.info('Docker container {} already running.'.format(DOCKER_CONTAINER_NAME))
        container = containers[0]
    else:
        logging.info('Docker container not running yet, starting...')
        start_container()
        containers = [c for c in dc.containers.list() if c.name == DOCKER_CONTAINER_NAME]
        assert len(containers) >= 1, 'Failed to start docker container...'
        container = containers[0]
        logging.info('Docker container started.')

    assert container is not None, 'container not valid?'

    # check that tuplex exists python3.9 -c "import tuplex"
    cmd = 'python3.9 -c "import tuplex"'
    exit_code, output = container.exec_run(cmd, stderr=True, stdout=True, detach=detach)
    if 0 != exit_code:
        logging.error("Did not find tuplex module, please run first build subcommand.")
        sys.exit(1)

    required_paths_in_docker = []
    # check that each file exists
    file_failure = False
    for i, path in enumerate(required_paths_in_docker):
        cmd = "test -f {}".format(path)
        exit_code, output = container.exec_run(cmd, stderr=True, stdout=True, detach=detach)
        if 0 != exit_code:
            logging.error("path {} not found in docker container, please check host.".format(path))
            file_failure = True

        if i % 100 == 0 and i > 0:
            logging.info('Checked {}/{} files for presence in container...'.format(i, len(required_paths_in_docker)))
    logging.info('File check completed.')
    if file_failure:
        logging.error("Not all files required to run benchmarks are present, aborting benchmark.")
        sys.exit(1)


    num_targets_run = 0
    for target in targets_to_run:
        # run individual targets
        # for these, the runbenchmark.sh scripts are used!
        # e.g., docker exec -e NUM_RUNS=1 sigmod21 bash -c 'cd /code/benchmarks/zillow/Z1/ && bash runbenchmark.sh'

        # lowercase
        target = target.lower()
        results_root_dir = 'results'
        local_result_dir = os.path.join(results_root_dir, target)
        log_run_path = os.path.join(local_result_dir, 'experiment-log.txt')


        if not target in experiments_path_dict.keys():
            logging.error('target {} not found, skip.'.format(target))
            continue

        os.makedirs(local_result_dir, exist_ok=True)
        
        # get script
        benchmark_path = experiments_path_dict[target]['wd']
        benchmark_script = experiments_path_dict[target]['script']
        benchmark_result_dir = experiments_path_dict[target]['result_dir']
        benchmark_output_name = target.replace('/', '_')

        cmd = 'bash -c "cd {} && bash {}"'.format(benchmark_path, benchmark_script)
        env = {'NUM_RUNS': num_runs,
               'PATH': '/opt/spark@2/bin:/bin:/opt/scala/bin:/root/.cargo/bin:/root/.cargo/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
               'SPARK_HOME': '/opt/spark@2',
               'SCALA_HOME': '/opt/scala'}
        env.update(get_aws_env())
        logging.info('Starting benchmark using command: docker exec -i{}t {} {}'.format('d' if detach else '', DOCKER_CONTAINER_NAME,
                                                                                        cmd))
        exit_code, output = container.exec_run(cmd, stderr=True, stdout=True, detach=detach, environment=env)

        # save output to log path
        with open(log_run_path, 'w') as fp:
            fp.write(output.decode() if isinstance(output, bytes) else output)

        # copy results from docker container over

        logging.info('Finished with code: {}'.format(exit_code))
        logging.info('Output:\n{}'.format(output.decode() if isinstance(output, bytes) else output))
        if detach:
            logging.info('Started command in detached mode, to stop container use "stop" command')

        # create tar out of result and copy back to machine
        output_archive = benchmark_output_name + '.tar.gz'
        cmd = 'bash -c "cd {} && tar cvzf {} *"'.format(benchmark_result_dir, output_archive)
        exit_code, output = container.exec_run(cmd, stderr=True, stdout=True, detach=detach, environment=env)
        if exit_code != 0:
            logging.error("Failed creating result archive, details: " + str(output.decode()))
            sys.exit(1)
        # copy back
        local_dest = os.path.join(local_result_dir, output_archive)
        copy_from_container(container, os.path.join(benchmark_result_dir, output_archive), local_dest)

        # remove file
        cmd = 'bash -c "cd {} && rm {} *"'.format(benchmark_result_dir, output_archive)
        exit_code, output = container.exec_run(cmd, stderr=True, stdout=True, detach=detach, environment=env)

        num_targets_run += 1
    logging.info('Done ({}/{} targets).'.format(num_targets_run, len(targets_to_run)))

def find_image():
    dc = docker.from_env()
    found_tags = [img.tags for img in dc.images.list() if len(img.tags) > 0]
    found_tags = [tags[0] for tags in found_tags]

    logging.info('Found following images: {}'.format('\n'.join(found_tags)))

    # check whether tag is part of it
    found_image = False
    for tag in found_tags:
        if tag.startswith(DOCKER_IMAGE_TAG):
            found_image = True
    return found_image

def start_container():
    # docker client
    dc = docker.from_env()

    # check whether tuplex/benchmark image exists. If not run create-image script!
    logging.info('Checking whether tuplex/benchmark image exists locally...')
    if find_image():
        # check if it's already running, if not start!
        containers = [c for c in dc.containers.list() if c.name == DOCKER_CONTAINER_NAME]
        if len(containers) >= 1:
            logging.info('Docker container {} already running.'.format(DOCKER_CONTAINER_NAME))
        else:
            logging.info('Starting up docker container')
            # docker run -v /disk/data:/data -v /disk/benchmark_results:/results -v /disk/tuplex-public:/code --name sigmod21 --rm -dit tuplex/benchmark
            # check directory layout!

            REPO_PATH=os.path.join(os.getcwd(), 'tuplex')
            logging.info('Mounting source code from {}'.format(REPO_PATH))
            volumes = {REPO_PATH: {'bind': '/code', 'mode': 'rw'}}

            dc.containers.run(DOCKER_IMAGE_TAG + ':latest', name=DOCKER_CONTAINER_NAME,
                              tty=True, stdin_open=True, detach=True, volumes=volumes, remove=True)
            logging.info('Started docker container {} from image {}'.format(DOCKER_CONTAINER_NAME, DOCKER_IMAGE_TAG))
    else:
        # build image upon first invocation...
        build_docker_image()
        logging.info("attempting to start container...")

        # check that image is there, if not fail!
        if not find_image():
            logging.error('Did not find docker image {}, consider building it!'.format(DOCKER_IMAGE_TAG))
            raise Exception('Docker image not found')
        start_container()

@click.command()
def start():
    """start Viton VLDB'23 experimental container"""
    start_container()


@click.command()
def stop():
    """stop Viton VLDB'23 experimental container"""

    # docker client
    dc = docker.from_env()

    containers = [c for c in dc.containers.list() if c.name == DOCKER_CONTAINER_NAME]
    if len(containers) >= 1:
        logging.info('Found docker container {}, stopping now...'.format(DOCKER_CONTAINER_NAME))
        c = containers[0]
        c.kill()  # use kill
        try:
            c.remove()
        except:
            pass
        logging.info('Container stopped.')
    else:
        logging.info('No docker container found with name {}, nothing todo.'.format(DOCKER_CONTAINER_NAME))


@click.command()
@click.argument('target', type=click.Choice(plot_targets, case_sensitive=False))
@click.option('--output-path', type=str, default=DEFAULT_OUTPUT_PATH,
              help='path where to save plots to, default={}'.format(DEFAULT_OUTPUT_PATH))
@click.option('--input-path', type=str, default=DEFAULT_RESULT_PATH,
              help='path from where to read experimental logs, default={}'.format(DEFAULT_RESULT_PATH))
def plot(target, output_path, input_path):
    logging.info('Plotting not yet implemented.')


def create_package_tar(dest_path, src_path, lambda_src_path=None):
    """ helper function to create combined package for build_cache """
    allowed_prefixes = ['python/tuplex', 'python/setup.py', 'python/MANIFEST.in']
    dest_root = ''

    with tarfile.open(src_path, 'r') as tf:

        mems = tf.getmembers()

        with tarfile.open(dest_path, 'w:gz') as target:
            for obj in mems:

                if len(list(filter(lambda prefix: obj.name.startswith(prefix), allowed_prefixes))) > 0:
                    if obj.name.endswith('tar.gz'):
                        continue

                    if os.path.splitext(obj.name)[1][1:] not in {'in', 'py', 'so', 'dylib', 'toml'}:
                        continue
                    fileobj = tf.extractfile(obj)
                    obj.name = obj.name.replace('python/', dest_root)
                    target.addfile(obj, fileobj)
                    logging.debug('adding {}'.format(obj.name))
            if lambda_src_path is not None:
                with tarfile.open(lambda_src_path, 'r') as tf_lam:
                    obj = list(filter(lambda x: x.name.endswith('tplxlam.zip'), tf_lam.getmembers()))[0]
                    fileobj = tf_lam.extractfile(obj)
                    obj.name = os.path.join(dest_root, 'tuplex/other/tplxlam.zip')
                    target.addfile(obj, fileobj)
                    logging.debug('adding {}'.format(obj.name))


@click.command()
@click.option('--cereal/--no-cereal', is_flag=True, default=True,
              help='whether to build tuplex and lambda using cereal as serialization library.')
@click.pass_context
def build(ctx, cereal):
    """Downloads tuplex repo to tuplex, switches to correct branch and builds it using the sigmod21 experiment container."""

    GIT_REPO_URI = 'https://github.com/LeonhardFS/tuplex-public'
    GIT_BRANCH = 'origin/lambda-exp'
    if not os.path.isdir('tuplex'):
        logging.info('Tuplex repo does not exist here yet, cloning')

        cmd = ['git', 'clone', GIT_REPO_URI, 'tuplex']

        logging.info('Running {}'.format(' '.join(cmd)))
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # set a timeout of 2 seconds to keep everything interactive
        p_stdout, p_stderr = process.communicate(timeout=300)

    # checkout sigmod-repro branch
    cmd = ['git', 'checkout', '--track', GIT_BRANCH]

    logging.info('Running {}'.format(' '.join(cmd)))
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='tuplex')
    # set a timeout of 2 seconds to keep everything interactive
    p_stdout, p_stderr = process.communicate(timeout=300)

    # update repo if it exists
    cmd = ['git', 'pull']
    logging.info('Running {}'.format(' '.join(cmd)))
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd='tuplex')
    # set a timeout of 2 seconds to keep everything interactive
    p_stdout, p_stderr = process.communicate(timeout=300)
    if process.returncode != 0:
        logging.error('updating git directory failed, details: {}'.format(p_stderr))
        sys.exit(process.returncode)

    start_container()

    CEREAL_FLAG='BUILD_WITH_CEREAL={}'.format('ON' if cereal else 'OFF')
    if cereal:
        logging.info('Building with Cereal as serialization format')

    # build tuplex within docker container & install it there as well!
    # i.e. build command is: docker exec sigmod21 bash /code/benchmarks/sigmod21-reproducibility/build_scripts/build_tuplex.sh
    BUILD_SCRIPT_PATH = '/code/benchmarks/nextconf/build_scripts/build_tuplex.sh'
    cmd = ['docker', 'exec', '-e', CEREAL_FLAG, DOCKER_CONTAINER_NAME, 'bash', BUILD_SCRIPT_PATH]

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    for line in iter(p.stdout.readline, b''):
        logging.info(line.decode().strip())
    p.stdout.close()
    p.wait()


    # note: building the lambda within the experimental docker container does not work yet/is buggy.
    # isntead, build using the provided ./scripts/create_lambda_zip and copy contents!
    # logging.info('Built in docker container.')
    # logging.info('Building compatible Lambda runner now...')
    # BUILD_SCRIPT_PATH = '/code/benchmarks/nextconf/build_scripts/build_lambda.sh'
    # cmd = ['docker', 'exec', '-e', CEREAL_FLAG, DOCKER_CONTAINER_NAME, 'bash', BUILD_SCRIPT_PATH]
    #
    # p = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1)
    # for line in iter(p.stdout.readline, b''):
    #     logging.info(line.decode().strip())
    # p.stdout.close()
    # p.wait()
    #
    # logging.info('Lambda runner built.')

    logging.info("Building lambda runner...")
    BUILD_SCRIPT_PATH = './scripts/create_lambda_zip.sh'
    cmd = ['bash', os.path.join('tuplex', BUILD_SCRIPT_PATH)]

    env = os.environ.copy()
    env['BUILD_WITH_CEREAL'] = 'ON' if cereal else 'OFF'
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1, env=env)
    for line in iter(p.stdout.readline, b''):
        logging.info(line.decode().strip())
    p.stdout.close()
    p.wait()
    logging.info("Lambda runner built.")

    logging.info('Copying results to host machine...')

    storage_path = os.path.join(build_cache(), 'cereal' if cereal else 'nocereal')
    os.makedirs(storage_path, exist_ok=True)

    container = get_container()
    assert container, 'Container should have been started'

    # use container API (get_archive/put_archive)
    # cf. docker-py.readthedocs.io
    # fetch both lambda and package
    package_container_path = '/code/tuplex/build/dist/python'
    package_path = os.path.join(storage_path, 'tuplex.tar')
    lambda_path = os.path.join(storage_path, 'lambda-runner.tar')
    bits, stat = container.get_archive(package_container_path)
    with open(package_path, 'wb') as fp:
        for chunk in bits:
            fp.write(chunk)
    logging.info('Transferred python package from docker to {} ({} bytes)'.format(package_path, stat['size']))

    # # when building lambda from within
    # lambda_container_path = '/code/tuplex/build-lambda/tplxlam.zip'
    # bits, stat = container.get_archive(lambda_container_path)
    # with open(lambda_path, 'wb') as fp:
    #     for chunk in bits:
    #         fp.write(chunk)
    # logging.info('Transferred lambda runner from docker to {} ({} bytes)'.format(lambda_path, stat['size']))

    # copy file from build-lambda/tplxlam.zip to storage path
    lambda_local_path = './tuplex/build-lambda/tplxlam.zip'
    with tarfile.open(lambda_path, 'w') as tf:
        tf.add(lambda_local_path, 'tplxlam.zip')

    # create combined archive & store it as package.tar.gz
    dest_path = os.path.join(storage_path, 'tuplex-package.tar.gz')
    create_package_tar(dest_path, package_path, lambda_path)
    os.remove(package_path)
    os.remove(lambda_path)
    logging.info('Done, built all required artifacts and created combined archive {}.'.format(dest_path))

def copy_to_container(container: 'Container', src: str, dst_dir: str):
    """ src shall be an absolute path """
    # stream = io.BytesIO()
    # with tarfile.open(fileobj=stream, mode='w|') as tar, open(src, 'rb') as f:
    #     info = tar.gettarinfo(fileobj=f)
    #     info.name = os.path.basename(src)
    #     tar.addfile(info, f)
    #
    # container.put_archive(dst_dir, stream.getvalue())

    tstart = time.time()

    # create dir if not exists in container
    parent_dir = str(pathlib.Path(dst_dir).parent)
    cmd = ['mkdir', '-p', parent_dir]
    exit_code, output = container.exec_run(cmd)
    if exit_code != 0:
        logging.error(output.decode())
        sys.exit(exit_code)

    # is src file? append to dst dir if necessary!
    if os.path.isfile(src) and parent_dir == dst_dir:
        dst_dir = os.path.join(dst_dir, os.path.basename(src))

    cmd = ['docker', 'cp', os.path.abspath(src), '{}:{}'.format(container.name, dst_dir)]
    logging.info('Running {}'.format(' '.join(cmd)))
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # set a timeout of 2 seconds to keep everything interactive
    p_stdout, p_stderr = process.communicate(timeout=300)

    if process.returncode != 0:
        logging.error(p_stderr.decode())
        sys.exit(process.returncode)
    logging.info('Copied to container {} in {:.2f}s'.format(container.name, time.time() - tstart))

def copy_from_container(container: 'Container', src: str, dst: str):
    """ src shall be an absolute path """

    tstart = time.time()

    cmd = ['docker', 'cp', '{}:{}'.format(container.name, src), os.path.abspath(dst)]
    logging.info('Running {}'.format(' '.join(cmd)))
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # set a timeout of 2 seconds to keep everything interactive
    p_stdout, p_stderr = process.communicate(timeout=300)

    if process.returncode != 0:
        logging.error(p_stderr.decode())
        sys.exit(process.returncode)
    logging.info('Copied from container {} in {:.2f}s to {}'.format(container.name, time.time() - tstart, dst))

def get_aws_env():
    session = boto3.Session()
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    token = credentials.token
    region = session.region_name

    return {'AWS_ACCESS_KEY_ID': access_key, 'AWS_SECRET_ACCESS_KEY': secret_key, 'AWS_DEFAULT_REGION': region}


# the container.exec_run is buggy, therefore invoke appropriate command directly with AWS credentials
def docker_exec(container: 'Container', cmd):
    tstart = time.time()
    logging.info('Running {}'.format(' '.join(cmd)))

    aws = get_aws_env()
    env_flags = []
    for k, v in aws.items():
        env_flags += ['-e', '{}={}'.format(k, v)]

    # ['bash', '-c', 'python3.9 -c "print(\'test\')"'] # <-- this allows to correctly capture output.
    cmd = ['docker', 'exec'] + env_flags + [container.name] + ['bash', '-c', ' '.join(cmd)]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, universal_newlines=True)
    # set a timeout of 2 seconds to keep everything interactive
    p_stdout, p_stderr = process.communicate(timeout=300)

    if process.returncode != 0:
        logging.error(p_stdout)
        logging.error(p_stderr)
        sys.exit(process.returncode)
    logging.info('Completed command in {:.2f}s'.format(time.time() - tstart))

@click.command()
@click.option('--cereal/--no-cereal', is_flag=True, default=True,
              help='whether to build tuplex and lambda using cereal as serialization library.')
@click.pass_context
def deploy(ctx, cereal):
    """ deploy Lambda runner to AWS and install compatible version in docker container. If build cache has no package, build first (may take a while) """
    logging.info("Deploying Lambda runner & installing package in docker container")

    storage_path = os.path.join(build_cache(), 'cereal' if cereal else 'nocereal')
    package_path = os.path.join(storage_path, 'tuplex-package.tar.gz')

    if not os.path.isfile(package_path):
        logging.warning('Could not find artifacts under {}, building first (may take a while)'.format(storage_path))

        result = ctx.invoke(build, cereal=cereal)
        sys.stdout.writelines(result)

    assert os.path.isfile(package_path), 'internal corruption?'

    logging.info('Installing into container')

    container = get_container()
    # container.put_archive('/tmp/tuplex', open(package_path, 'rb').read())
    copy_to_container(container, package_path, '/tmp/tuplex/package.tar.gz')

    # run extract cmd tar xf /tmp/tuplex/package.tar.gz -C /tmp/tuplex
    cmd = ['tar', 'xf', '/tmp/tuplex/package.tar.gz', '-C', '/tmp/tuplex']
    exit_code, output = container.exec_run(cmd)
    if exit_code != 0:
        logging.error(output.decode())
        sys.exit(exit_code)

    # jupyter install is broken, manual fix here.
    cmd = ['pip', 'install', 'jupyter']
    exit_code, output = container.exec_run(cmd)
    if exit_code != 0:
        logging.error(output.decode())
        sys.exit(exit_code)

    cmd = ['python3.9', '/tmp/tuplex/setup.py', 'install']
    exit_code, output = container.exec_run(cmd)
    if exit_code != 0:
        logging.error(output.decode())
        sys.exit(exit_code)
    logging.info('Installed Tuplex')
    # python3.9 -m pip install boto3
    cmd = ['python3.9', '-m', 'pip', 'install', 'boto3']
    exit_code, output = container.exec_run(cmd)
    if exit_code != 0:
        logging.error(output.decode())
        sys.exit(exit_code)
    logging.info('Installed Boto3')

    logging.info('Deploying runner to AWS Lambda...')

    cmd = ["python3.9", "-c", "'import tuplex.distributed; tuplex.distributed.setup_aws()'"]
    docker_exec(container, cmd)

    logging.info('Done.')
    logging.info('copied file to container')


def add_aws_info(f):
    """
    Add the version of the tool to the help heading.
    :param f: function to decorate
    :return: decorated function
    """

    # fetch some info about aws and display it in runner script
    session = boto3.Session()
    credentials = session.get_credentials()
    credentials = credentials.get_frozen_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    token = credentials.token
    region = session.region_name
    info = 'AWS credentials found: access key [{}] secret key [{}] token [{}]'.format('x' if access_key else ' ',
                                                                'x' if secret_key else ' ',
                                                                'x' if token else ' ',)
    info += '\n\nAWS region={}, '.format(region)
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()
    account_id = identity['Account']
    user_name = identity['Arn'][identity['Arn'].rfind('user/') + len('user/'):]
    info += 'IAM user: {} (id={})'.format(user_name, account_id)

    doc = f.__doc__

    f.__doc__ = doc.strip() + '\n\n' + info

    return f

# customize help
@click.group()
@click.pass_context
@add_aws_info
def commands(ctx):
    """
    Welcome to the runner tool to invoke various Viton benchmarks.
    """
    pass


commands.add_command(run)
commands.add_command(plot)
commands.add_command(build)
commands.add_command(deploy)
commands.add_command(start)
commands.add_command(stop)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(levelname)s: %(message)s',
                        handlers=[logging.FileHandler("experiment.log", mode='w'),
                                  logging.StreamHandler()])
    stream_handler = [h for h in logging.root.handlers if isinstance(h, logging.StreamHandler)][0]
    stream_handler.setLevel(logging.INFO)

    commands()
