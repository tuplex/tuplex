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
except ModuleNotFoundError as e:
    logging.error("Module not found: {}".format(e))
    logging.info("Install missing modules via {} -m pip install -r requirements.txt".format(sys.executable))
    sys.exit(1)

#  meta targets, i.e. targets that are comprised of other targets
meta_targets = ['all', 'zillow', 'tpch', 'flights']

experiment_targets = ['all', 'zillow', 'flights', 'logs', '311',
                      'tpch', 'zillow/Z1', 'zillow/Z2', 'zillow/exceptions',
                      'tpch/Q06', 'tpch/Q19', 'flights/breakdown', 'flights/flights']

plot_targets = ['all', 'figure3', 'figure4', 'figure5',
                'figure6', 'figure7', 'figure8', 'figure9', 'figure10', 'table3']

# default paths
DEFAULT_RESULT_PATH = 'r5d.8xlarge'
DEFAULT_OUTPUT_PATH = 'plots'
DOCKER_IMAGE_TAG = 'tuplex/benchmarkii'
DOCKER_CONTAINER_NAME = 'vldb22'

# hidden config file to check compatibility of settings
CONFIG_FILE='.config.json'

BUILD_CACHE='build_cache'
def build_cache():
   os.makedirs(BUILD_CACHE, exist_ok=True)
   return BUILD_CACHE


@click.command()
@click.argument('target', type=click.Choice(experiment_targets, case_sensitive=False))
@click.option('--num-runs', type=int, default=11,
              help='How many runs to run experiment with (default=11 for 10 runs + 1 warmup run')
@click.option('--detach/--no-detach', default=False, help='whether to launch command in detached mode (non-blocking)')
@click.pass_context
def run(ctx, target, num_runs, detach):
    """ run benchmarks for specific dataset. THIS MIGHT TAKE A WHILE! """

    logging.info("Retrieving AWS credentials")
    session = boto3.Session()
    credentials = session.get_credentials()

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
    sys.exit(0)
    logging.info('Running experiments for target {}'.format(target))

    # experiment_targets = ['all', 'zillow', 'flights', 'logs', '311',
    #                       'tpch', 'zillow/Z1', 'zillow/Z2', 'zillow/exceptions',
    #                       'tpch/Q06', 'tpch/Q19', 'flights/breakdown', 'flights/flights']

    targets = []
    target = target.lower()

    # decode compound targets...
    if target == 'zillow':
        targets += ['zillow/Z1', 'zillow/Z2', 'zillow/exceptions']
    elif target == 'flights':
        targets += ['flights/flights', 'flights/breakdown']
    elif target == 'tpch':
        targets += ['tpch/q06', 'tpch/q19']
    elif target == 'all':
        targets = [name.lower() for name in experiment_targets if name.lower() not in meta_targets]
    else:
        targets = [target]

    targets = sorted(targets)

    if len(targets) > 1:
        logging.info('Target {} comprised of subtargets:\n\t- {}'.format(target, '\n\t- '.join(targets)))

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

    # check that tuplex exists python3.6 -c "import tuplex"
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


    for target in targets:
        # run individual targets
        # for these, the runbenchmark.sh scripts are used!
        # e.g., docker exec -e NUM_RUNS=1 sigmod21 bash -c 'cd /code/benchmarks/zillow/Z1/ && bash runbenchmark.sh'
        path_dict = {'zillow/z1': '/code/benchmarks/zillow/Z1/',
                     'zillow/z2': '/code/benchmarks/zillow/Z2/',
                     'zillow/exceptions': '/code/benchmarks/dirty_zillow/',
                     'logs': '/code/benchmarks/logs/',
                     '311': '/code/benchmarks/311/',
                     'tpch/q06': '/code/benchmarks/tpch/Q06/',
                     'tpch/q19': '/code/benchmarks/tpch/Q19/',
                     'flights/flights': '/code/benchmarks/flights/',
                     'flights/breakdown': '/code/benchmarks/flights/'}

        # lowercase
        target = target.lower()

        benchmark_path = path_dict[target]
        benchmark_script = 'runbenchmark.sh'

        # only for flights/breakdown other script
        if target == 'flights/breakdown':
            benchmark_script = 'runbreakdown.sh'

        cmd = 'bash -c "cd {} && bash {}"'.format(benchmark_path, benchmark_script)
        env = {'NUM_RUNS': num_runs,
               'PATH': '/opt/spark@2/bin:/bin:/opt/scala/bin:/root/.cargo/bin:/root/.cargo/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
               'SPARK_HOME': '/opt/spark@2',
               'SCALA_HOME': '/opt/scala'}

        logging.info('Starting benchmark using command: docker exec -i{}t {} {}'.format('d' if detach else '', DOCKER_CONTAINER_NAME,
                                                                                        cmd))
        exit_code, output = container.exec_run(cmd, stderr=True, stdout=True, detach=detach, environment=env)

        logging.info('Finished with code: {}'.format(exit_code))
        logging.info('Output:\n{}'.format(output.decode() if isinstance(output, bytes) else output))
        if detach:
            logging.info('Started command in detached mode, to stop container use "stop" command')
        logging.info('Done.')


def start_container():
    # docker client
    dc = docker.from_env()

    # check whether tuplex/benchmark image exists. If not run create-image script!
    logging.info('Checking whether tuplex/benchmark image exists locally...')
    found_tags = [img.tags for img in dc.images.list() if len(img.tags) > 0]
    found_tags = [tags[0] for tags in found_tags]

    logging.info('Found following images: {}'.format('\n'.join(found_tags)))

    # check whether tag is part of it
    found_image = False
    for tag in found_tags:
        if tag.startswith(DOCKER_IMAGE_TAG):
            found_image = True

    if found_image:
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
        logging.error('Did not find docker image {}, consider building it!'.format(DOCKER_IMAGE_TAG))
        raise Exception('Docker image not found')

@click.command()
def start():
    """start Viton VLDB'22 experimental container"""
    start_container()


@click.command()
def stop():
    """stop Viton VLDB'22 experimental container"""

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


@click.command()
@click.option('--cereal/--no-cereal', is_flag=True, default=False,
              help='whether to build tuplex and lambda using cereal as serialization library.')
def build(cereal):
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
    cmd = ['docker', 'exec', DOCKER_CONTAINER_NAME, '-e', CEREAL_FLAG, 'bash', BUILD_SCRIPT_PATH]

    #cmd = ['docker', 'exec', 'vldb22', 'ls', '/code/benchmarks/nextconf']
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1)
    for line in iter(p.stdout.readline, b''):
        logging.info(line.decode().strip())
    p.stdout.close()
    p.wait()

    logging.info('Build and installed Tuplex in docker container.')

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
