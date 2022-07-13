#!/usr/bin/env python3
# this is for running the incremental exception handling experiments.
# CLI to run experiments/reproduce easily
try:
    import os.path
    import click
    import logging
    import subprocess
    import docker
    import sys
    import os
    import tarfile
    import io
    import time
    import pathlib
except ModuleNotFoundError as e:
    logging.error("Module not found: {}".format(e))
    logging.info("Install missing modules via {} -m pip install -r requirements.txt".format(sys.executable))
    sys.exit(1)

# subtargets can be done via /
experiment_targets = ['all', 'zillow']

experiment_targets_description = {'all': 'a meta target to run all experiments',
                                  'zillow': 'runs the zillow experiment using different modes'}

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
DOCKER_CONTAINER_NAME = 'vldb22'

# hidden config file to check compatibility of settings
CONFIG_FILE = '.config.json'

BUILD_CACHE = 'build_cache'


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


def is_tool(name):
    """Check whether `name` is on PATH and marked as executable."""

    # from whichcraft import which
    from shutil import which

    return which(name) is not None

# zillow experiment (different configurations)
def run_zillow_experiment(container, local_result_dir, clear_cache):
    # this is the AWS setup

    # first check whether clearcache program exists...
    if not is_tool('clearcache'):
        logging.error("clearcache does not exist on host machine, skip clearing caches.")
        clear_cache = None

    INPUT_PATH = '/data/zillow_dirty@10G.csv'
    SCRATCH_DIR = '/data/scratch'

    # check that path in docker exists
    cmd = ['stat', INPUT_PATH]
    exit_code, output = container.exec_run(cmd, stderr=True, stdout=True)
    if 0 != exit_code or 'No such file or' in output.decode():
        logging.error("Did not find input path {} in container.".format(INPUT_PATH))
        sys.exit(1)

    # execute scripts now
    os.makedirs(local_result_dir, exist_ok=True)

    run = 1

    # basically benchmark.sh, but in docker
    # docker exec vldb22 python3.9 /code/benchmarks/incremental/runtuplex.py --path /data/zillow_dirty@10G.csv --output-path /data/scratch/plain
    cmd = ['python3.9', '/code/benchmarks/incremental/runtuplex.py', '--path', INPUT_PATH, '--output-path', os.path.join(SCRATCH_DIR, 'plain')]
    log_path = os.path.join(local_result_dir, 'tuplex-plain-out-of-order-ssd-{:02d}.txt'.format(run))
    if clear_cache is not None:
        logging.info('clearing caches...')
        subprocess.run(["clearcache"])
        logging.info('OS caches cleared.')

    # run within docker
    exit_code, output = container.exec_run(cmd, stderr=True, stdout=True)
    if 0 != exit_code:
        logging.error("failed to execute {}, code={}".format(' '.join(cmd), exit_code))
        log_path += '.failed'

    with open(log_path, 'w') as fp:
        fp.write(output.decode() if isinstance(output, bytes) else output)


    logging.info('zillow exp done!')

@click.command()
@click.argument('target', type=click.Choice(sorted(list(set(meta_targets + experiment_targets))), case_sensitive=False))
@click.option('--num-runs', type=int, default=1,  # 11,
              help='How many runs to run experiment with (default=11 for 10 runs + 1 warmup run')
@click.option('--detach/--no-detach', default=False, help='whether to launch command in detached mode (non-blocking)')
@click.option('--help/--no-help', default=False, help='display help about target')
@click.option('--clear-cache/--no-clear-cache', default=True, help='whether to call clear cache in between runs')
@click.pass_context
def run(ctx, target, num_runs, detach, help, clear_cache):
    """ run benchmarks for specific dataset. THIS MIGHT TAKE A WHILE! """

    logging.info('Running experiments for target {}'.format(target))
    targets_to_run = retrieve_targets_to_run(target)
    if len(targets_to_run) > 1:
        logging.info('requires running targets {}'.format(targets_to_run))

    if help:
        print('Detailed descriptions of experimental targets:\n')
        # display help and exit
        for target in targets_to_run:
            print('\t{}:\n\t{}'.format(target, '-' * (1 + len(target))))
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
        os.makedirs(local_result_dir, exist_ok=True)

        if target == 'zillow':
            run_zillow_experiment(container, local_result_dir, clear_cache)
            num_targets_run += 1
            continue

        # other targets...
        path_dict = {}
        if not target in path_dict.keys():
            logging.error('target {} not found, skip.'.format(target))
            continue

        # get script
        benchmark_path = path_dict[target]['wd']
        benchmark_script = path_dict[target]['script']

        cmd = 'bash -c "cd {} && bash {}"'.format(benchmark_path, benchmark_script)
        env = {'NUM_RUNS': num_runs,
               'PATH': '/opt/spark@2/bin:/bin:/opt/scala/bin:/root/.cargo/bin:/root/.cargo/bin:/usr/local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
               'SPARK_HOME': '/opt/spark@2',
               'SCALA_HOME': '/opt/scala'}
        logging.info('Starting benchmark using command: docker exec -i{}t {} {}'.format('d' if detach else '',
                                                                                        DOCKER_CONTAINER_NAME,
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
        num_targets_run += 1
    logging.info('Done ({}/{} targets).'.format(num_targets_run, len(targets_to_run)))


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

            REPO_PATH = os.path.join(os.getcwd(), 'tuplex')
            LOCAL_DATA_PATH = '/disk/data'
            logging.info('Mounting source code from {}'.format(REPO_PATH))
            logging.info('Mounting data from {}'.format(LOCAL_DATA_PATH))
            volumes = {REPO_PATH: {'bind': '/code', 'mode': 'rw'},
                       LOCAL_DATA_PATH: {'bind': '/data', 'mode': 'rw'}}

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
@click.option('--cereal/--no-cereal', is_flag=True, default=False,
              help='whether to build tuplex and lambda using cereal as serialization library.')
@click.pass_context
def build(ctx, cereal):
    """Downloads tuplex repo to tuplex, switches to correct branch and builds it using the sigmod21 experiment container."""

    GIT_REPO_URI = 'https://github.com/LeonhardFS/tuplex-public'
    GIT_BRANCH = 'origin/inc-exp'
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

    CEREAL_FLAG = 'BUILD_WITH_CEREAL={}'.format('ON' if cereal else 'OFF')
    if cereal:
        logging.info('Building with Cereal as serialization format')

    # build tuplex within docker container & install it there as well!
    # i.e. build command is: docker exec sigmod21 bash /code/benchmarks/sigmod21-reproducibility/build_scripts/build_tuplex.sh
    BUILD_SCRIPT_PATH = '/code/benchmarks/incremental/build_scripts/build_tuplex.sh'
    cmd = ['docker', 'exec', '-e', CEREAL_FLAG, DOCKER_CONTAINER_NAME, 'bash', BUILD_SCRIPT_PATH]

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1)
    for line in iter(p.stdout.readline, b''):
        logging.info(line.decode().strip())
    p.stdout.close()
    p.wait()

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
    bits, stat = container.get_archive(package_container_path)
    with open(package_path, 'wb') as fp:
        for chunk in bits:
            fp.write(chunk)
    logging.info('Transferred python package from docker to {} ({} bytes)'.format(package_path, stat['size']))

    # create combined archive & store it as package.tar.gz
    dest_path = os.path.join(storage_path, 'tuplex-package.tar.gz')
    os.remove(package_path)
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
@click.option('--cereal/--no-cereal', is_flag=True, default=False,
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
    cmd = ['python3.9', '/tmp/tuplex/setup.py', 'install']
    exit_code, output = container.exec_run(cmd)
    if exit_code != 0:
        logging.error(output.decode())
        sys.exit(exit_code)
    logging.info('Installed Tuplex')
    logging.info('Done.')


# customize help
@click.group()
@click.pass_context
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