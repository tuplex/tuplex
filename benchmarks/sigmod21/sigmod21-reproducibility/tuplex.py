#!/usr/bin/env python3

# CLI to run experiments/reproduce easily
import os.path

import click
import logging
import subprocess
import docker
import gdown

experiment_targets = ['all', 'zillow', 'flights', 'logs', '311',
                      'tpch', 'zillow/Z1', 'zillow/Z2', 'zillow/exceptions',
                      'tpch/Q06', 'tpch/Q19', 'flights/breakdown', 'flights/flights']

plot_targets = ['all', 'figure3', 'figure4', 'figure5',
                'figure6', 'figure7', 'figure8', 'figure9', 'figure10', 'table3']

# default paths
DEFAULT_RESULT_PATH = 'r5d.8xlarge'
DEFAULT_OUTPUT_PATH = 'plots'
DOCKER_IMAGE_TAG = 'tuplex/benchmark'
DOCKER_CONTAINER_NAME = 'sigmod21'


@click.group()
def commands():
    pass


@click.command()
@click.argument('target', type=click.Choice(experiment_targets, case_sensitive=False))
@click.option('--num-runs', type=int, default=11,
              help='How many runs to run experiment with (default=11 for 10 runs + 1 warmup run')
@click.option('--detach/--no-detach', default=False, help='whether to launch command in detached mode (non-blocking)')
def run(target, num_runs, detach):
    """ run benchmarks for specific dataset. THIS MIGHT TAKE A WHILE! """
    logging.info('Running experiments for target {}'.format(target))

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
        targets = [name.lower() for name in experiment_targets if name.lower() != 'all']
    else:
        targets = [target]

    for target in targets:
        # run individual targets
        # for these, the runbenchmark.sh scripts are used!
        # e.g., docker exec -e NUM_RUNS=1 sigmod21 bash -c 'cd /code/benchmarks/zillow/Z1/ && bash runbenchmark.sh'
        path_dict = {'zillow/z1': '/code/benchmarks/zillow/Z1/',
                     'zillow/z2': '/code/benchmarks/zillow/Z2/',
                     'zillow/exceptions': '/code/benchmarks/dirty_zillow/',
                     'logs': '/code/benchmarks/logs/',
                     '311': '/code/benchmarks/311/',
                     'tpch/q06': '/code/benchmarks/Q06/',
                     'tpch/q19': '/code/benchmarks/Q19/',
                     'flights/flights': '/code/benchmarks/flights/',
                     'flights/breakdown': '/code/benchmarks/flights/', }

        benchmark_path = path_dict[target]
        benchmark_script = 'runbenchmark.sh'

        # only for flights/breakdown other script
        if target == 'flights/breakdown':
            benchmark_script = 'runbreakdown.sh'

        cmd = 'bash -c "cd {} && bash {}"'.format(benchmark_path, benchmark_script)
        env = {'NUM_RUNS': num_runs}

        logging.info('Starting benchmark using command: docker exec -i{}t {} {}'.format('d' if detach else '', DOCKER_CONTAINER_NAME,
                                                                                        cmd))
        exit_code, output = container.exec_run(cmd, stderr=True, stdout=True, detach=detach, environment=env)

        logging.info('Finished with code: {}'.format(exit_code))
        logging.info('Output:\n{}'.format(output.decode() if isinstance(output, bytes) else output))
        if detach:
            logging.info('Started command in detached mode, to stop container use "stop" command')
        logging.info('Done.')


# plot helpers
def plot_table3(zillow_path='r5d.8xlarge/zillow', output_folder='plots'):
    logging.info('Plotting Figure3 (Zillow experiment)')
    logging.info('Benchmark result folder specified as {}'.format(zillow_path))
    from plot_scripts.zillow_plots import table3, load_data

    logging.info('Loading data...')
    df_Z1, df_Z2 = load_data(zillow_path)
    table3(df_Z1)
    logging.info('Table shown.')


def plot_figure3(zillow_path='r5d.8xlarge/zillow', output_folder='plots'):
    logging.info('Plotting Figure3 (Zillow experiment)')
    logging.info('Benchmark result folder specified as {}'.format(zillow_path))
    from plot_scripts.zillow_plots import figure3, load_data

    logging.info('Loading data...')
    df_Z1, df_Z2 = load_data(zillow_path)
    logging.info('Plotting Z1/Z2 (Figure3)')
    figure3(df_Z1, df_Z2, output_folder)
    logging.info('Plots saved in {}'.format(output_folder))


def plot_figure4(flights_path='r5d.8xlarge/flights', output_folder='plots'):
    logging.info('Plotting Figure4 (Flights experiment)')
    logging.info('Benchmark result folder specified as {}'.format(flights_path))
    from plot_scripts.figure4 import figure4

    logging.info('Loading data...')
    figure4(flights_path, output_folder)
    logging.info('Plots saved in {}'.format(output_folder))


def plot_figure5(logs_path='r5d.8xlarge/logs', output_folder='plots'):
    logging.info('Plotting Figure5 (logs experiment)')
    logging.warning('DO NOT SHARE DATA')
    logging.info('Benchmark result folder specified as {}'.format(logs_path))
    from plot_scripts.figure5 import figure5

    logging.info('Loading data...')
    figure5(logs_path, output_folder)
    logging.info('Plots saved in {}'.format(output_folder))


def plot_figure6(zillow_path='r5d.8xlarge/zillow', output_folder='plots'):
    logging.info('Plotting Figure6 (Tuplex exceptions)')
    logging.info('Benchmark result folder specified as {}'.format(zillow_path))
    from plot_scripts.figure6 import figure6
    figure6(zillow_path, output_folder)
    logging.info('Plots saved in {}'.format(output_folder))


def plot_figure7(zillow_path='r5d.8xlarge/zillow', output_folder='plots'):
    logging.info('Plotting Figure7 (Tuplex vs. other JITs experiment)')
    logging.info('Benchmark result folder specified as {}'.format(zillow_path))
    from plot_scripts.zillow_plots import figure7, load_data

    logging.info('Loading data...')
    df_Z1, df_Z2 = load_data(zillow_path)
    logging.info('Plotting Z1 (Figure7)')
    figure7(df_Z1, output_folder)
    logging.info('Plots saved in {}'.format(output_folder))


def plot_figure8(service_path='r5d.8xlarge/311', output_folder='plots'):
    logging.info('Plotting Figure8 (311 experiment/agrgegates)')
    logging.info('Benchmark result folder specified as {}'.format(service_path))
    from plot_scripts.figure8 import figure8

    logging.info('Loading data...')
    figure8(service_path, output_folder)
    logging.info('Plots saved in {}'.format(output_folder))


def plot_figure9(tpch_path='r5d.8xlarge/tpch', output_folder='plots'):
    logging.info('Plotting Figure9 (TPCH Q6/Q19)')
    logging.info('Benchmark result folder specified as {}'.format(tpch_path))
    from plot_scripts.figure9 import figure9

    logging.info('Loading data...')
    figure9(tpch_path, output_folder)
    logging.info('Plots saved in {}'.format(output_folder))


def plot_figure10(flights_path='r5d.8xlarge/flights', output_folder='plots'):
    logging.info('Plotting Figure10 (Flights breakdown')
    logging.info('Benchmark result folder specified as {}'.format(flights_path))
    from plot_scripts.figure10 import figure10

    logging.info('Loading data...')
    figure10(flights_path, output_folder)
    logging.info('Plots saved in {}'.format(output_folder))


# end plot helpers


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
            if not os.path.isdir('/disk'):
                raise Exception('Could not find /disk directory, is machine properly setup?')
            # create dirs
            os.makedirs('/disk/data', exist_ok=True)
            os.makedirs('/disk/benchmark_results', exist_ok=True)
            if not os.path.isdir('/disk/tuplex-public'):
                raise Exception('Could not find tuplex repo at directory /disk/tuplex-public. Please check out first!')

            volumes = {'/disk/data/': {'bind': '/data', 'mode': 'rw'},
                       '/disk/benchmark_results': {'bind': '/results', 'mode': 'rw'},
                       '/disk/tuplex-public': {'bind': '/code', 'mode': 'rw'}}

            dc.containers.run(DOCKER_IMAGE_TAG + ':latest', name=DOCKER_CONTAINER_NAME,
                              tty=True, stdin_open=True, detach=True, volumes=volumes, remove=True)
            logging.info('Started docker container {} from image {}'.format(DOCKER_CONTAINER_NAME, DOCKER_IMAGE_TAG))
    else:
        logging.error('Did not find docker image {}, consider building it!'.format(DOCKER_IMAGE_TAG))
        raise Exception('Docker image not found')

@click.command()
def start():
    """start Tuplex SIGMOD21 experimental container"""
    start_container()


@click.command()
def stop():
    """stop Tuplex SIGMOD21 experimental container"""

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
    """Plot all or individual figures from the Tuplex paper. Use --help to retrieve more information on this command. """
    # check if input path is r5d.8xlarge and it's not existing -> unpack tar!
    if input_path == DEFAULT_RESULT_PATH and not os.path.isdir(input_path):
        if os.path.isfile(input_path):
            raise Exception('fatal error, conflicting file {} found! Remove file...'.format(DEFAULT_RESULT_PATH))

        import tarfile

        # extract tar file
        logging.info('Extracting experiment data...')
        with tarfile.open(DEFAULT_RESULT_PATH + '.tar.gz') as tf:
            tf.extractall()
        logging.info('Extraction done!')

    os.makedirs(output_path, exist_ok=True)

    DEFAULT_ZILLOW_PATH = input_path + '/zillow'
    DEFAULT_FLIGHTS_PATH = input_path + '/flights'
    DEFAULT_LOGS_PATH = input_path + '/logs'
    DEFAULT_311_PATH = input_path + '/311'
    DEFAULT_TPCH_PATH = input_path + '/tpch'

    PLOT_ALL = False
    if 'all' == target.lower():
        PLOT_ALL = True

    logging.info('Plotting output for target {}'.format(target))

    # go through list and plot whatever was selected
    if 'table3' == target.lower() or PLOT_ALL:
        plot_table3(DEFAULT_ZILLOW_PATH, output_path)
    if 'figure3' == target.lower() or PLOT_ALL:
        plot_figure3(DEFAULT_ZILLOW_PATH, output_path)
    if 'figure4' == target.lower() or PLOT_ALL:
        plot_figure4(DEFAULT_FLIGHTS_PATH, output_path)
    if 'figure5' == target.lower() or PLOT_ALL:
        plot_figure5(DEFAULT_LOGS_PATH, output_path)
    if 'figure6' == target.lower() or PLOT_ALL:
        plot_figure6(DEFAULT_ZILLOW_PATH, output_path)
    if 'figure7' == target.lower() or PLOT_ALL:
        plot_figure7(DEFAULT_ZILLOW_PATH, output_path)
    if 'figure8' == target.lower() or PLOT_ALL:
        plot_figure8(DEFAULT_311_PATH, output_path)
    if 'figure9' == target.lower() or PLOT_ALL:
        plot_figure9(DEFAULT_TPCH_PATH, output_path)
    if 'figure10' == target.lower() or PLOT_ALL:
        plot_figure10(DEFAULT_FLIGHTS_PATH, output_path)
    logging.info('Plotting done.')


@click.command()
def build():
    """Downloads tuplex repo to tuplex, switches to correct branch and builds it using the sigmod21 experiment container."""

    GIT_REPO_URI = 'https://github.com/LeonhardFS/tuplex-public'
    GIT_BRANCH = 'origin/sigmod-repro'
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

    start_container()

    # build tuplex within docker container & install it there as well!
    # i.e. build command is: docker exec sigmod21 bash /code/benchmarks/sigmod21-reproducibility/build_scripts/build_tuplex.sh
    BUILD_SCRIPT_PATH = '/code/benchmarks/sigmod21-reproducibility/build_scripts/build_tuplex.sh'
    cmd = ['docker', 'exec', 'sigmod21', 'bash', BUILD_SCRIPT_PATH]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1)
    for line in iter(p.stdout.readline, b''):
        logging.info(line.decode().strip())
    p.stdout.close()
    p.wait()

    logging.info('Build and installed Tuplex in docker container.')


@click.command()
@click.option('--target', type=str, default='/disk',
              help='path where to save data to, default={}'.format('/disk'))
@click.option('--password', type=str, default='',
              help='Specify password to unpack as well')
def download(target, password):
    """downloads sigmod21 data to target path and extracts if password is specified"""

    gdrive_md5 = '1358ffed089704f7a3e587680c1299ee'
    gdrive_link = 'https://drive.google.com/uc?id=1chJncLpuSOPUvlWwODg_a7A-sEbEORL1'
    target_path = os.path.join(target, 'sigmod21.7z')

    logging.info('Downloading data from Google Drive to {}'.format(target_path))
    gdown.cached_download(gdrive_link, target_path, md5=gdrive_md5, quiet=False)

    if '' == password:
        logging.info('no password specified, extract archive manually via \n7z x {}'.format(target_path))
    else:
        logging.info('extracting data... (this might take a while, ~180G to write)')

        cmd = ['7z', 'x', 'sigmod21.7z', '-aoa', '-p{}'.format(password)]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1, cwd=target)
        for line in iter(p.stdout.readline, b''):
            logging.info(line.decode().strip())
        p.stdout.close()
        p.wait()

        logging.info('done.')

commands.add_command(run)
commands.add_command(plot)
commands.add_command(build)
commands.add_command(start)
commands.add_command(stop)
commands.add_command(download)

# scripts to run experiments:
# 1. Zillow
# zillow/Z1/runbenchmark.sh
# zillow/Z2/runbenchmark.sh
# 2. Flights
# flights/runbenchmark.sh
# flights/runbreakdown.sh
# 3. Logs
# logs/benchmark.sh --> check? rename?
# 4. tpch
# tpch/Q06/runbenchmark.sh
# tpch/Q19/runbenchmark.sh
# 5. 311
# 311/runbenchmark.sh


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s:%(levelname)s: %(message)s',
                        handlers=[logging.FileHandler("experiment.log", mode='w'),
                                  logging.StreamHandler()])
    stream_handler = [h for h in logging.root.handlers if isinstance(h, logging.StreamHandler)][0]
    stream_handler.setLevel(logging.INFO)

    commands()
