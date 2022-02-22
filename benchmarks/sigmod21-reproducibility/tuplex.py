#!/usr/bin/env python3

# CLI to run experiments/reproduce easily
import os.path

import click
import logging
import subprocess
import docker
import gdown
import sys

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

    # check that all paths exist in docker container
    required_paths_in_docker = '''/data/311/311-service-requests-raw.csv
/data/311/311_2010_to_2020.csv
/data/311/311_preprocessed.csv
/data/311/actual_311_2010_to_2020_nyc.csv
/data/flights/GlobalAirportDatabase.txt
/data/flights/L_AIRPORT_ID.csv
/data/flights/L_CARRIER_HISTORY.csv
/data/flights/flights_on_time_performance_2009_01.csv
/data/flights/flights_on_time_performance_2009_02.csv
/data/flights/flights_on_time_performance_2009_03.csv
/data/flights/flights_on_time_performance_2009_04.csv
/data/flights/flights_on_time_performance_2009_05.csv
/data/flights/flights_on_time_performance_2009_06.csv
/data/flights/flights_on_time_performance_2009_07.csv
/data/flights/flights_on_time_performance_2009_08.csv
/data/flights/flights_on_time_performance_2009_09.csv
/data/flights/flights_on_time_performance_2009_10.csv
/data/flights/flights_on_time_performance_2009_11.csv
/data/flights/flights_on_time_performance_2009_12.csv
/data/flights/flights_on_time_performance_2010_01.csv
/data/flights/flights_on_time_performance_2010_02.csv
/data/flights/flights_on_time_performance_2010_03.csv
/data/flights/flights_on_time_performance_2010_04.csv
/data/flights/flights_on_time_performance_2010_05.csv
/data/flights/flights_on_time_performance_2010_06.csv
/data/flights/flights_on_time_performance_2010_07.csv
/data/flights/flights_on_time_performance_2010_08.csv
/data/flights/flights_on_time_performance_2010_09.csv
/data/flights/flights_on_time_performance_2010_10.csv
/data/flights/flights_on_time_performance_2010_11.csv
/data/flights/flights_on_time_performance_2010_12.csv
/data/flights/flights_on_time_performance_2011_01.csv
/data/flights/flights_on_time_performance_2011_02.csv
/data/flights/flights_on_time_performance_2011_03.csv
/data/flights/flights_on_time_performance_2011_04.csv
/data/flights/flights_on_time_performance_2011_05.csv
/data/flights/flights_on_time_performance_2011_06.csv
/data/flights/flights_on_time_performance_2011_07.csv
/data/flights/flights_on_time_performance_2011_08.csv
/data/flights/flights_on_time_performance_2011_09.csv
/data/flights/flights_on_time_performance_2011_10.csv
/data/flights/flights_on_time_performance_2011_11.csv
/data/flights/flights_on_time_performance_2011_12.csv
/data/flights/flights_on_time_performance_2012_01.csv
/data/flights/flights_on_time_performance_2012_02.csv
/data/flights/flights_on_time_performance_2012_03.csv
/data/flights/flights_on_time_performance_2012_04.csv
/data/flights/flights_on_time_performance_2012_05.csv
/data/flights/flights_on_time_performance_2012_06.csv
/data/flights/flights_on_time_performance_2012_07.csv
/data/flights/flights_on_time_performance_2012_08.csv
/data/flights/flights_on_time_performance_2012_09.csv
/data/flights/flights_on_time_performance_2012_10.csv
/data/flights/flights_on_time_performance_2012_11.csv
/data/flights/flights_on_time_performance_2012_12.csv
/data/flights/flights_on_time_performance_2013_01.csv
/data/flights/flights_on_time_performance_2013_02.csv
/data/flights/flights_on_time_performance_2013_03.csv
/data/flights/flights_on_time_performance_2013_04.csv
/data/flights/flights_on_time_performance_2013_05.csv
/data/flights/flights_on_time_performance_2013_06.csv
/data/flights/flights_on_time_performance_2013_07.csv
/data/flights/flights_on_time_performance_2013_08.csv
/data/flights/flights_on_time_performance_2013_09.csv
/data/flights/flights_on_time_performance_2013_10.csv
/data/flights/flights_on_time_performance_2013_11.csv
/data/flights/flights_on_time_performance_2013_12.csv
/data/flights/flights_on_time_performance_2014_01.csv
/data/flights/flights_on_time_performance_2014_02.csv
/data/flights/flights_on_time_performance_2014_03.csv
/data/flights/flights_on_time_performance_2014_04.csv
/data/flights/flights_on_time_performance_2014_05.csv
/data/flights/flights_on_time_performance_2014_06.csv
/data/flights/flights_on_time_performance_2014_07.csv
/data/flights/flights_on_time_performance_2014_08.csv
/data/flights/flights_on_time_performance_2014_09.csv
/data/flights/flights_on_time_performance_2014_10.csv
/data/flights/flights_on_time_performance_2014_11.csv
/data/flights/flights_on_time_performance_2014_12.csv
/data/flights/flights_on_time_performance_2015_01.csv
/data/flights/flights_on_time_performance_2015_02.csv
/data/flights/flights_on_time_performance_2015_03.csv
/data/flights/flights_on_time_performance_2015_04.csv
/data/flights/flights_on_time_performance_2015_05.csv
/data/flights/flights_on_time_performance_2015_06.csv
/data/flights/flights_on_time_performance_2015_07.csv
/data/flights/flights_on_time_performance_2015_08.csv
/data/flights/flights_on_time_performance_2015_09.csv
/data/flights/flights_on_time_performance_2015_10.csv
/data/flights/flights_on_time_performance_2015_11.csv
/data/flights/flights_on_time_performance_2015_12.csv
/data/flights/flights_on_time_performance_2016_01.csv
/data/flights/flights_on_time_performance_2016_02.csv
/data/flights/flights_on_time_performance_2016_03.csv
/data/flights/flights_on_time_performance_2016_04.csv
/data/flights/flights_on_time_performance_2016_05.csv
/data/flights/flights_on_time_performance_2016_06.csv
/data/flights/flights_on_time_performance_2016_07.csv
/data/flights/flights_on_time_performance_2016_08.csv
/data/flights/flights_on_time_performance_2016_09.csv
/data/flights/flights_on_time_performance_2016_10.csv
/data/flights/flights_on_time_performance_2016_11.csv
/data/flights/flights_on_time_performance_2016_12.csv
/data/flights/flights_on_time_performance_2017_01.csv
/data/flights/flights_on_time_performance_2017_02.csv
/data/flights/flights_on_time_performance_2017_03.csv
/data/flights/flights_on_time_performance_2017_04.csv
/data/flights/flights_on_time_performance_2017_05.csv
/data/flights/flights_on_time_performance_2017_06.csv
/data/flights/flights_on_time_performance_2017_07.csv
/data/flights/flights_on_time_performance_2017_08.csv
/data/flights/flights_on_time_performance_2017_09.csv
/data/flights/flights_on_time_performance_2017_10.csv
/data/flights/flights_on_time_performance_2017_11.csv
/data/flights/flights_on_time_performance_2017_12.csv
/data/flights/flights_on_time_performance_2018_01.csv
/data/flights/flights_on_time_performance_2018_02.csv
/data/flights/flights_on_time_performance_2018_03.csv
/data/flights/flights_on_time_performance_2018_04.csv
/data/flights/flights_on_time_performance_2018_05.csv
/data/flights/flights_on_time_performance_2018_06.csv
/data/flights/flights_on_time_performance_2018_07.csv
/data/flights/flights_on_time_performance_2018_08.csv
/data/flights/flights_on_time_performance_2018_09.csv
/data/flights/flights_on_time_performance_2018_10.csv
/data/flights/flights_on_time_performance_2018_11.csv
/data/flights/flights_on_time_performance_2018_12.csv
/data/flights/flights_on_time_performance_2019_01.csv
/data/flights/flights_on_time_performance_2019_02.csv
/data/flights/flights_on_time_performance_2019_03.csv
/data/flights/flights_on_time_performance_2019_04.csv
/data/flights/flights_on_time_performance_2019_05.csv
/data/flights/flights_on_time_performance_2019_06.csv
/data/flights/flights_on_time_performance_2019_07.csv
/data/flights/flights_on_time_performance_2019_08.csv
/data/flights/flights_on_time_performance_2019_09.csv
/data/flights/flights_on_time_performance_2019_10.csv
/data/flights/flights_on_time_performance_2019_11.csv
/data/flights/flights_on_time_performance_2019_12.csv
/data/flights_small/GlobalAirportDatabase.txt
/data/flights_small/L_AIRPORT_ID.csv
/data/flights_small/L_CARRIER_HISTORY.csv
/data/flights_small/flights_on_time_performance_2018_01.csv
/data/flights_small/flights_on_time_performance_2018_02.csv
/data/flights_small/flights_on_time_performance_2018_03.csv
/data/flights_small/flights_on_time_performance_2018_04.csv
/data/flights_small/flights_on_time_performance_2018_05.csv
/data/flights_small/flights_on_time_performance_2018_06.csv
/data/flights_small/flights_on_time_performance_2018_07.csv
/data/flights_small/flights_on_time_performance_2018_08.csv
/data/flights_small/flights_on_time_performance_2018_09.csv
/data/flights_small/flights_on_time_performance_2018_10.csv
/data/flights_small/flights_on_time_performance_2018_11.csv
/data/flights_small/flights_on_time_performance_2018_12.csv
/data/flights_small/flights_on_time_performance_2019_01.csv
/data/flights_small/flights_on_time_performance_2019_02.csv
/data/flights_small/flights_on_time_performance_2019_03.csv
/data/flights_small/flights_on_time_performance_2019_04.csv
/data/flights_small/flights_on_time_performance_2019_05.csv
/data/flights_small/flights_on_time_performance_2019_06.csv
/data/flights_small/flights_on_time_performance_2019_07.csv
/data/flights_small/flights_on_time_performance_2019_08.csv
/data/flights_small/flights_on_time_performance_2019_09.csv
/data/flights_small/flights_on_time_performance_2019_10.csv
/data/flights_small/flights_on_time_performance_2019_11.csv
/data/logs/2000.01.01.txt
/data/logs/2000.01.02.txt
/data/logs/2000.01.03.txt
/data/logs/2000.01.04.txt
/data/logs/2000.01.05.txt
/data/logs/2000.01.06.txt
/data/logs/2000.01.07.txt
/data/logs/2000.01.08.txt
/data/logs/2000.01.09.txt
/data/logs/2000.01.10.txt
/data/logs/2000.01.11.txt
/data/logs/2000.01.12.txt
/data/logs/2000.01.13.txt
/data/logs/2000.01.14.txt
/data/logs/2000.01.15.txt
/data/logs/2000.01.16.txt
/data/logs/2000.01.17.txt
/data/logs/2000.01.18.txt
/data/logs/2000.01.19.txt
/data/logs/2000.01.20.txt
/data/logs/2000.01.21.txt
/data/logs/2000.01.22.txt
/data/logs/2000.01.23.txt
/data/logs/2000.01.24.txt
/data/logs/2000.01.25.txt
/data/logs/2000.01.26.txt
/data/logs/2000.01.27.txt
/data/logs/2000.01.28.txt
/data/logs/2000.01.29.txt
/data/logs/2000.01.30.txt
/data/logs/2000.01.31.txt
/data/logs/2000.02.01.txt
/data/logs/2000.02.02.txt
/data/logs/2000.02.03.txt
/data/logs/2000.02.04.txt
/data/logs/2000.02.05.txt
/data/logs/2000.02.06.txt
/data/logs/2000.02.07.txt
/data/logs/2000.02.08.txt
/data/logs/2000.02.09.txt
/data/logs/2000.02.10.txt
/data/logs/2000.02.11.txt
/data/logs/2000.02.12.txt
/data/logs/2000.02.13.txt
/data/logs/2000.02.14.txt
/data/logs/2000.02.15.txt
/data/logs/2000.02.16.txt
/data/logs/2000.02.17.txt
/data/logs/2000.02.18.txt
/data/logs/2000.02.19.txt
/data/logs/2000.02.20.txt
/data/logs/2000.02.21.txt
/data/logs/2000.02.22.txt
/data/logs/2000.02.23.txt
/data/logs/2000.02.24.txt
/data/logs/2000.02.25.txt
/data/logs/2000.02.26.txt
/data/logs/2000.02.27.txt
/data/logs/2000.02.28.txt
/data/logs/2000.02.29.txt
/data/logs/2000.03.01.txt
/data/logs/2000.03.02.txt
/data/logs/2000.03.03.txt
/data/logs/2000.03.04.txt
/data/logs/2000.03.05.txt
/data/logs/2000.03.06.txt
/data/logs/2000.03.07.txt
/data/logs/2000.03.08.txt
/data/logs/2000.03.09.txt
/data/logs/2000.03.10.txt
/data/logs/2000.03.11.txt
/data/logs/2000.03.12.txt
/data/logs/2000.03.13.txt
/data/logs/2000.03.14.txt
/data/logs/2000.03.15.txt
/data/logs/2000.03.16.txt
/data/logs/2000.03.17.txt
/data/logs/2000.03.18.txt
/data/logs/2000.03.19.txt
/data/logs/2000.03.20.txt
/data/logs/2000.03.21.txt
/data/logs/2000.03.22.txt
/data/logs/2000.03.23.txt
/data/logs/2000.03.24.txt
/data/logs/2000.03.25.txt
/data/logs/2000.03.26.txt
/data/logs/2000.03.27.txt
/data/logs/2000.03.28.txt
/data/logs/2000.03.29.txt
/data/logs/2000.03.30.txt
/data/logs/2000.03.31.txt
/data/logs/2000.04.01.txt
/data/logs/2000.04.02.txt
/data/logs/2000.04.03.txt
/data/logs/2000.04.04.txt
/data/logs/2000.04.05.txt
/data/logs/2000.04.06.txt
/data/logs/2000.04.07.txt
/data/logs/2000.04.08.txt
/data/logs/2000.04.09.txt
/data/logs/2000.04.10.txt
/data/logs/2000.04.11.txt
/data/logs/2000.04.12.txt
/data/logs/2000.04.13.txt
/data/logs/2000.04.14.txt
/data/logs/2000.04.15.txt
/data/logs/2000.04.16.txt
/data/logs/2000.04.17.txt
/data/logs/2000.04.18.txt
/data/logs/2000.04.19.txt
/data/logs/2000.04.20.txt
/data/logs/2000.04.21.txt
/data/logs/2000.04.23.txt
/data/logs/2000.04.24.txt
/data/logs/2000.04.25.txt
/data/logs/2000.04.26.txt
/data/logs/2000.04.27.txt
/data/logs/2000.04.28.txt
/data/logs/2000.04.29.txt
/data/logs/2000.04.30.txt
/data/logs/2000.05.01.txt
/data/logs/2000.05.02.txt
/data/logs/2000.05.03.txt
/data/logs/2000.05.04.txt
/data/logs/2000.05.05.txt
/data/logs/2000.05.06.txt
/data/logs/2000.05.07.txt
/data/logs/2000.05.08.txt
/data/logs/2000.05.09.txt
/data/logs/2000.05.10.txt
/data/logs/2000.05.11.txt
/data/logs/2000.05.12.txt
/data/logs/2000.05.13.txt
/data/logs/2000.05.14.txt
/data/logs/2000.05.15.txt
/data/logs/2000.05.16.txt
/data/logs/2000.05.17.txt
/data/logs/2000.05.18.txt
/data/logs/2000.05.19.txt
/data/logs/2000.05.20.txt
/data/logs/2000.05.21.txt
/data/logs/2000.05.22.txt
/data/logs/2000.05.23.txt
/data/logs/2000.05.24.txt
/data/logs/2000.05.25.txt
/data/logs/2000.05.26.txt
/data/logs/2000.05.27.txt
/data/logs/2000.05.28.txt
/data/logs/2000.05.29.txt
/data/logs/2000.05.30.txt
/data/logs/2000.05.31.txt
/data/logs/2000.06.01.txt
/data/logs/2000.06.02.txt
/data/logs/2000.06.03.txt
/data/logs/2000.06.04.txt
/data/logs/2000.06.05.txt
/data/logs/2000.06.06.txt
/data/logs/2000.06.07.txt
/data/logs/2000.06.08.txt
/data/logs/2000.06.09.txt
/data/logs/2000.06.10.txt
/data/logs/2000.06.11.txt
/data/logs/2000.06.12.txt
/data/logs/2000.06.13.txt
/data/logs/2000.06.14.txt
/data/logs/2000.06.15.txt
/data/logs/2000.06.16.txt
/data/logs/2000.06.17.txt
/data/logs/2000.06.18.txt
/data/logs/2000.06.19.txt
/data/logs/2000.06.20.txt
/data/logs/2000.06.21.txt
/data/logs/2000.06.22.txt
/data/logs/2000.06.23.txt
/data/logs/2000.06.24.txt
/data/logs/2000.06.25.txt
/data/logs/2000.06.26.txt
/data/logs/2000.06.27.txt
/data/logs/2000.06.28.txt
/data/logs/2000.06.29.txt
/data/logs/2000.06.30.txt
/data/logs/2000.07.01.txt
/data/logs/2000.07.02.txt
/data/logs/2000.07.03.txt
/data/logs/2000.07.04.txt
/data/logs/2000.07.05.txt
/data/logs/2000.07.06.txt
/data/logs/2000.07.07.txt
/data/logs/2000.07.08.txt
/data/logs/2000.07.09.txt
/data/logs/2000.07.10.txt
/data/logs/2000.07.11.txt
/data/logs/2000.07.12.txt
/data/logs/2000.07.13.txt
/data/logs/2000.07.14.txt
/data/logs/2000.07.15.txt
/data/logs/2000.07.16.txt
/data/logs/2000.07.17.txt
/data/logs/2000.07.18.txt
/data/logs/2000.07.19.txt
/data/logs/2000.07.20.txt
/data/logs/2000.07.21.txt
/data/logs/2000.07.22.txt
/data/logs/2000.07.23.txt
/data/logs/2000.07.24.txt
/data/logs/2000.07.25.txt
/data/logs/2000.07.26.txt
/data/logs/2000.07.27.txt
/data/logs/2000.07.28.txt
/data/logs/2000.07.29.txt
/data/logs/2000.07.30.txt
/data/logs/2000.07.31.txt
/data/logs/2000.08.01.txt
/data/logs/2000.08.02.txt
/data/logs/2000.08.03.txt
/data/logs/2000.08.04.txt
/data/logs/2000.08.05.txt
/data/logs/2000.08.06.txt
/data/logs/2000.08.07.txt
/data/logs/2000.08.08.txt
/data/logs/2000.08.09.txt
/data/logs/2000.08.10.txt
/data/logs/2000.08.11.txt
/data/logs/2000.08.12.txt
/data/logs/2000.08.13.txt
/data/logs/2000.08.14.txt
/data/logs/2000.08.15.txt
/data/logs/2000.08.16.txt
/data/logs/2000.08.17.txt
/data/logs/2000.08.18.txt
/data/logs/2000.08.19.txt
/data/logs/2000.08.20.txt
/data/logs/2000.08.21.txt
/data/logs/2000.08.22.txt
/data/logs/2000.08.23.txt
/data/logs/2000.08.24.txt
/data/logs/2000.08.25.txt
/data/logs/2000.08.26.txt
/data/logs/2000.08.27.txt
/data/logs/2000.08.28.txt
/data/logs/2000.08.29.txt
/data/logs/2000.08.30.txt
/data/logs/2000.08.31.txt
/data/logs/2000.09.01.txt
/data/logs/2000.09.02.txt
/data/logs/2000.09.03.txt
/data/logs/2000.09.04.txt
/data/logs/2000.09.05.txt
/data/logs/2000.09.06.txt
/data/logs/2000.09.07.txt
/data/logs/2000.09.08.txt
/data/logs/2000.09.09.txt
/data/logs/2000.09.10.txt
/data/logs/2000.09.11.txt
/data/logs/2000.09.12.txt
/data/logs/2000.09.13.txt
/data/logs/2000.09.14.txt
/data/logs/2000.09.15.txt
/data/logs/2000.09.16.txt
/data/logs/2000.09.17.txt
/data/logs/2000.09.18.txt
/data/logs/2000.09.19.txt
/data/logs/2000.09.20.txt
/data/logs/2000.09.21.txt
/data/logs/2000.09.22.txt
/data/logs/2000.09.23.txt
/data/logs/2000.09.24.txt
/data/logs/2000.09.25.txt
/data/logs/2000.09.26.txt
/data/logs/2000.09.27.txt
/data/logs/2000.09.28.txt
/data/logs/2000.09.29.txt
/data/logs/2000.09.30.txt
/data/logs/2000.10.01.txt
/data/logs/2000.10.02.txt
/data/logs/2000.10.03.txt
/data/logs/2000.10.04.txt
/data/logs/2000.10.05.txt
/data/logs/2000.10.06.txt
/data/logs/2000.10.07.txt
/data/logs/2000.10.08.txt
/data/logs/2000.10.09.txt
/data/logs/2000.10.10.txt
/data/logs/2000.10.11.txt
/data/logs/2000.10.12.txt
/data/logs/2000.10.13.txt
/data/logs/2000.10.14.txt
/data/logs/2000.10.15.txt
/data/logs/2000.10.16.txt
/data/logs/2000.10.17.txt
/data/logs/2000.10.18.txt
/data/logs/2000.10.19.txt
/data/logs/2000.10.20.txt
/data/logs/2000.10.21.txt
/data/logs/2000.10.22.txt
/data/logs/2000.10.23.txt
/data/logs/2000.10.24.txt
/data/logs/2000.10.25.txt
/data/logs/2000.10.26.txt
/data/logs/2000.10.27.txt
/data/logs/2000.10.28.txt
/data/logs/2000.10.29.txt
/data/logs/2000.10.30.txt
/data/logs/2000.10.31.txt
/data/logs/2000.11.01.txt
/data/logs/2000.11.02.txt
/data/logs/2000.11.03.txt
/data/logs/2000.11.04.txt
/data/logs/2000.11.05.txt
/data/logs/2000.11.06.txt
/data/logs/2000.11.07.txt
/data/logs/2000.11.08.txt
/data/logs/2000.11.09.txt
/data/logs/2000.11.10.txt
/data/logs/2000.11.11.txt
/data/logs/2000.11.12.txt
/data/logs/2000.11.13.txt
/data/logs/2000.11.14.txt
/data/logs/2000.11.15.txt
/data/logs/2000.11.16.txt
/data/logs/2000.11.17.txt
/data/logs/2000.11.18.txt
/data/logs/2000.11.19.txt
/data/logs/2000.11.20.txt
/data/logs/2000.11.21.txt
/data/logs/2000.11.22.txt
/data/logs/2000.11.23.txt
/data/logs/2000.11.24.txt
/data/logs/2000.11.25.txt
/data/logs/2000.11.26.txt
/data/logs/2000.11.27.txt
/data/logs/2000.11.28.txt
/data/logs/2000.11.29.txt
/data/logs/2000.11.30.txt
/data/logs/2000.12.01.txt
/data/logs/2000.12.02.txt
/data/logs/2000.12.03.txt
/data/logs/2000.12.04.txt
/data/logs/2000.12.05.txt
/data/logs/2000.12.06.txt
/data/logs/2000.12.07.txt
/data/logs/2000.12.08.txt
/data/logs/2000.12.09.txt
/data/logs/2000.12.10.txt
/data/logs/2000.12.11.txt
/data/logs/2000.12.12.txt
/data/logs/2000.12.13.txt
/data/logs/2000.12.14.txt
/data/logs/2000.12.15.txt
/data/logs/2000.12.16.txt
/data/logs/2000.12.17.txt
/data/logs/2000.12.18.txt
/data/logs/2000.12.19.txt
/data/logs/2000.12.20.txt
/data/logs/2000.12.21.txt
/data/logs/2000.12.22.txt
/data/logs/2000.12.23.txt
/data/logs/2000.12.24.txt
/data/logs/2000.12.25.txt
/data/logs/2000.12.27.txt
/data/logs/2000.12.28.txt
/data/logs/2000.12.29.txt
/data/logs/2000.12.30.txt
/data/logs/2000.12.31.txt
/data/logs/2001.01.01.txt
/data/logs/2001.01.02.txt
/data/logs/2001.01.03.txt
/data/logs/2001.01.04.txt
/data/logs/2001.01.05.txt
/data/logs/2001.01.06.txt
/data/logs/2001.01.07.txt
/data/logs/2001.01.08.txt
/data/logs/2001.01.09.txt
/data/logs/2001.01.10.txt
/data/logs/2001.01.11.txt
/data/logs/2001.01.12.txt
/data/logs/2001.01.13.txt
/data/logs/2001.01.14.txt
/data/logs/2001.01.15.txt
/data/logs/2001.01.16.txt
/data/logs/2001.01.17.txt
/data/logs/2001.01.18.txt
/data/logs/2001.01.19.txt
/data/logs/2001.01.20.txt
/data/logs/2001.01.21.txt
/data/logs/2001.01.22.txt
/data/logs/2001.01.23.txt
/data/logs/2001.01.24.txt
/data/logs/2001.01.25.txt
/data/logs/2001.01.26.txt
/data/logs/2001.01.27.txt
/data/logs/2001.01.28.txt
/data/logs/2001.01.29.txt
/data/logs/2001.01.30.txt
/data/logs/2001.01.31.txt
/data/logs/2001.02.01.txt
/data/logs/2001.02.02.txt
/data/logs/2001.02.03.txt
/data/logs/2001.02.04.txt
/data/logs/2001.02.05.txt
/data/logs/2001.02.06.txt
/data/logs/2001.02.07.txt
/data/logs/2001.02.08.txt
/data/logs/2001.02.09.txt
/data/logs/2001.02.10.txt
/data/logs/2001.02.11.txt
/data/logs/2001.02.12.txt
/data/logs/2001.02.13.txt
/data/logs/2001.02.14.txt
/data/logs/2001.02.15.txt
/data/logs/2001.02.16.txt
/data/logs/2001.02.17.txt
/data/logs/2001.02.18.txt
/data/logs/2001.02.19.txt
/data/logs/2001.02.20.txt
/data/logs/2001.02.21.txt
/data/logs/2001.02.22.txt
/data/logs/2001.02.23.txt
/data/logs/2001.02.24.txt
/data/logs/2001.02.25.txt
/data/logs/2001.02.26.txt
/data/logs/2001.02.27.txt
/data/logs/2001.02.28.txt
/data/logs/2001.03.01.txt
/data/logs/2001.03.02.txt
/data/logs/2001.03.03.txt
/data/logs/2001.03.04.txt
/data/logs/2001.03.05.txt
/data/logs/2001.03.06.txt
/data/logs/2001.03.07.txt
/data/logs/2001.03.08.txt
/data/logs/2001.03.09.txt
/data/logs/2001.03.10.txt
/data/logs/2001.03.11.txt
/data/logs/2001.03.12.txt
/data/logs/2001.03.13.txt
/data/logs/2001.03.14.txt
/data/logs/2001.03.15.txt
/data/logs/2001.03.16.txt
/data/logs/2001.03.17.txt
/data/logs/2001.03.18.txt
/data/logs/2001.03.19.txt
/data/logs/2001.03.20.txt
/data/logs/2001.03.21.txt
/data/logs/2001.03.22.txt
/data/logs/2001.03.23.txt
/data/logs/2001.03.24.txt
/data/logs/2001.03.25.txt
/data/logs/2001.03.26.txt
/data/logs/2001.03.27.txt
/data/logs/2001.03.28.txt
/data/logs/2001.03.29.txt
/data/logs/2001.03.30.txt
/data/logs/2001.03.31.txt
/data/logs/2001.04.01.txt
/data/logs/2001.04.02.txt
/data/logs/2001.04.03.txt
/data/logs/2001.04.04.txt
/data/logs/2001.04.05.txt
/data/logs/2001.04.06.txt
/data/logs/2001.04.07.txt
/data/logs/2001.04.08.txt
/data/logs/2001.04.09.txt
/data/logs/2001.04.10.txt
/data/logs/2001.04.11.txt
/data/logs/2001.04.12.txt
/data/logs/2001.04.13.txt
/data/logs/2001.04.14.txt
/data/logs/2001.04.15.txt
/data/logs/2001.04.16.txt
/data/logs/2001.04.17.txt
/data/logs/2001.04.18.txt
/data/logs/2001.04.19.txt
/data/logs/2001.04.20.txt
/data/logs/2001.04.21.txt
/data/logs/2001.04.22.txt
/data/logs/2001.04.23.txt
/data/logs/2001.04.24.txt
/data/logs/2001.04.25.txt
/data/logs/2001.04.26.txt
/data/logs/2001.04.27.txt
/data/logs/2001.04.28.txt
/data/logs/2001.04.29.txt
/data/logs/2001.04.30.txt
/data/logs/2001.05.01.txt
/data/logs/2001.05.02.txt
/data/logs/2001.05.03.txt
/data/logs/2001.05.04.txt
/data/logs/2001.05.05.txt
/data/logs/2001.05.06.txt
/data/logs/2001.05.07.txt
/data/logs/2001.05.08.txt
/data/logs/2001.05.09.txt
/data/logs/2001.05.10.txt
/data/logs/2001.05.11.txt
/data/logs/2001.05.12.txt
/data/logs/2001.05.13.txt
/data/logs/2001.05.14.txt
/data/logs/2001.05.15.txt
/data/logs/2001.05.16.txt
/data/logs/2001.05.17.txt
/data/logs/2001.05.18.txt
/data/logs/2001.05.19.txt
/data/logs/2001.05.20.txt
/data/logs/2001.05.21.txt
/data/logs/2001.05.22.txt
/data/logs/2001.05.23.txt
/data/logs/2001.05.24.txt
/data/logs/2001.05.25.txt
/data/logs/2001.05.26.txt
/data/logs/2001.05.27.txt
/data/logs/2001.05.28.txt
/data/logs/2001.05.29.txt
/data/logs/2001.05.30.txt
/data/logs/2001.05.31.txt
/data/logs/2001.06.01.txt
/data/logs/2001.06.02.txt
/data/logs/2001.06.03.txt
/data/logs/2001.06.04.txt
/data/logs/2001.06.05.txt
/data/logs/2001.06.06.txt
/data/logs/2001.06.07.txt
/data/logs/2001.06.08.txt
/data/logs/2001.06.09.txt
/data/logs/2001.06.10.txt
/data/logs/2001.06.11.txt
/data/logs/2001.06.12.txt
/data/logs/2001.06.13.txt
/data/logs/2001.06.14.txt
/data/logs/2001.06.15.txt
/data/logs/2001.06.16.txt
/data/logs/2001.06.17.txt
/data/logs/2001.06.18.txt
/data/logs/2001.06.19.txt
/data/logs/2001.06.20.txt
/data/logs/2001.06.21.txt
/data/logs/2001.06.22.txt
/data/logs/2001.06.23.txt
/data/logs/2001.06.24.txt
/data/logs/2001.06.25.txt
/data/logs/2001.06.26.txt
/data/logs/2001.06.27.txt
/data/logs/2001.06.28.txt
/data/logs/2001.06.29.txt
/data/logs/2001.06.30.txt
/data/logs/2001.07.01.txt
/data/logs/2001.07.02.txt
/data/logs/2001.07.03.txt
/data/logs/2001.07.04.txt
/data/logs/2001.07.05.txt
/data/logs/2001.07.06.txt
/data/logs/2001.07.07.txt
/data/logs/2001.07.08.txt
/data/logs/2001.07.09.txt
/data/logs/2001.07.10.txt
/data/logs/2001.07.11.txt
/data/logs/2001.07.12.txt
/data/logs/2001.07.13.txt
/data/logs/2001.07.14.txt
/data/logs/2001.07.15.txt
/data/logs/2001.07.16.txt
/data/logs/2001.07.17.txt
/data/logs/2001.07.18.txt
/data/logs/2001.07.19.txt
/data/logs/2001.07.20.txt
/data/logs/2001.07.21.txt
/data/logs/2001.07.22.txt
/data/logs/2001.07.23.txt
/data/logs/2001.07.24.txt
/data/logs/2001.07.25.txt
/data/logs/2001.07.26.txt
/data/logs/2001.07.27.txt
/data/logs/2001.07.28.txt
/data/logs/2001.07.29.txt
/data/logs/2001.07.30.txt
/data/logs/2001.07.31.txt
/data/logs/2001.08.01.txt
/data/logs/2001.08.02.txt
/data/logs/2001.08.03.txt
/data/logs/2001.08.04.txt
/data/logs/2001.08.05.txt
/data/logs/2001.08.06.txt
/data/logs/2001.08.07.txt
/data/logs/2001.08.08.txt
/data/logs/2001.08.09.txt
/data/logs/2001.08.10.txt
/data/logs/2001.08.11.txt
/data/logs/2001.08.12.txt
/data/logs/2001.08.13.txt
/data/logs/2001.08.14.txt
/data/logs/2001.08.15.txt
/data/logs/2001.08.16.txt
/data/logs/2001.08.17.txt
/data/logs/2001.08.18.txt
/data/logs/2001.08.19.txt
/data/logs/2001.08.20.txt
/data/logs/2001.08.21.txt
/data/logs/2001.08.22.txt
/data/logs/2001.08.23.txt
/data/logs/2001.08.24.txt
/data/logs/2001.08.25.txt
/data/logs/2001.08.26.txt
/data/logs/2001.08.27.txt
/data/logs/2001.08.28.txt
/data/logs/2001.08.29.txt
/data/logs/2001.08.30.txt
/data/logs/2001.08.31.txt
/data/logs/2001.09.01.txt
/data/logs/2001.09.02.txt
/data/logs/2001.09.03.txt
/data/logs/2001.09.04.txt
/data/logs/2001.09.05.txt
/data/logs/2001.09.06.txt
/data/logs/2001.09.07.txt
/data/logs/2001.09.08.txt
/data/logs/2001.09.09.txt
/data/logs/2001.09.10.txt
/data/logs/2001.09.11.txt
/data/logs/2001.09.12.txt
/data/logs/2001.09.13.txt
/data/logs/2001.09.14.txt
/data/logs/2001.09.15.txt
/data/logs/2001.09.16.txt
/data/logs/2001.09.17.txt
/data/logs/2001.09.18.txt
/data/logs/2001.09.19.txt
/data/logs/2001.09.20.txt
/data/logs/2001.09.21.txt
/data/logs/2001.09.22.txt
/data/logs/2001.09.23.txt
/data/logs/2001.09.24.txt
/data/logs/2001.09.25.txt
/data/logs/2001.09.26.txt
/data/logs/2001.09.27.txt
/data/logs/2001.09.28.txt
/data/logs/2001.09.29.txt
/data/logs/2001.09.30.txt
/data/logs/2001.10.01.txt
/data/logs/2001.10.02.txt
/data/logs/2001.10.03.txt
/data/logs/2001.10.04.txt
/data/logs/2001.10.05.txt
/data/logs/2001.10.06.txt
/data/logs/2001.10.07.txt
/data/logs/2001.10.08.txt
/data/logs/2001.10.09.txt
/data/logs/2001.10.10.txt
/data/logs/2001.10.11.txt
/data/logs/2001.10.12.txt
/data/logs/2001.10.13.txt
/data/logs/2001.10.14.txt
/data/logs/2001.10.15.txt
/data/logs/2001.10.16.txt
/data/logs/2001.10.17.txt
/data/logs/2001.10.18.txt
/data/logs/2001.10.19.txt
/data/logs/2001.10.20.txt
/data/logs/2001.10.21.txt
/data/logs/2001.10.22.txt
/data/logs/2001.10.23.txt
/data/logs/2001.10.24.txt
/data/logs/2001.10.25.txt
/data/logs/2001.10.26.txt
/data/logs/2001.10.27.txt
/data/logs/2001.10.28.txt
/data/logs/2001.10.29.txt
/data/logs/2001.10.30.txt
/data/logs/2001.10.31.txt
/data/logs/2001.11.01.txt
/data/logs/2001.11.02.txt
/data/logs/2001.11.03.txt
/data/logs/2001.11.04.txt
/data/logs/2001.11.05.txt
/data/logs/2001.11.06.txt
/data/logs/2001.11.07.txt
/data/logs/2001.11.08.txt
/data/logs/2001.11.09.txt
/data/logs/2001.11.10.txt
/data/logs/2001.11.11.txt
/data/logs/2001.11.12.txt
/data/logs/2001.11.13.txt
/data/logs/2001.11.14.txt
/data/logs/2001.11.15.txt
/data/logs/2001.11.16.txt
/data/logs/2001.11.17.txt
/data/logs/2001.11.18.txt
/data/logs/2001.11.19.txt
/data/logs/2001.11.20.txt
/data/logs/2001.11.21.txt
/data/logs/2001.11.22.txt
/data/logs/2001.11.23.txt
/data/logs/2001.11.24.txt
/data/logs/2001.11.25.txt
/data/logs/2001.11.26.txt
/data/logs/2001.11.27.txt
/data/logs/2001.11.28.txt
/data/logs/2001.11.29.txt
/data/logs/2001.11.30.txt
/data/logs/2001.12.01.txt
/data/logs/2001.12.02.txt
/data/logs/2001.12.03.txt
/data/logs/2001.12.04.txt
/data/logs/2001.12.05.txt
/data/logs/2001.12.06.txt
/data/logs/2001.12.07.txt
/data/logs/2001.12.08.txt
/data/logs/2001.12.09.txt
/data/logs/2001.12.10.txt
/data/logs/2001.12.11.txt
/data/logs/2001.12.12.txt
/data/logs/2001.12.13.txt
/data/logs/2001.12.14.txt
/data/logs/2001.12.15.txt
/data/logs/2001.12.16.txt
/data/logs/2001.12.17.txt
/data/logs/2001.12.18.txt
/data/logs/2001.12.19.txt
/data/logs/2001.12.20.txt
/data/logs/2001.12.21.txt
/data/logs/2001.12.22.txt
/data/logs/2001.12.23.txt
/data/logs/2001.12.24.txt
/data/logs/2001.12.25.txt
/data/logs/2001.12.26.txt
/data/logs/2001.12.27.txt
/data/logs/2001.12.28.txt
/data/logs/2001.12.29.txt
/data/logs/2001.12.30.txt
/data/logs/2001.12.31.txt
/data/logs/2002.01.01.txt
/data/logs/2002.01.02.txt
/data/logs/2002.01.03.txt
/data/logs/2002.01.04.txt
/data/logs/2002.01.05.txt
/data/logs/2002.01.06.txt
/data/logs/2002.01.07.txt
/data/logs/2002.01.08.txt
/data/logs/2002.01.09.txt
/data/logs/2002.01.10.txt
/data/logs/2002.01.11.txt
/data/logs/2002.01.12.txt
/data/logs/2002.01.13.txt
/data/logs/2002.01.14.txt
/data/logs/2002.01.15.txt
/data/logs/2002.01.16.txt
/data/logs/2002.01.17.txt
/data/logs/2002.01.18.txt
/data/logs/2002.01.19.txt
/data/logs/2002.01.20.txt
/data/logs/2002.01.21.txt
/data/logs/2002.01.22.txt
/data/logs/2002.01.23.txt
/data/logs/2002.01.24.txt
/data/logs/2002.01.25.txt
/data/logs/2002.01.26.txt
/data/logs/2002.01.27.txt
/data/logs/2002.01.28.txt
/data/logs/2002.01.29.txt
/data/logs/2002.01.30.txt
/data/logs/2002.01.31.txt
/data/logs/2002.02.01.txt
/data/logs/2002.02.02.txt
/data/logs/2002.02.03.txt
/data/logs/2002.02.04.txt
/data/logs/2002.02.05.txt
/data/logs/2002.02.06.txt
/data/logs/2002.02.07.txt
/data/logs/2002.02.08.txt
/data/logs/2002.02.09.txt
/data/logs/2002.02.10.txt
/data/logs/2002.02.11.txt
/data/logs/2002.02.12.txt
/data/logs/2002.02.13.txt
/data/logs/2002.02.14.txt
/data/logs/2002.02.15.txt
/data/logs/2002.02.16.txt
/data/logs/2002.02.17.txt
/data/logs/2002.02.18.txt
/data/logs/2002.02.19.txt
/data/logs/2002.02.20.txt
/data/logs/2002.02.21.txt
/data/logs/2002.02.22.txt
/data/logs/2002.02.23.txt
/data/logs/2002.02.24.txt
/data/logs/2002.02.25.txt
/data/logs/2002.02.26.txt
/data/logs/2002.02.27.txt
/data/logs/2002.02.28.txt
/data/logs/2002.03.01.txt
/data/logs/2002.03.02.txt
/data/logs/2002.03.03.txt
/data/logs/2002.03.04.txt
/data/logs/2002.03.05.txt
/data/logs/2002.03.06.txt
/data/logs/2002.03.07.txt
/data/logs/2002.03.08.txt
/data/logs/2002.03.09.txt
/data/logs/2002.03.10.txt
/data/logs/2002.03.11.txt
/data/logs/2002.03.12.txt
/data/logs/2002.03.13.txt
/data/logs/2002.03.14.txt
/data/logs/2002.03.15.txt
/data/logs/2002.03.16.txt
/data/logs/2002.03.17.txt
/data/logs/2002.03.18.txt
/data/logs/2002.03.19.txt
/data/logs/2002.03.20.txt
/data/logs/2002.03.21.txt
/data/logs/2002.03.22.txt
/data/logs/2002.03.23.txt
/data/logs/2002.03.24.txt
/data/logs/2002.03.25.txt
/data/logs/2002.03.26.txt
/data/logs/2002.03.27.txt
/data/logs/2002.03.28.txt
/data/logs/2002.03.29.txt
/data/logs/2002.03.30.txt
/data/logs/2002.03.31.txt
/data/logs/2002.04.01.txt
/data/logs/2002.04.02.txt
/data/logs/2002.04.03.txt
/data/logs/2002.04.04.txt
/data/logs/2002.04.05.txt
/data/logs/2002.04.06.txt
/data/logs/2002.04.07.txt
/data/logs/2002.04.08.txt
/data/logs/2002.04.09.txt
/data/logs/2002.04.10.txt
/data/logs/2002.04.11.txt
/data/logs/2002.04.12.txt
/data/logs/2002.04.13.txt
/data/logs/2002.04.14.txt
/data/logs/2002.04.15.txt
/data/logs/2002.04.16.txt
/data/logs/2002.04.17.txt
/data/logs/2002.04.18.txt
/data/logs/2002.04.19.txt
/data/logs/2002.04.20.txt
/data/logs/2002.04.21.txt
/data/logs/2002.04.22.txt
/data/logs/2002.04.23.txt
/data/logs/2002.04.24.txt
/data/logs/2002.04.25.txt
/data/logs/2002.04.26.txt
/data/logs/2002.04.27.txt
/data/logs/2002.04.28.txt
/data/logs/2002.04.29.txt
/data/logs/2002.04.30.txt
/data/logs/2002.05.01.txt
/data/logs/2002.05.02.txt
/data/logs/2002.05.03.txt
/data/logs/2002.05.04.txt
/data/logs/2002.05.05.txt
/data/logs/2002.05.06.txt
/data/logs/2002.05.07.txt
/data/logs/2002.05.08.txt
/data/logs/2002.05.09.txt
/data/logs/2002.05.10.txt
/data/logs/2002.05.11.txt
/data/logs/2002.05.12.txt
/data/logs/2002.05.13.txt
/data/logs/2002.05.14.txt
/data/logs/2002.05.15.txt
/data/logs/2002.05.16.txt
/data/logs/2002.05.17.txt
/data/logs/2002.05.18.txt
/data/logs/2002.05.19.txt
/data/logs/2002.05.20.txt
/data/logs/2002.05.21.txt
/data/logs/2002.05.22.txt
/data/logs/2002.05.23.txt
/data/logs/2002.05.24.txt
/data/logs/2002.05.25.txt
/data/logs/2002.05.26.txt
/data/logs/2002.05.27.txt
/data/logs/2002.05.28.txt
/data/logs/2002.05.29.txt
/data/logs/2002.05.30.txt
/data/logs/2002.05.31.txt
/data/logs/2002.06.01.txt
/data/logs/2002.06.02.txt
/data/logs/2002.06.03.txt
/data/logs/2002.06.04.txt
/data/logs/2002.06.05.txt
/data/logs/2002.06.06.txt
/data/logs/2002.06.08.txt
/data/logs/2002.06.09.txt
/data/logs/2002.06.10.txt
/data/logs/2002.06.11.txt
/data/logs/2002.06.12.txt
/data/logs/2002.06.13.txt
/data/logs/2002.06.14.txt
/data/logs/2002.06.15.txt
/data/logs/2002.06.16.txt
/data/logs/2002.06.17.txt
/data/logs/2002.06.18.txt
/data/logs/2002.06.19.txt
/data/logs/2002.06.20.txt
/data/logs/2002.06.21.txt
/data/logs/2002.06.22.txt
/data/logs/2002.06.23.txt
/data/logs/2002.06.24.txt
/data/logs/2002.06.25.txt
/data/logs/2002.06.26.txt
/data/logs/2002.06.27.txt
/data/logs/2002.06.28.txt
/data/logs/2002.06.29.txt
/data/logs/2002.06.30.txt
/data/logs/2002.07.01.txt
/data/logs/2002.07.02.txt
/data/logs/2002.07.03.txt
/data/logs/2002.07.04.txt
/data/logs/2002.07.05.txt
/data/logs/2002.07.06.txt
/data/logs/2002.07.07.txt
/data/logs/2002.07.08.txt
/data/logs/2002.07.09.txt
/data/logs/2002.07.10.txt
/data/logs/2002.07.11.txt
/data/logs/2002.07.12.txt
/data/logs/2002.07.13.txt
/data/logs/2002.07.14.txt
/data/logs/2002.07.15.txt
/data/logs/2002.07.16.txt
/data/logs/2002.07.17.txt
/data/logs/2002.07.18.txt
/data/logs/2002.07.19.txt
/data/logs/2002.07.20.txt
/data/logs/2002.07.21.txt
/data/logs/2002.07.22.txt
/data/logs/2002.07.23.txt
/data/logs/2002.07.24.txt
/data/logs/2002.07.25.txt
/data/logs/2002.07.26.txt
/data/logs/2002.07.27.txt
/data/logs/2002.07.28.txt
/data/logs/2002.07.29.txt
/data/logs/2002.07.30.txt
/data/logs/2002.07.31.txt
/data/logs/2002.08.01.txt
/data/logs/2002.08.02.txt
/data/logs/2002.08.03.txt
/data/logs/2002.08.04.txt
/data/logs/2002.08.05.txt
/data/logs/2002.08.06.txt
/data/logs/2002.08.07.txt
/data/logs/2002.08.08.txt
/data/logs/2002.08.09.txt
/data/logs/2002.08.10.txt
/data/logs/2002.08.11.txt
/data/logs/2002.08.12.txt
/data/logs/2002.08.13.txt
/data/logs/2002.08.14.txt
/data/logs/2002.08.15.txt
/data/logs/2002.08.16.txt
/data/logs/2002.08.17.txt
/data/logs/2002.08.18.txt
/data/logs/2002.08.19.txt
/data/logs/2002.08.20.txt
/data/logs/2002.08.21.txt
/data/logs/2002.08.22.txt
/data/logs/2002.08.23.txt
/data/logs/2002.08.24.txt
/data/logs/2002.08.25.txt
/data/logs/2002.08.26.txt
/data/logs/2002.08.27.txt
/data/logs/2002.08.28.txt
/data/logs/2002.08.29.txt
/data/logs/2002.08.30.txt
/data/logs/2002.08.31.txt
/data/logs/2002.09.01.txt
/data/logs/2002.09.02.txt
/data/logs/2002.09.03.txt
/data/logs/2002.09.04.txt
/data/logs/2002.09.05.txt
/data/logs/2002.09.06.txt
/data/logs/2002.09.07.txt
/data/logs/2002.09.08.txt
/data/logs/2002.09.09.txt
/data/logs/2002.09.10.txt
/data/logs/2002.09.11.txt
/data/logs/2002.09.12.txt
/data/logs/2002.09.13.txt
/data/logs/2002.09.14.txt
/data/logs/2002.09.15.txt
/data/logs/2002.09.16.txt
/data/logs/2002.09.17.txt
/data/logs/2002.09.18.txt
/data/logs/2002.09.19.txt
/data/logs/2002.09.20.txt
/data/logs/2002.09.21.txt
/data/logs/2002.09.22.txt
/data/logs/2002.09.23.txt
/data/logs/2002.09.24.txt
/data/logs/2002.09.25.txt
/data/logs/2002.09.26.txt
/data/logs/2002.09.27.txt
/data/logs/2002.09.28.txt
/data/logs/2002.09.29.txt
/data/logs/2002.09.30.txt
/data/logs/2002.10.01.txt
/data/logs/2002.10.02.txt
/data/logs/2002.10.03.txt
/data/logs/2002.10.04.txt
/data/logs/2002.10.05.txt
/data/logs/2002.10.06.txt
/data/logs/2002.10.07.txt
/data/logs/2002.10.08.txt
/data/logs/2002.10.09.txt
/data/logs/2002.10.10.txt
/data/logs/2002.10.11.txt
/data/logs/2002.10.12.txt
/data/logs/2002.10.13.txt
/data/logs/2002.10.14.txt
/data/logs/2002.10.15.txt
/data/logs/2002.10.16.txt
/data/logs/2002.10.17.txt
/data/logs/2002.10.18.txt
/data/logs/2002.10.19.txt
/data/logs/2002.10.20.txt
/data/logs/2002.10.21.txt
/data/logs/2002.10.22.txt
/data/logs/2002.10.23.txt
/data/logs/2002.10.24.txt
/data/logs/2002.10.25.txt
/data/logs/2002.10.26.txt
/data/logs/2002.10.27.txt
/data/logs/2002.10.28.txt
/data/logs/2002.10.29.txt
/data/logs/2002.10.30.txt
/data/logs/2002.10.31.txt
/data/logs/2002.11.01.txt
/data/logs/2002.11.02.txt
/data/logs/2002.11.03.txt
/data/logs/2002.11.04.txt
/data/logs/2002.11.05.txt
/data/logs/2002.11.06.txt
/data/logs/2002.11.07.txt
/data/logs/2002.11.08.txt
/data/logs/2002.11.09.txt
/data/logs/2002.11.10.txt
/data/logs/2002.11.11.txt
/data/logs/2002.11.12.txt
/data/logs/2002.11.13.txt
/data/logs/2002.11.14.txt
/data/logs/2002.11.15.txt
/data/logs/2002.11.16.txt
/data/logs/2002.11.17.txt
/data/logs/2002.11.18.txt
/data/logs/2002.11.19.txt
/data/logs/2002.11.20.txt
/data/logs/2002.11.21.txt
/data/logs/2002.11.22.txt
/data/logs/2002.11.23.txt
/data/logs/2002.11.24.txt
/data/logs/2002.11.25.txt
/data/logs/2002.11.26.txt
/data/logs/2002.11.27.txt
/data/logs/2002.11.28.txt
/data/logs/2002.11.29.txt
/data/logs/2002.11.30.txt
/data/logs/2002.12.01.txt
/data/logs/2002.12.02.txt
/data/logs/2002.12.03.txt
/data/logs/2002.12.04.txt
/data/logs/2002.12.05.txt
/data/logs/2002.12.06.txt
/data/logs/2002.12.07.txt
/data/logs/2002.12.08.txt
/data/logs/2002.12.09.txt
/data/logs/2002.12.10.txt
/data/logs/2002.12.11.txt
/data/logs/2002.12.12.txt
/data/logs/2002.12.13.txt
/data/logs/2002.12.14.txt
/data/logs/2002.12.15.txt
/data/logs/2002.12.16.txt
/data/logs/2002.12.17.txt
/data/logs/2002.12.18.txt
/data/logs/2002.12.19.txt
/data/logs/2002.12.20.txt
/data/logs/2002.12.21.txt
/data/logs/2002.12.22.txt
/data/logs/2002.12.23.txt
/data/logs/2002.12.24.txt
/data/logs/2002.12.25.txt
/data/logs/2002.12.26.txt
/data/logs/2002.12.27.txt
/data/logs/2002.12.28.txt
/data/logs/2002.12.29.txt
/data/logs/2002.12.30.txt
/data/logs/2002.12.31.txt
/data/logs/2003.01.01.txt
/data/logs/2003.01.02.txt
/data/logs/2003.01.03.txt
/data/logs/2003.01.04.txt
/data/logs/2003.01.05.txt
/data/logs/2003.01.06.txt
/data/logs/2003.01.07.txt
/data/logs/2003.01.08.txt
/data/logs/2003.01.09.txt
/data/logs/2003.01.10.txt
/data/logs/2003.01.11.txt
/data/logs/2003.01.12.txt
/data/logs/2003.01.13.txt
/data/logs/2003.01.14.txt
/data/logs/2003.01.15.txt
/data/logs/2003.01.16.txt
/data/logs/2003.01.17.txt
/data/logs/2003.01.18.txt
/data/logs/2003.01.19.txt
/data/logs/2003.01.20.txt
/data/logs/2003.01.21.txt
/data/logs/2003.01.22.txt
/data/logs/2003.01.23.txt
/data/logs/2003.01.24.txt
/data/logs/2003.01.25.txt
/data/logs/2003.01.26.txt
/data/logs/2003.01.27.txt
/data/logs/2003.01.28.txt
/data/logs/2003.01.29.txt
/data/logs/2003.01.30.txt
/data/logs/2003.01.31.txt
/data/logs/2003.02.01.txt
/data/logs/2003.02.02.txt
/data/logs/2003.02.03.txt
/data/logs/2003.02.04.txt
/data/logs/2003.02.05.txt
/data/logs/2003.02.06.txt
/data/logs/2003.02.07.txt
/data/logs/2003.02.08.txt
/data/logs/2003.02.09.txt
/data/logs/2003.02.10.txt
/data/logs/2003.02.11.txt
/data/logs/2003.02.12.txt
/data/logs/2003.02.13.txt
/data/logs/2003.02.14.txt
/data/logs/2003.02.15.txt
/data/logs/2003.02.16.txt
/data/logs/2003.02.17.txt
/data/logs/2003.02.18.txt
/data/logs/2003.02.19.txt
/data/logs/2003.02.20.txt
/data/logs/2003.02.21.txt
/data/logs/2003.02.22.txt
/data/logs/2003.02.23.txt
/data/logs/2003.02.24.txt
/data/logs/2003.02.25.txt
/data/logs/2003.02.26.txt
/data/logs/2003.02.27.txt
/data/logs/2003.02.28.txt
/data/logs/2003.03.01.txt
/data/logs/2003.03.02.txt
/data/logs/2003.03.03.txt
/data/logs/2003.03.04.txt
/data/logs/2003.03.05.txt
/data/logs/2003.03.06.txt
/data/logs/2003.03.07.txt
/data/logs/2003.03.08.txt
/data/logs/2003.03.09.txt
/data/logs/2003.03.10.txt
/data/logs/2003.03.11.txt
/data/logs/2003.03.12.txt
/data/logs/2003.03.13.txt
/data/logs/2003.03.14.txt
/data/logs/2003.03.15.txt
/data/logs/2003.03.16.txt
/data/logs/2003.03.17.txt
/data/logs/2003.03.18.txt
/data/logs/2003.03.19.txt
/data/logs/2003.03.20.txt
/data/logs/2003.03.21.txt
/data/logs/2003.03.22.txt
/data/logs/2003.03.23.txt
/data/logs/2003.03.24.txt
/data/logs/2003.03.25.txt
/data/logs/2003.03.26.txt
/data/logs/2003.03.27.txt
/data/logs/2003.03.28.txt
/data/logs/2003.03.29.txt
/data/logs/2003.03.30.txt
/data/logs/2003.03.31.txt
/data/logs/2003.04.01.txt
/data/logs/2003.04.02.txt
/data/logs/2003.04.03.txt
/data/logs/2003.04.04.txt
/data/logs/2003.04.05.txt
/data/logs/2003.04.06.txt
/data/logs/2003.04.07.txt
/data/logs/2003.04.08.txt
/data/logs/2003.04.09.txt
/data/logs/2003.04.10.txt
/data/logs/2003.04.11.txt
/data/logs/2003.04.12.txt
/data/logs/2003.04.13.txt
/data/logs/2003.04.14.txt
/data/logs/2003.04.15.txt
/data/logs/2003.04.16.txt
/data/logs/2003.04.17.txt
/data/logs/2003.04.18.txt
/data/logs/2003.04.19.txt
/data/logs/2003.04.20.txt
/data/logs/2003.04.21.txt
/data/logs/2003.04.22.txt
/data/logs/2003.04.23.txt
/data/logs/2003.04.24.txt
/data/logs/2003.04.25.txt
/data/logs/2003.04.26.txt
/data/logs/2003.04.27.txt
/data/logs/2003.04.28.txt
/data/logs/2003.04.29.txt
/data/logs/2003.04.30.txt
/data/logs/2003.05.01.txt
/data/logs/2003.05.02.txt
/data/logs/2003.05.03.txt
/data/logs/2003.05.04.txt
/data/logs/2003.05.05.txt
/data/logs/2003.05.06.txt
/data/logs/2003.05.07.txt
/data/logs/2003.05.08.txt
/data/logs/2003.05.09.txt
/data/logs/2003.05.10.txt
/data/logs/2003.05.11.txt
/data/logs/2003.05.12.txt
/data/logs/2003.05.13.txt
/data/logs/2003.05.14.txt
/data/logs/2003.05.15.txt
/data/logs/2003.05.16.txt
/data/logs/2003.05.17.txt
/data/logs/2003.05.18.txt
/data/logs/2003.05.19.txt
/data/logs/2003.05.20.txt
/data/logs/2003.05.21.txt
/data/logs/2003.05.22.txt
/data/logs/2003.05.23.txt
/data/logs/2003.05.24.txt
/data/logs/2003.05.25.txt
/data/logs/2003.05.26.txt
/data/logs/2003.05.27.txt
/data/logs/2003.05.28.txt
/data/logs/2003.05.29.txt
/data/logs/2003.05.30.txt
/data/logs/2003.05.31.txt
/data/logs/2003.06.01.txt
/data/logs/2003.06.02.txt
/data/logs/2003.06.03.txt
/data/logs/2003.06.04.txt
/data/logs/2003.06.05.txt
/data/logs/2003.06.06.txt
/data/logs/2003.06.07.txt
/data/logs/2003.06.08.txt
/data/logs/2003.06.09.txt
/data/logs/2003.06.10.txt
/data/logs/2003.06.11.txt
/data/logs/2003.06.12.txt
/data/logs/2003.06.13.txt
/data/logs/2003.06.14.txt
/data/logs/2003.06.15.txt
/data/logs/2003.06.16.txt
/data/logs/2003.06.17.txt
/data/logs/2003.06.18.txt
/data/logs/2003.06.19.txt
/data/logs/2003.06.20.txt
/data/logs/2003.06.21.txt
/data/logs/2003.06.22.txt
/data/logs/2003.06.23.txt
/data/logs/2003.06.24.txt
/data/logs/2003.06.25.txt
/data/logs/2003.06.26.txt
/data/logs/2003.06.27.txt
/data/logs/2003.06.28.txt
/data/logs/2003.06.29.txt
/data/logs/2003.06.30.txt
/data/logs/2003.07.01.txt
/data/logs/2003.07.02.txt
/data/logs/2003.07.03.txt
/data/logs/2003.07.04.txt
/data/logs/2003.07.05.txt
/data/logs/2003.07.06.txt
/data/logs/2003.07.07.txt
/data/logs/2003.07.08.txt
/data/logs/2003.07.09.txt
/data/logs/2003.07.10.txt
/data/logs/2003.07.11.txt
/data/logs/2003.07.12.txt
/data/logs/2003.07.13.txt
/data/logs/2003.07.14.txt
/data/logs/2003.07.15.txt
/data/logs/2003.07.16.txt
/data/logs/2003.07.17.txt
/data/logs/2003.07.18.txt
/data/logs/2003.07.19.txt
/data/logs/2003.07.20.txt
/data/logs/2003.07.21.txt
/data/logs/2003.07.22.txt
/data/logs/2003.07.23.txt
/data/logs/2003.07.24.txt
/data/logs/2003.07.25.txt
/data/logs/2003.07.26.txt
/data/logs/2003.07.27.txt
/data/logs/2003.07.28.txt
/data/logs/2003.07.29.txt
/data/logs/2003.07.30.txt
/data/logs/2003.07.31.txt
/data/logs/2003.08.01.txt
/data/logs/2003.08.02.txt
/data/logs/2003.08.03.txt
/data/logs/2003.08.04.txt
/data/logs/2003.08.05.txt
/data/logs/2003.08.06.txt
/data/logs/2003.08.07.txt
/data/logs/2003.08.08.txt
/data/logs/2003.08.09.txt
/data/logs/2003.08.10.txt
/data/logs/2003.08.11.txt
/data/logs/2003.08.12.txt
/data/logs/2003.08.13.txt
/data/logs/2003.08.14.txt
/data/logs/2003.08.15.txt
/data/logs/2003.08.16.txt
/data/logs/2003.08.17.txt
/data/logs/2003.08.18.txt
/data/logs/2003.08.19.txt
/data/logs/2003.08.20.txt
/data/logs/2003.08.21.txt
/data/logs/2003.08.22.txt
/data/logs/2003.08.23.txt
/data/logs/2003.08.24.txt
/data/logs/2003.08.25.txt
/data/logs/2003.08.26.txt
/data/logs/2003.08.27.txt
/data/logs/2003.08.28.txt
/data/logs/2003.08.29.txt
/data/logs/2003.08.30.txt
/data/logs/2003.08.31.txt
/data/logs/2003.09.01.txt
/data/logs/2003.09.02.txt
/data/logs/2003.09.03.txt
/data/logs/2003.09.04.txt
/data/logs/2003.09.05.txt
/data/logs/2003.09.06.txt
/data/logs/2003.09.07.txt
/data/logs/2003.09.08.txt
/data/logs/2003.09.09.txt
/data/logs/2003.09.10.txt
/data/logs/2003.09.11.txt
/data/logs/2003.09.12.txt
/data/logs/2003.09.13.txt
/data/logs/2003.09.14.txt
/data/logs/2003.09.15.txt
/data/logs/2003.09.16.txt
/data/logs/2003.09.17.txt
/data/logs/2003.09.18.txt
/data/logs/2003.09.19.txt
/data/logs/2003.09.20.txt
/data/logs/2003.09.21.txt
/data/logs/2003.09.22.txt
/data/logs/2003.09.23.txt
/data/logs/2003.09.24.txt
/data/logs/2003.09.25.txt
/data/logs/2003.09.26.txt
/data/logs/2003.09.27.txt
/data/logs/2003.09.28.txt
/data/logs/2003.09.29.txt
/data/logs/2003.09.30.txt
/data/logs/2003.10.01.txt
/data/logs/2003.10.02.txt
/data/logs/2003.10.03.txt
/data/logs/2003.10.04.txt
/data/logs/2003.10.05.txt
/data/logs/2003.10.06.txt
/data/logs/2003.10.07.txt
/data/logs/2003.10.08.txt
/data/logs/2003.10.09.txt
/data/logs/2003.10.10.txt
/data/logs/2003.10.11.txt
/data/logs/2003.10.12.txt
/data/logs/2003.10.13.txt
/data/logs/2003.10.14.txt
/data/logs/2003.10.15.txt
/data/logs/2003.10.16.txt
/data/logs/2003.10.17.txt
/data/logs/2003.10.18.txt
/data/logs/2003.10.19.txt
/data/logs/2003.10.20.txt
/data/logs/2003.10.21.txt
/data/logs/2003.10.22.txt
/data/logs/2003.10.23.txt
/data/logs/2003.10.24.txt
/data/logs/2003.10.25.txt
/data/logs/2003.10.26.txt
/data/logs/2003.10.27.txt
/data/logs/2003.10.28.txt
/data/logs/2003.10.29.txt
/data/logs/2003.10.30.txt
/data/logs/2003.10.31.txt
/data/logs/2003.11.01.txt
/data/logs/2003.11.02.txt
/data/logs/2003.11.03.txt
/data/logs/2003.11.04.txt
/data/logs/2003.11.05.txt
/data/logs/2003.11.06.txt
/data/logs/2003.11.07.txt
/data/logs/2003.11.08.txt
/data/logs/2003.11.09.txt
/data/logs/2003.11.10.txt
/data/logs/2003.11.11.txt
/data/logs/2003.11.12.txt
/data/logs/2003.11.13.txt
/data/logs/2003.11.14.txt
/data/logs/2003.11.15.txt
/data/logs/2003.11.16.txt
/data/logs/2003.11.17.txt
/data/logs/2003.11.18.txt
/data/logs/2003.11.19.txt
/data/logs/2003.11.20.txt
/data/logs/2003.11.21.txt
/data/logs/2003.11.22.txt
/data/logs/2003.11.23.txt
/data/logs/2003.11.24.txt
/data/logs/2003.11.25.txt
/data/logs/2003.11.26.txt
/data/logs/2003.11.27.txt
/data/logs/2003.11.28.txt
/data/logs/2003.11.29.txt
/data/logs/2003.11.30.txt
/data/logs/2003.12.01.txt
/data/logs/2003.12.02.txt
/data/logs/2003.12.03.txt
/data/logs/2003.12.04.txt
/data/logs/2003.12.05.txt
/data/logs/2003.12.06.txt
/data/logs/2003.12.07.txt
/data/logs/2003.12.08.txt
/data/logs/2003.12.09.txt
/data/logs/2003.12.10.txt
/data/logs/2003.12.11.txt
/data/logs/2003.12.12.txt
/data/logs/2003.12.13.txt
/data/logs/2003.12.14.txt
/data/logs/2003.12.15.txt
/data/logs/2003.12.16.txt
/data/logs/2003.12.17.txt
/data/logs/2003.12.18.txt
/data/logs/2003.12.19.txt
/data/logs/2003.12.20.txt
/data/logs/2003.12.21.txt
/data/logs/2003.12.22.txt
/data/logs/2003.12.23.txt
/data/logs/2003.12.24.txt
/data/logs/2003.12.25.txt
/data/logs/2003.12.26.txt
/data/logs/2003.12.27.txt
/data/logs/2003.12.28.txt
/data/logs/2003.12.29.txt
/data/logs/2003.12.30.txt
/data/logs/2003.12.31.txt
/data/logs/2004.01.01.txt
/data/logs/2004.01.02.txt
/data/logs/2004.01.03.txt
/data/logs/2004.01.04.txt
/data/logs/2004.01.05.txt
/data/logs/2004.01.06.txt
/data/logs/2004.01.07.txt
/data/logs/2004.01.08.txt
/data/logs/2004.01.09.txt
/data/logs/2004.01.10.txt
/data/logs/2004.01.11.txt
/data/logs/2004.01.12.txt
/data/logs/2004.01.13.txt
/data/logs/2004.01.14.txt
/data/logs/2004.01.15.txt
/data/logs/2004.01.16.txt
/data/logs/2004.01.17.txt
/data/logs/2004.01.18.txt
/data/logs/2004.01.19.txt
/data/logs/2004.01.20.txt
/data/logs/2004.01.21.txt
/data/logs/2004.01.22.txt
/data/logs/2004.01.23.txt
/data/logs/2004.01.24.txt
/data/logs/2004.01.25.txt
/data/logs/2004.01.26.txt
/data/logs/2004.01.27.txt
/data/logs/2004.01.28.txt
/data/logs/2004.01.29.txt
/data/logs/2004.01.30.txt
/data/logs/2004.01.31.txt
/data/logs/2004.02.01.txt
/data/logs/2004.02.02.txt
/data/logs/2004.02.03.txt
/data/logs/2004.02.04.txt
/data/logs/2004.02.05.txt
/data/logs/2004.02.06.txt
/data/logs/2004.02.07.txt
/data/logs/2004.02.08.txt
/data/logs/2004.02.09.txt
/data/logs/2004.02.10.txt
/data/logs/2004.02.11.txt
/data/logs/2004.02.12.txt
/data/logs/2004.02.13.txt
/data/logs/2004.02.14.txt
/data/logs/2004.02.15.txt
/data/logs/2004.02.16.txt
/data/logs/2004.02.17.txt
/data/logs/2004.02.18.txt
/data/logs/2004.02.19.txt
/data/logs/2004.02.20.txt
/data/logs/2004.02.21.txt
/data/logs/2004.02.22.txt
/data/logs/2004.02.23.txt
/data/logs/2004.02.24.txt
/data/logs/2004.02.25.txt
/data/logs/2004.02.26.txt
/data/logs/2004.02.27.txt
/data/logs/2004.02.28.txt
/data/logs/2004.02.29.txt
/data/logs/2004.03.01.txt
/data/logs/2004.03.02.txt
/data/logs/2004.03.03.txt
/data/logs/2004.03.04.txt
/data/logs/2004.03.05.txt
/data/logs/2004.03.06.txt
/data/logs/2004.03.07.txt
/data/logs/2004.03.08.txt
/data/logs/2004.03.09.txt
/data/logs/2004.03.10.txt
/data/logs/2004.03.11.txt
/data/logs/2004.03.12.txt
/data/logs/2004.03.13.txt
/data/logs/2004.03.14.txt
/data/logs/2004.03.15.txt
/data/logs/2004.03.16.txt
/data/logs/2004.03.17.txt
/data/logs/2004.03.18.txt
/data/logs/2004.03.19.txt
/data/logs/2004.03.20.txt
/data/logs/2004.03.21.txt
/data/logs/2004.03.22.txt
/data/logs/2004.03.23.txt
/data/logs/2004.03.24.txt
/data/logs/2004.03.25.txt
/data/logs/2004.03.26.txt
/data/logs/2004.03.27.txt
/data/logs/2004.03.28.txt
/data/logs/2004.03.29.txt
/data/logs/2004.03.30.txt
/data/logs/2004.03.31.txt
/data/logs/2004.04.01.txt
/data/logs/2004.04.02.txt
/data/logs/2004.04.03.txt
/data/logs/2004.04.04.txt
/data/logs/2004.04.05.txt
/data/logs/2004.04.06.txt
/data/logs/2004.04.07.txt
/data/logs/2004.04.08.txt
/data/logs/2004.04.09.txt
/data/logs/2004.04.10.txt
/data/logs/2004.04.11.txt
/data/logs/2004.04.12.txt
/data/logs/2004.04.13.txt
/data/logs/2004.04.14.txt
/data/logs/2004.04.15.txt
/data/logs/2004.04.16.txt
/data/logs/2004.04.17.txt
/data/logs/2004.04.18.txt
/data/logs/2004.04.19.txt
/data/logs/2004.04.20.txt
/data/logs/2004.04.21.txt
/data/logs/2004.04.22.txt
/data/logs/2004.04.23.txt
/data/logs/2004.04.24.txt
/data/logs/2004.04.25.txt
/data/logs/2004.04.26.txt
/data/logs/2004.04.27.txt
/data/logs/2004.04.28.txt
/data/logs/2004.04.29.txt
/data/logs/2004.04.30.txt
/data/logs/2004.05.01.txt
/data/logs/2004.05.02.txt
/data/logs/2004.05.03.txt
/data/logs/2004.05.04.txt
/data/logs/2004.05.05.txt
/data/logs/2004.05.06.txt
/data/logs/2004.05.07.txt
/data/logs/2004.05.08.txt
/data/logs/2004.05.09.txt
/data/logs/2004.05.10.txt
/data/logs/2004.05.11.txt
/data/logs/2004.05.12.txt
/data/logs/2004.05.13.txt
/data/logs/2004.05.14.txt
/data/logs/2004.05.15.txt
/data/logs/2004.05.16.txt
/data/logs/2004.05.17.txt
/data/logs/2004.05.18.txt
/data/logs/2004.05.19.txt
/data/logs/2004.05.20.txt
/data/logs/2004.05.21.txt
/data/logs/2004.05.22.txt
/data/logs/2004.05.23.txt
/data/logs/2004.05.24.txt
/data/logs/2004.05.25.txt
/data/logs/2004.05.26.txt
/data/logs/2004.05.27.txt
/data/logs/2004.05.28.txt
/data/logs/2004.05.29.txt
/data/logs/2004.05.30.txt
/data/logs/2004.05.31.txt
/data/logs/2004.06.01.txt
/data/logs/2004.06.02.txt
/data/logs/2004.06.03.txt
/data/logs/2004.06.04.txt
/data/logs/2004.06.05.txt
/data/logs/2004.06.06.txt
/data/logs/2004.06.07.txt
/data/logs/2004.06.08.txt
/data/logs/2004.06.09.txt
/data/logs/2004.06.10.txt
/data/logs/2004.06.11.txt
/data/logs/2004.06.12.txt
/data/logs/2004.06.13.txt
/data/logs/2004.06.14.txt
/data/logs/2004.06.15.txt
/data/logs/2004.06.16.txt
/data/logs/2004.06.17.txt
/data/logs/2004.06.18.txt
/data/logs/2004.06.19.txt
/data/logs/2004.06.20.txt
/data/logs/2004.06.21.txt
/data/logs/2004.06.22.txt
/data/logs/2004.06.23.txt
/data/logs/2004.06.24.txt
/data/logs/2004.06.25.txt
/data/logs/2004.06.26.txt
/data/logs/2004.06.27.txt
/data/logs/2004.06.28.txt
/data/logs/2004.06.29.txt
/data/logs/2004.06.30.txt
/data/logs/2004.07.01.txt
/data/logs/2004.07.02.txt
/data/logs/2004.07.03.txt
/data/logs/2004.07.04.txt
/data/logs/2004.07.05.txt
/data/logs/2004.07.06.txt
/data/logs/2004.07.07.txt
/data/logs/2004.07.08.txt
/data/logs/2004.07.09.txt
/data/logs/2004.07.10.txt
/data/logs/2004.07.11.txt
/data/logs/2004.07.12.txt
/data/logs/2004.07.13.txt
/data/logs/2004.07.14.txt
/data/logs/2004.07.15.txt
/data/logs/2004.07.16.txt
/data/logs/2004.07.17.txt
/data/logs/2004.07.18.txt
/data/logs/2004.07.19.txt
/data/logs/2004.07.20.txt
/data/logs/2004.07.21.txt
/data/logs/2004.07.22.txt
/data/logs/2004.07.23.txt
/data/logs/2004.07.24.txt
/data/logs/2004.07.25.txt
/data/logs/2004.07.26.txt
/data/logs/2004.07.27.txt
/data/logs/2004.07.28.txt
/data/logs/2004.07.29.txt
/data/logs/2004.07.30.txt
/data/logs/2004.07.31.txt
/data/logs/2004.08.01.txt
/data/logs/2004.08.02.txt
/data/logs/2004.08.03.txt
/data/logs/2004.08.04.txt
/data/logs/2004.08.05.txt
/data/logs/2004.08.06.txt
/data/logs/2004.08.07.txt
/data/logs/2004.08.08.txt
/data/logs/2004.08.09.txt
/data/logs/2004.08.10.txt
/data/logs/2004.08.11.txt
/data/logs/2004.08.12.txt
/data/logs/2004.08.13.txt
/data/logs/2004.08.14.txt
/data/logs/2004.08.15.txt
/data/logs/2004.08.16.txt
/data/logs/2004.08.17.txt
/data/logs/2004.08.18.txt
/data/logs/2004.08.19.txt
/data/logs/2004.08.20.txt
/data/logs/2004.08.21.txt
/data/logs/2004.08.22.txt
/data/logs/2004.08.23.txt
/data/logs/2004.08.24.txt
/data/logs/2004.08.25.txt
/data/logs/2004.08.26.txt
/data/logs/2004.08.27.txt
/data/logs/2004.08.28.txt
/data/logs/2004.08.29.txt
/data/logs/2004.08.30.txt
/data/logs/2004.08.31.txt
/data/logs/2004.09.01.txt
/data/logs/2004.09.02.txt
/data/logs/2004.09.03.txt
/data/logs/2004.09.04.txt
/data/logs/2004.09.05.txt
/data/logs/2004.09.06.txt
/data/logs/2004.09.07.txt
/data/logs/2004.09.08.txt
/data/logs/2004.09.09.txt
/data/logs/2004.09.10.txt
/data/logs/2004.09.11.txt
/data/logs/2004.09.12.txt
/data/logs/2004.09.13.txt
/data/logs/2004.09.14.txt
/data/logs/2004.09.15.txt
/data/logs/2004.09.16.txt
/data/logs/2004.09.17.txt
/data/logs/2004.09.18.txt
/data/logs/2004.09.19.txt
/data/logs/2004.09.20.txt
/data/logs/2004.09.21.txt
/data/logs/2004.09.22.txt
/data/logs/2004.09.23.txt
/data/logs/2004.09.24.txt
/data/logs/2004.09.25.txt
/data/logs/2004.09.26.txt
/data/logs/2004.09.27.txt
/data/logs/2004.09.28.txt
/data/logs/2004.09.29.txt
/data/logs/2004.09.30.txt
/data/logs/2004.10.01.txt
/data/logs/2004.10.02.txt
/data/logs/2004.10.03.txt
/data/logs/2004.10.04.txt
/data/logs/2004.10.05.txt
/data/logs/2004.10.06.txt
/data/logs/2004.10.07.txt
/data/logs/2004.10.08.txt
/data/logs/2004.10.09.txt
/data/logs/2004.10.10.txt
/data/logs/2004.10.11.txt
/data/logs/2004.10.12.txt
/data/logs/2004.10.13.txt
/data/logs/2004.10.14.txt
/data/logs/2004.10.15.txt
/data/logs/2004.10.16.txt
/data/logs/2004.10.17.txt
/data/logs/2004.10.18.txt
/data/logs/2004.10.19.txt
/data/logs/2004.10.20.txt
/data/logs/2004.10.21.txt
/data/logs/2004.10.22.txt
/data/logs/2004.10.23.txt
/data/logs/2004.10.25.txt
/data/logs/2004.10.26.txt
/data/logs/2004.10.27.txt
/data/logs/2004.10.28.txt
/data/logs/2004.10.29.txt
/data/logs/2004.10.30.txt
/data/logs/2004.10.31.txt
/data/logs/2004.11.01.txt
/data/logs/2004.11.02.txt
/data/logs/2004.11.03.txt
/data/logs/2004.11.04.txt
/data/logs/2004.11.05.txt
/data/logs/2004.11.06.txt
/data/logs/2004.11.07.txt
/data/logs/2004.11.08.txt
/data/logs/2004.11.09.txt
/data/logs/2004.11.10.txt
/data/logs/2004.11.11.txt
/data/logs/2004.11.12.txt
/data/logs/2004.11.13.txt
/data/logs/2004.11.14.txt
/data/logs/2004.11.15.txt
/data/logs/2004.11.16.txt
/data/logs/2004.11.17.txt
/data/logs/2004.11.18.txt
/data/logs/2004.11.19.txt
/data/logs/2004.11.20.txt
/data/logs/2004.11.21.txt
/data/logs/2004.11.22.txt
/data/logs/2004.11.23.txt
/data/logs/2004.11.24.txt
/data/logs/2004.11.25.txt
/data/logs/2004.11.26.txt
/data/logs/2004.11.27.txt
/data/logs/2004.11.28.txt
/data/logs/2004.11.29.txt
/data/logs/2004.11.30.txt
/data/logs/2004.12.01.txt
/data/logs/2004.12.02.txt
/data/logs/2004.12.03.txt
/data/logs/2004.12.04.txt
/data/logs/2004.12.05.txt
/data/logs/2004.12.06.txt
/data/logs/2004.12.07.txt
/data/logs/2004.12.08.txt
/data/logs/2004.12.09.txt
/data/logs/2004.12.10.txt
/data/logs/2004.12.11.txt
/data/logs/2004.12.12.txt
/data/logs/2004.12.13.txt
/data/logs/2004.12.14.txt
/data/logs/2004.12.15.txt
/data/logs/2004.12.16.txt
/data/logs/2004.12.17.txt
/data/logs/2004.12.18.txt
/data/logs/2004.12.19.txt
/data/logs/2004.12.20.txt
/data/logs/2004.12.21.txt
/data/logs/2004.12.22.txt
/data/logs/2004.12.23.txt
/data/logs/2004.12.24.txt
/data/logs/2004.12.25.txt
/data/logs/2004.12.26.txt
/data/logs/2004.12.27.txt
/data/logs/2004.12.28.txt
/data/logs/2004.12.29.txt
/data/logs/2004.12.30.txt
/data/logs/2004.12.31.txt
/data/logs/2005.01.01.txt
/data/logs/2005.01.02.txt
/data/logs/2005.01.03.txt
/data/logs/2005.01.04.txt
/data/logs/2005.01.05.txt
/data/logs/2005.01.06.txt
/data/logs/2005.01.07.txt
/data/logs/2005.01.08.txt
/data/logs/2005.01.09.txt
/data/logs/2005.01.10.txt
/data/logs/2005.01.11.txt
/data/logs/2005.01.12.txt
/data/logs/2005.01.13.txt
/data/logs/2005.01.14.txt
/data/logs/2005.01.15.txt
/data/logs/2005.01.16.txt
/data/logs/2005.01.17.txt
/data/logs/2005.01.18.txt
/data/logs/2005.01.19.txt
/data/logs/2005.01.20.txt
/data/logs/2005.01.21.txt
/data/logs/2005.01.22.txt
/data/logs/2005.01.23.txt
/data/logs/2005.01.24.txt
/data/logs/2005.01.25.txt
/data/logs/2005.01.26.txt
/data/logs/2005.01.27.txt
/data/logs/2005.01.28.txt
/data/logs/2005.01.29.txt
/data/logs/2005.01.30.txt
/data/logs/2005.01.31.txt
/data/logs/2005.02.01.txt
/data/logs/2005.02.02.txt
/data/logs/2005.02.03.txt
/data/logs/2005.02.04.txt
/data/logs/2005.02.05.txt
/data/logs/2005.02.06.txt
/data/logs/2005.02.07.txt
/data/logs/2005.02.08.txt
/data/logs/2005.02.09.txt
/data/logs/2005.02.10.txt
/data/logs/2005.02.11.txt
/data/logs/2005.02.12.txt
/data/logs/2005.02.13.txt
/data/logs/2005.02.14.txt
/data/logs/2005.02.15.txt
/data/logs/2005.02.16.txt
/data/logs/2005.02.17.txt
/data/logs/2005.02.18.txt
/data/logs/2005.02.19.txt
/data/logs/2005.02.20.txt
/data/logs/2005.02.21.txt
/data/logs/2005.02.22.txt
/data/logs/2005.02.23.txt
/data/logs/2005.02.24.txt
/data/logs/2005.02.25.txt
/data/logs/2005.02.26.txt
/data/logs/2005.02.27.txt
/data/logs/2005.02.28.txt
/data/logs/2005.03.01.txt
/data/logs/2005.03.02.txt
/data/logs/2005.03.03.txt
/data/logs/2005.03.04.txt
/data/logs/2005.03.05.txt
/data/logs/2005.03.06.txt
/data/logs/2005.03.07.txt
/data/logs/2005.03.08.txt
/data/logs/2005.03.09.txt
/data/logs/2005.03.10.txt
/data/logs/2005.03.11.txt
/data/logs/2005.03.12.txt
/data/logs/2005.03.13.txt
/data/logs/2005.03.14.txt
/data/logs/2005.03.15.txt
/data/logs/2005.03.16.txt
/data/logs/2005.03.17.txt
/data/logs/2005.03.18.txt
/data/logs/2005.03.19.txt
/data/logs/2005.03.20.txt
/data/logs/2005.03.21.txt
/data/logs/2005.03.22.txt
/data/logs/2005.03.23.txt
/data/logs/2005.03.24.txt
/data/logs/2005.03.25.txt
/data/logs/2005.03.26.txt
/data/logs/2005.03.27.txt
/data/logs/2005.03.28.txt
/data/logs/2005.03.29.txt
/data/logs/2005.03.30.txt
/data/logs/2005.03.31.txt
/data/logs/2005.04.01.txt
/data/logs/2005.04.02.txt
/data/logs/2005.04.03.txt
/data/logs/2005.04.04.txt
/data/logs/2005.04.05.txt
/data/logs/2005.04.06.txt
/data/logs/2005.04.07.txt
/data/logs/2005.04.08.txt
/data/logs/2005.04.09.txt
/data/logs/2005.04.10.txt
/data/logs/2005.04.11.txt
/data/logs/2005.04.12.txt
/data/logs/2005.04.13.txt
/data/logs/2005.04.14.txt
/data/logs/2005.04.15.txt
/data/logs/2005.04.16.txt
/data/logs/2005.04.17.txt
/data/logs/2005.04.18.txt
/data/logs/2005.04.19.txt
/data/logs/2005.04.20.txt
/data/logs/2005.04.21.txt
/data/logs/2005.04.22.txt
/data/logs/2005.04.23.txt
/data/logs/2005.04.24.txt
/data/logs/2005.04.25.txt
/data/logs/2005.04.26.txt
/data/logs/2005.04.27.txt
/data/logs/2005.04.28.txt
/data/logs/2005.04.29.txt
/data/logs/2005.04.30.txt
/data/logs/2005.05.01.txt
/data/logs/2005.05.02.txt
/data/logs/2005.05.03.txt
/data/logs/2005.05.04.txt
/data/logs/2005.05.05.txt
/data/logs/2005.05.06.txt
/data/logs/2005.05.07.txt
/data/logs/2005.05.08.txt
/data/logs/2005.05.09.txt
/data/logs/2005.05.10.txt
/data/logs/2005.05.11.txt
/data/logs/2005.05.12.txt
/data/logs/2005.05.13.txt
/data/logs/2005.05.14.txt
/data/logs/2005.05.15.txt
/data/logs/2005.05.16.txt
/data/logs/2005.05.17.txt
/data/logs/2005.05.18.txt
/data/logs/2005.05.19.txt
/data/logs/2005.05.20.txt
/data/logs/2005.05.21.txt
/data/logs/2005.05.22.txt
/data/logs/2005.05.23.txt
/data/logs/2005.05.24.txt
/data/logs/2005.05.25.txt
/data/logs/2005.05.26.txt
/data/logs/2005.05.27.txt
/data/logs/2005.05.28.txt
/data/logs/2005.05.29.txt
/data/logs/2005.05.30.txt
/data/logs/2005.05.31.txt
/data/logs/2005.06.01.txt
/data/logs/2005.06.02.txt
/data/logs/2005.06.03.txt
/data/logs/2005.06.04.txt
/data/logs/2005.06.05.txt
/data/logs/2005.06.06.txt
/data/logs/2005.06.07.txt
/data/logs/2005.06.08.txt
/data/logs/2005.06.09.txt
/data/logs/2005.06.10.txt
/data/logs/2005.06.11.txt
/data/logs/2005.06.12.txt
/data/logs/2005.06.13.txt
/data/logs/2005.06.14.txt
/data/logs/2005.06.15.txt
/data/logs/2005.06.16.txt
/data/logs/2005.06.17.txt
/data/logs/2005.06.18.txt
/data/logs/2005.06.19.txt
/data/logs/2005.06.20.txt
/data/logs/2005.06.21.txt
/data/logs/2005.06.22.txt
/data/logs/2005.06.23.txt
/data/logs/2005.06.24.txt
/data/logs/2005.06.25.txt
/data/logs/2005.06.26.txt
/data/logs/2005.06.27.txt
/data/logs/2005.06.28.txt
/data/logs/2005.06.29.txt
/data/logs/2005.06.30.txt
/data/logs/2005.07.01.txt
/data/logs/2005.07.02.txt
/data/logs/2005.07.03.txt
/data/logs/2005.07.04.txt
/data/logs/2005.07.05.txt
/data/logs/2005.07.06.txt
/data/logs/2005.07.07.txt
/data/logs/2005.07.08.txt
/data/logs/2005.07.09.txt
/data/logs/2005.07.10.txt
/data/logs/2005.07.11.txt
/data/logs/2005.07.12.txt
/data/logs/2005.07.13.txt
/data/logs/2005.07.14.txt
/data/logs/2005.07.15.txt
/data/logs/2005.07.16.txt
/data/logs/2005.07.17.txt
/data/logs/2005.07.18.txt
/data/logs/2005.07.19.txt
/data/logs/2005.07.20.txt
/data/logs/2005.07.21.txt
/data/logs/2005.07.22.txt
/data/logs/2005.07.23.txt
/data/logs/2005.07.24.txt
/data/logs/2005.07.25.txt
/data/logs/2005.07.26.txt
/data/logs/2005.07.27.txt
/data/logs/2005.07.28.txt
/data/logs/2005.07.29.txt
/data/logs/2005.07.30.txt
/data/logs/2005.07.31.txt
/data/logs/2005.08.01.txt
/data/logs/2005.08.02.txt
/data/logs/2005.08.03.txt
/data/logs/2005.08.04.txt
/data/logs/2005.08.05.txt
/data/logs/2005.08.06.txt
/data/logs/2005.08.07.txt
/data/logs/2005.08.08.txt
/data/logs/2005.08.09.txt
/data/logs/2005.08.10.txt
/data/logs/2005.08.11.txt
/data/logs/2005.08.12.txt
/data/logs/2005.08.13.txt
/data/logs/2005.08.14.txt
/data/logs/2005.08.15.txt
/data/logs/2005.08.16.txt
/data/logs/2005.08.17.txt
/data/logs/2005.08.18.txt
/data/logs/2005.08.19.txt
/data/logs/2005.08.20.txt
/data/logs/2005.08.21.txt
/data/logs/2005.08.22.txt
/data/logs/2005.08.23.txt
/data/logs/2005.08.24.txt
/data/logs/2005.08.25.txt
/data/logs/2005.08.26.txt
/data/logs/2005.08.27.txt
/data/logs/2005.08.28.txt
/data/logs/2005.08.29.txt
/data/logs/2005.08.30.txt
/data/logs/2005.08.31.txt
/data/logs/2005.09.01.txt
/data/logs/2005.09.02.txt
/data/logs/2005.09.03.txt
/data/logs/2005.09.04.txt
/data/logs/2005.09.05.txt
/data/logs/2005.09.06.txt
/data/logs/2005.09.07.txt
/data/logs/2005.09.08.txt
/data/logs/2005.09.09.txt
/data/logs/2005.09.10.txt
/data/logs/2005.09.11.txt
/data/logs/2005.09.12.txt
/data/logs/2005.09.13.txt
/data/logs/2005.09.14.txt
/data/logs/2005.09.15.txt
/data/logs/2005.09.16.txt
/data/logs/2005.09.17.txt
/data/logs/2005.09.18.txt
/data/logs/2005.09.19.txt
/data/logs/2005.09.20.txt
/data/logs/2005.09.21.txt
/data/logs/2005.09.22.txt
/data/logs/2005.09.23.txt
/data/logs/2005.09.24.txt
/data/logs/2005.09.25.txt
/data/logs/2005.09.26.txt
/data/logs/2005.09.27.txt
/data/logs/2005.09.28.txt
/data/logs/2005.09.29.txt
/data/logs/2005.09.30.txt
/data/logs/2005.10.01.txt
/data/logs/2005.10.02.txt
/data/logs/2005.10.03.txt
/data/logs/2005.10.04.txt
/data/logs/2005.10.05.txt
/data/logs/2005.10.06.txt
/data/logs/2005.10.07.txt
/data/logs/2005.10.08.txt
/data/logs/2005.10.09.txt
/data/logs/2005.10.10.txt
/data/logs/2005.10.11.txt
/data/logs/2005.10.12.txt
/data/logs/2005.10.13.txt
/data/logs/2005.10.14.txt
/data/logs/2005.10.15.txt
/data/logs/2005.10.16.txt
/data/logs/2005.10.17.txt
/data/logs/2005.10.18.txt
/data/logs/2005.10.19.txt
/data/logs/2005.10.20.txt
/data/logs/2005.10.21.txt
/data/logs/2005.10.22.txt
/data/logs/2005.10.23.txt
/data/logs/2005.10.24.txt
/data/logs/2005.10.25.txt
/data/logs/2005.10.26.txt
/data/logs/2005.10.27.txt
/data/logs/2005.10.28.txt
/data/logs/2005.10.29.txt
/data/logs/2005.10.30.txt
/data/logs/2005.10.31.txt
/data/logs/2005.11.01.txt
/data/logs/2005.11.02.txt
/data/logs/2005.11.03.txt
/data/logs/2005.11.04.txt
/data/logs/2005.11.05.txt
/data/logs/2005.11.06.txt
/data/logs/2005.11.07.txt
/data/logs/2005.11.08.txt
/data/logs/2005.11.09.txt
/data/logs/2005.11.10.txt
/data/logs/2005.11.11.txt
/data/logs/2005.11.12.txt
/data/logs/2005.11.13.txt
/data/logs/2005.11.14.txt
/data/logs/2005.11.15.txt
/data/logs/2005.11.16.txt
/data/logs/2005.11.17.txt
/data/logs/2005.11.18.txt
/data/logs/2005.11.19.txt
/data/logs/2005.11.20.txt
/data/logs/2005.11.21.txt
/data/logs/2005.11.22.txt
/data/logs/2005.11.23.txt
/data/logs/2005.11.24.txt
/data/logs/2005.11.25.txt
/data/logs/2005.11.26.txt
/data/logs/2005.11.27.txt
/data/logs/2005.11.28.txt
/data/logs/2005.11.29.txt
/data/logs/2005.11.30.txt
/data/logs/2005.12.01.txt
/data/logs/2005.12.02.txt
/data/logs/2005.12.03.txt
/data/logs/2005.12.04.txt
/data/logs/2005.12.05.txt
/data/logs/2005.12.06.txt
/data/logs/2005.12.07.txt
/data/logs/2005.12.08.txt
/data/logs/2005.12.09.txt
/data/logs/2005.12.10.txt
/data/logs/2005.12.11.txt
/data/logs/2005.12.12.txt
/data/logs/2005.12.13.txt
/data/logs/2005.12.14.txt
/data/logs/2005.12.15.txt
/data/logs/2005.12.16.txt
/data/logs/2005.12.17.txt
/data/logs/2005.12.18.txt
/data/logs/2005.12.19.txt
/data/logs/2005.12.20.txt
/data/logs/2005.12.21.txt
/data/logs/2005.12.22.txt
/data/logs/2005.12.23.txt
/data/logs/2005.12.24.txt
/data/logs/2005.12.25.txt
/data/logs/2005.12.26.txt
/data/logs/2005.12.27.txt
/data/logs/2005.12.28.txt
/data/logs/2005.12.29.txt
/data/logs/2005.12.30.txt
/data/logs/2005.12.31.txt
/data/logs/2006.01.01.txt
/data/logs/2006.01.02.txt
/data/logs/2006.01.03.txt
/data/logs/2006.01.04.txt
/data/logs/2006.01.05.txt
/data/logs/2006.01.06.txt
/data/logs/2006.01.07.txt
/data/logs/2006.01.08.txt
/data/logs/2006.01.09.txt
/data/logs/2006.01.10.txt
/data/logs/2006.01.11.txt
/data/logs/2006.01.12.txt
/data/logs/2006.01.13.txt
/data/logs/2006.01.14.txt
/data/logs/2006.01.15.txt
/data/logs/2006.01.16.txt
/data/logs/2006.01.17.txt
/data/logs/2006.01.18.txt
/data/logs/2006.01.19.txt
/data/logs/2006.01.20.txt
/data/logs/2006.01.21.txt
/data/logs/2006.01.22.txt
/data/logs/2006.01.23.txt
/data/logs/2006.01.24.txt
/data/logs/2006.01.25.txt
/data/logs/2006.01.26.txt
/data/logs/2006.01.27.txt
/data/logs/2006.01.28.txt
/data/logs/2006.01.29.txt
/data/logs/2006.01.30.txt
/data/logs/2006.01.31.txt
/data/logs/2006.02.01.txt
/data/logs/2006.02.02.txt
/data/logs/2006.02.03.txt
/data/logs/2006.02.04.txt
/data/logs/2006.02.05.txt
/data/logs/2006.02.06.txt
/data/logs/2006.02.07.txt
/data/logs/2006.02.08.txt
/data/logs/2006.02.09.txt
/data/logs/2006.02.10.txt
/data/logs/2006.02.11.txt
/data/logs/2006.02.12.txt
/data/logs/2006.02.13.txt
/data/logs/2006.02.14.txt
/data/logs/2006.02.15.txt
/data/logs/2006.02.16.txt
/data/logs/2006.02.17.txt
/data/logs/2006.02.18.txt
/data/logs/2006.02.19.txt
/data/logs/2006.02.20.txt
/data/logs/2006.02.21.txt
/data/logs/2006.02.22.txt
/data/logs/2006.02.23.txt
/data/logs/2006.02.24.txt
/data/logs/2006.02.25.txt
/data/logs/2006.02.26.txt
/data/logs/2006.02.27.txt
/data/logs/2006.02.28.txt
/data/logs/2006.03.01.txt
/data/logs/2006.03.02.txt
/data/logs/2006.03.03.txt
/data/logs/2006.03.04.txt
/data/logs/2006.03.05.txt
/data/logs/2006.03.06.txt
/data/logs/2006.03.07.txt
/data/logs/2006.03.08.txt
/data/logs/2006.03.09.txt
/data/logs/2006.03.10.txt
/data/logs/2006.03.11.txt
/data/logs/2006.03.12.txt
/data/logs/2006.03.13.txt
/data/logs/2006.03.14.txt
/data/logs/2006.03.15.txt
/data/logs/2006.03.16.txt
/data/logs/2006.03.17.txt
/data/logs/2006.03.18.txt
/data/logs/2006.03.19.txt
/data/logs/2006.03.20.txt
/data/logs/2006.03.21.txt
/data/logs/2006.03.22.txt
/data/logs/2006.03.23.txt
/data/logs/2006.03.24.txt
/data/logs/2006.03.25.txt
/data/logs/2006.03.26.txt
/data/logs/2006.03.27.txt
/data/logs/2006.03.28.txt
/data/logs/2006.03.29.txt
/data/logs/2006.03.30.txt
/data/logs/2006.03.31.txt
/data/logs/2006.04.01.txt
/data/logs/2006.04.02.txt
/data/logs/2006.04.03.txt
/data/logs/2006.04.04.txt
/data/logs/2006.04.05.txt
/data/logs/2006.04.06.txt
/data/logs/2006.04.07.txt
/data/logs/2006.04.08.txt
/data/logs/2006.04.09.txt
/data/logs/2006.04.10.txt
/data/logs/2006.04.11.txt
/data/logs/2006.04.12.txt
/data/logs/2006.04.13.txt
/data/logs/2006.04.14.txt
/data/logs/2006.04.15.txt
/data/logs/2006.04.16.txt
/data/logs/2006.04.17.txt
/data/logs/2006.04.18.txt
/data/logs/2006.04.19.txt
/data/logs/2006.04.20.txt
/data/logs/2006.04.21.txt
/data/logs/2006.04.22.txt
/data/logs/2006.04.23.txt
/data/logs/2006.04.24.txt
/data/logs/2006.04.25.txt
/data/logs/2006.04.26.txt
/data/logs/2006.04.27.txt
/data/logs/2006.04.28.txt
/data/logs/2006.04.29.txt
/data/logs/2006.04.30.txt
/data/logs/2006.05.01.txt
/data/logs/2006.05.02.txt
/data/logs/2006.05.03.txt
/data/logs/2006.05.04.txt
/data/logs/2006.05.05.txt
/data/logs/2006.05.06.txt
/data/logs/2006.05.07.txt
/data/logs/2006.05.08.txt
/data/logs/2006.05.09.txt
/data/logs/2006.05.10.txt
/data/logs/2006.05.11.txt
/data/logs/2006.05.12.txt
/data/logs/2006.05.13.txt
/data/logs/2006.05.14.txt
/data/logs/2006.05.15.txt
/data/logs/2006.05.16.txt
/data/logs/2006.05.17.txt
/data/logs/2006.05.18.txt
/data/logs/2006.05.19.txt
/data/logs/2006.05.20.txt
/data/logs/2006.05.21.txt
/data/logs/2006.05.22.txt
/data/logs/2006.05.23.txt
/data/logs/2006.05.24.txt
/data/logs/2006.05.25.txt
/data/logs/2006.05.26.txt
/data/logs/2006.05.27.txt
/data/logs/2006.05.28.txt
/data/logs/2006.05.29.txt
/data/logs/2006.05.30.txt
/data/logs/2006.05.31.txt
/data/logs/2006.06.01.txt
/data/logs/2006.06.02.txt
/data/logs/2006.06.03.txt
/data/logs/2006.06.04.txt
/data/logs/2006.06.05.txt
/data/logs/2006.06.06.txt
/data/logs/2006.06.07.txt
/data/logs/2006.06.08.txt
/data/logs/2006.06.09.txt
/data/logs/2006.06.10.txt
/data/logs/2006.06.11.txt
/data/logs/2006.06.12.txt
/data/logs/2006.06.13.txt
/data/logs/2006.06.14.txt
/data/logs/2006.06.15.txt
/data/logs/2006.06.16.txt
/data/logs/2006.06.17.txt
/data/logs/2006.06.18.txt
/data/logs/2006.06.19.txt
/data/logs/2006.06.20.txt
/data/logs/2006.06.21.txt
/data/logs/2006.06.22.txt
/data/logs/2006.06.23.txt
/data/logs/2006.06.24.txt
/data/logs/2006.06.25.txt
/data/logs/2006.06.26.txt
/data/logs/2006.06.27.txt
/data/logs/2006.06.28.txt
/data/logs/2006.06.29.txt
/data/logs/2006.06.30.txt
/data/logs/2006.07.01.txt
/data/logs/2006.07.02.txt
/data/logs/2006.07.03.txt
/data/logs/2006.07.04.txt
/data/logs/2006.07.05.txt
/data/logs/2006.07.06.txt
/data/logs/2006.07.07.txt
/data/logs/2006.07.08.txt
/data/logs/2006.07.09.txt
/data/logs/2006.07.10.txt
/data/logs/2006.07.11.txt
/data/logs/2006.07.12.txt
/data/logs/2006.07.13.txt
/data/logs/2006.07.14.txt
/data/logs/2006.07.15.txt
/data/logs/2006.07.16.txt
/data/logs/2006.07.17.txt
/data/logs/2006.07.18.txt
/data/logs/2006.07.19.txt
/data/logs/2006.07.20.txt
/data/logs/2006.07.21.txt
/data/logs/2006.07.22.txt
/data/logs/2006.07.23.txt
/data/logs/2006.07.24.txt
/data/logs/2006.07.25.txt
/data/logs/2006.07.26.txt
/data/logs/2006.07.27.txt
/data/logs/2006.07.28.txt
/data/logs/2006.07.29.txt
/data/logs/2006.07.30.txt
/data/logs/2006.07.31.txt
/data/logs/2006.08.01.txt
/data/logs/2006.08.02.txt
/data/logs/2006.08.03.txt
/data/logs/2006.08.04.txt
/data/logs/2006.08.05.txt
/data/logs/2006.08.06.txt
/data/logs/2006.08.07.txt
/data/logs/2006.08.08.txt
/data/logs/2006.08.09.txt
/data/logs/2006.08.10.txt
/data/logs/2006.08.11.txt
/data/logs/2006.08.12.txt
/data/logs/2006.08.13.txt
/data/logs/2006.08.14.txt
/data/logs/2006.08.15.txt
/data/logs/2006.08.16.txt
/data/logs/2006.08.17.txt
/data/logs/2006.08.18.txt
/data/logs/2006.08.19.txt
/data/logs/2006.08.20.txt
/data/logs/2006.08.21.txt
/data/logs/2006.08.22.txt
/data/logs/2006.08.23.txt
/data/logs/2006.08.24.txt
/data/logs/2006.08.25.txt
/data/logs/2006.08.26.txt
/data/logs/2006.08.27.txt
/data/logs/2006.08.28.txt
/data/logs/2006.08.29.txt
/data/logs/2006.08.30.txt
/data/logs/2006.08.31.txt
/data/logs/2006.09.01.txt
/data/logs/2006.09.02.txt
/data/logs/2006.09.03.txt
/data/logs/2006.09.04.txt
/data/logs/2006.09.05.txt
/data/logs/2006.09.06.txt
/data/logs/2006.09.07.txt
/data/logs/2006.09.08.txt
/data/logs/2006.09.09.txt
/data/logs/2006.09.10.txt
/data/logs/2006.09.11.txt
/data/logs/2006.09.12.txt
/data/logs/2006.09.13.txt
/data/logs/2006.09.14.txt
/data/logs/2006.09.15.txt
/data/logs/2006.09.16.txt
/data/logs/2006.09.17.txt
/data/logs/2006.09.18.txt
/data/logs/2006.09.19.txt
/data/logs/2006.09.20.txt
/data/logs/2006.09.21.txt
/data/logs/2006.09.22.txt
/data/logs/2006.09.23.txt
/data/logs/2006.09.24.txt
/data/logs/2006.09.25.txt
/data/logs/2006.09.26.txt
/data/logs/2006.09.27.txt
/data/logs/2006.09.28.txt
/data/logs/2006.09.29.txt
/data/logs/2006.09.30.txt
/data/logs/2006.10.01.txt
/data/logs/2006.10.02.txt
/data/logs/2006.10.03.txt
/data/logs/2006.10.04.txt
/data/logs/2006.10.05.txt
/data/logs/2006.10.06.txt
/data/logs/2006.10.07.txt
/data/logs/2006.10.08.txt
/data/logs/2006.10.09.txt
/data/logs/2006.10.10.txt
/data/logs/2006.10.11.txt
/data/logs/2006.10.12.txt
/data/logs/2006.10.13.txt
/data/logs/2006.10.14.txt
/data/logs/2006.10.15.txt
/data/logs/2006.10.16.txt
/data/logs/2006.10.17.txt
/data/logs/2006.10.18.txt
/data/logs/2006.10.19.txt
/data/logs/2006.10.20.txt
/data/logs/2006.10.21.txt
/data/logs/2006.10.22.txt
/data/logs/2006.10.23.txt
/data/logs/2006.10.24.txt
/data/logs/2006.10.25.txt
/data/logs/2006.10.26.txt
/data/logs/2006.10.27.txt
/data/logs/2006.10.28.txt
/data/logs/2006.10.29.txt
/data/logs/2006.10.30.txt
/data/logs/2006.10.31.txt
/data/logs/2006.11.01.txt
/data/logs/2006.11.02.txt
/data/logs/2006.11.03.txt
/data/logs/2006.11.04.txt
/data/logs/2006.11.05.txt
/data/logs/2006.11.06.txt
/data/logs/2006.11.07.txt
/data/logs/2006.11.08.txt
/data/logs/2006.11.09.txt
/data/logs/2006.11.10.txt
/data/logs/2006.11.11.txt
/data/logs/2006.11.12.txt
/data/logs/2006.11.13.txt
/data/logs/2006.11.14.txt
/data/logs/2006.11.15.txt
/data/logs/2006.11.16.txt
/data/logs/2006.11.17.txt
/data/logs/2006.11.18.txt
/data/logs/2006.11.19.txt
/data/logs/2006.11.20.txt
/data/logs/2006.11.21.txt
/data/logs/2006.11.22.txt
/data/logs/2006.11.23.txt
/data/logs/2006.11.24.txt
/data/logs/2006.11.25.txt
/data/logs/2006.11.26.txt
/data/logs/2006.11.27.txt
/data/logs/2006.11.28.txt
/data/logs/2006.11.29.txt
/data/logs/2006.11.30.txt
/data/logs/2006.12.01.txt
/data/logs/2006.12.02.txt
/data/logs/2006.12.03.txt
/data/logs/2006.12.04.txt
/data/logs/2006.12.05.txt
/data/logs/2006.12.06.txt
/data/logs/2006.12.07.txt
/data/logs/2006.12.08.txt
/data/logs/2006.12.09.txt
/data/logs/2006.12.10.txt
/data/logs/2006.12.11.txt
/data/logs/2006.12.12.txt
/data/logs/2006.12.13.txt
/data/logs/2006.12.14.txt
/data/logs/2006.12.15.txt
/data/logs/2006.12.16.txt
/data/logs/2006.12.17.txt
/data/logs/2006.12.18.txt
/data/logs/2006.12.19.txt
/data/logs/2006.12.20.txt
/data/logs/2006.12.21.txt
/data/logs/2006.12.22.txt
/data/logs/2006.12.23.txt
/data/logs/2006.12.24.txt
/data/logs/2006.12.25.txt
/data/logs/2006.12.26.txt
/data/logs/2006.12.27.txt
/data/logs/2006.12.28.txt
/data/logs/2006.12.29.txt
/data/logs/2006.12.30.txt
/data/logs/2006.12.31.txt
/data/logs/2007.01.01.txt
/data/logs/2007.01.02.txt
/data/logs/2007.01.03.txt
/data/logs/2007.01.04.txt
/data/logs/2007.01.05.txt
/data/logs/2007.01.06.txt
/data/logs/2007.01.07.txt
/data/logs/2007.01.08.txt
/data/logs/2007.01.09.txt
/data/logs/2007.01.10.txt
/data/logs/2007.01.11.txt
/data/logs/2007.01.12.txt
/data/logs/2007.01.13.txt
/data/logs/2007.01.14.txt
/data/logs/2007.01.15.txt
/data/logs/2007.01.16.txt
/data/logs/2007.01.17.txt
/data/logs/2007.01.18.txt
/data/logs/2007.01.19.txt
/data/logs/2007.01.20.txt
/data/logs/2007.01.21.txt
/data/logs/2007.01.22.txt
/data/logs/2007.01.23.txt
/data/logs/2007.01.24.txt
/data/logs/2007.01.25.txt
/data/logs/2008.01.01.txt
/data/logs/2008.01.02.txt
/data/logs/2008.01.03.txt
/data/logs/2008.01.04.txt
/data/logs/2008.01.05.txt
/data/logs/2008.01.06.txt
/data/logs/2008.01.07.txt
/data/logs/2008.01.08.txt
/data/logs/2008.01.09.txt
/data/logs/2008.01.10.txt
/data/logs/2008.01.11.txt
/data/logs/2008.01.12.txt
/data/logs/2008.01.13.txt
/data/logs/2008.01.14.txt
/data/logs/2008.01.15.txt
/data/logs/2008.01.16.txt
/data/logs/2008.01.17.txt
/data/logs/2008.01.18.txt
/data/logs/2008.01.19.txt
/data/logs/2008.01.20.txt
/data/logs/2008.01.21.txt
/data/logs/2008.01.22.txt
/data/logs/2008.01.23.txt
/data/logs/2008.01.24.txt
/data/logs/2008.01.25.txt
/data/logs/2008.01.26.txt
/data/logs/2008.01.27.txt
/data/logs/2008.01.28.txt
/data/logs/2008.01.29.txt
/data/logs/2008.01.30.txt
/data/logs/2008.01.31.txt
/data/logs/2008.02.01.txt
/data/logs/2008.02.02.txt
/data/logs/2008.02.03.txt
/data/logs/2008.02.04.txt
/data/logs/2008.02.05.txt
/data/logs/2008.02.06.txt
/data/logs/2008.02.07.txt
/data/logs/2008.02.08.txt
/data/logs/2008.02.09.txt
/data/logs/2008.02.10.txt
/data/logs/2008.02.11.txt
/data/logs/2008.02.12.txt
/data/logs/2008.02.13.txt
/data/logs/2008.02.14.txt
/data/logs/2008.02.15.txt
/data/logs/2008.02.16.txt
/data/logs/2008.02.17.txt
/data/logs/2008.02.18.txt
/data/logs/2008.02.19.txt
/data/logs/2008.02.20.txt
/data/logs/2008.02.21.txt
/data/logs/2008.02.22.txt
/data/logs/2008.02.23.txt
/data/logs/2008.02.24.txt
/data/logs/2008.02.25.txt
/data/logs/2008.02.26.txt
/data/logs/2008.02.27.txt
/data/logs/2008.02.28.txt
/data/logs/2008.02.29.txt
/data/logs/2008.03.01.txt
/data/logs/2008.03.02.txt
/data/logs/2008.03.03.txt
/data/logs/2008.03.04.txt
/data/logs/2008.03.05.txt
/data/logs/2008.03.06.txt
/data/logs/2008.03.07.txt
/data/logs/2008.03.08.txt
/data/logs/2008.03.09.txt
/data/logs/2008.03.10.txt
/data/logs/2008.03.11.txt
/data/logs/2008.03.12.txt
/data/logs/2008.03.13.txt
/data/logs/2008.03.14.txt
/data/logs/2008.03.15.txt
/data/logs/2008.03.16.txt
/data/logs/2008.03.17.txt
/data/logs/2008.03.18.txt
/data/logs/2008.03.19.txt
/data/logs/2008.03.20.txt
/data/logs/2008.03.21.txt
/data/logs/2008.03.22.txt
/data/logs/2008.03.23.txt
/data/logs/2008.03.24.txt
/data/logs/2008.03.25.txt
/data/logs/2008.03.26.txt
/data/logs/2008.03.27.txt
/data/logs/2008.03.28.txt
/data/logs/2008.03.29.txt
/data/logs/2008.03.30.txt
/data/logs/2008.03.31.txt
/data/logs/2008.04.01.txt
/data/logs/2008.04.02.txt
/data/logs/2008.04.03.txt
/data/logs/2008.04.04.txt
/data/logs/2008.04.05.txt
/data/logs/2008.04.06.txt
/data/logs/2008.04.07.txt
/data/logs/2008.04.08.txt
/data/logs/2008.04.09.txt
/data/logs/2008.04.10.txt
/data/logs/2008.04.11.txt
/data/logs/2008.04.12.txt
/data/logs/2008.04.13.txt
/data/logs/2008.04.14.txt
/data/logs/2008.04.15.txt
/data/logs/2008.04.16.txt
/data/logs/2008.04.17.txt
/data/logs/2008.04.18.txt
/data/logs/2008.04.19.txt
/data/logs/2008.04.20.txt
/data/logs/2008.04.21.txt
/data/logs/2008.04.22.txt
/data/logs/2008.04.23.txt
/data/logs/2008.04.24.txt
/data/logs/2008.04.25.txt
/data/logs/2008.04.26.txt
/data/logs/2008.04.27.txt
/data/logs/2008.04.28.txt
/data/logs/2008.04.29.txt
/data/logs/2008.04.30.txt
/data/logs/2008.05.01.txt
/data/logs/2008.05.02.txt
/data/logs/2008.05.03.txt
/data/logs/2008.05.04.txt
/data/logs/2008.05.05.txt
/data/logs/2008.05.06.txt
/data/logs/2008.05.07.txt
/data/logs/2008.05.08.txt
/data/logs/2008.05.09.txt
/data/logs/2008.05.10.txt
/data/logs/2008.05.11.txt
/data/logs/2008.05.12.txt
/data/logs/2008.05.13.txt
/data/logs/2008.05.14.txt
/data/logs/2008.05.15.txt
/data/logs/2008.05.16.txt
/data/logs/2008.05.17.txt
/data/logs/2008.05.18.txt
/data/logs/2008.05.19.txt
/data/logs/2008.05.20.txt
/data/logs/2008.05.21.txt
/data/logs/2008.05.22.txt
/data/logs/2008.05.23.txt
/data/logs/2008.05.24.txt
/data/logs/2008.05.25.txt
/data/logs/2008.05.26.txt
/data/logs/2008.05.27.txt
/data/logs/2008.05.28.txt
/data/logs/2008.05.29.txt
/data/logs/2008.05.30.txt
/data/logs/2008.05.31.txt
/data/logs/2008.06.01.txt
/data/logs/2008.06.02.txt
/data/logs/2008.06.03.txt
/data/logs/2008.06.04.txt
/data/logs/2008.06.05.txt
/data/logs/2008.06.06.txt
/data/logs/2008.06.07.txt
/data/logs/2008.06.08.txt
/data/logs/2008.06.09.txt
/data/logs/2008.06.10.txt
/data/logs/2008.06.11.txt
/data/logs/2008.06.12.txt
/data/logs/2008.06.13.txt
/data/logs/2008.06.14.txt
/data/logs/2008.06.15.txt
/data/logs/2008.06.16.txt
/data/logs/2008.06.17.txt
/data/logs/2008.06.18.txt
/data/logs/2008.06.19.txt
/data/logs/2008.06.20.txt
/data/logs/2008.06.21.txt
/data/logs/2008.06.22.txt
/data/logs/2008.06.23.txt
/data/logs/2008.06.24.txt
/data/logs/2008.06.25.txt
/data/logs/2008.06.26.txt
/data/logs/2008.06.27.txt
/data/logs/2008.06.28.txt
/data/logs/2008.06.29.txt
/data/logs/2008.06.30.txt
/data/logs/2008.07.01.txt
/data/logs/2008.07.02.txt
/data/logs/2008.07.03.txt
/data/logs/2008.07.04.txt
/data/logs/2008.07.05.txt
/data/logs/2008.07.06.txt
/data/logs/2008.07.07.txt
/data/logs/2008.07.08.txt
/data/logs/2008.07.09.txt
/data/logs/2008.07.10.txt
/data/logs/2008.07.11.txt
/data/logs/2008.07.12.txt
/data/logs/2008.07.13.txt
/data/logs/2008.07.14.txt
/data/logs/2008.07.15.txt
/data/logs/2008.07.16.txt
/data/logs/2008.07.17.txt
/data/logs/2008.07.18.txt
/data/logs/2008.07.19.txt
/data/logs/2008.07.20.txt
/data/logs/2008.07.21.txt
/data/logs/2008.07.22.txt
/data/logs/2008.07.23.txt
/data/logs/2008.07.24.txt
/data/logs/2008.07.25.txt
/data/logs/2008.07.26.txt
/data/logs/2008.07.27.txt
/data/logs/2008.07.28.txt
/data/logs/2008.07.29.txt
/data/logs/2008.07.31.txt
/data/logs/2008.08.01.txt
/data/logs/2008.08.02.txt
/data/logs/2010.04.02.txt
/data/logs/2010.04.03.txt
/data/logs/2010.04.04.txt
/data/logs/2010.04.05.txt
/data/logs/2010.04.06.txt
/data/logs/2010.04.07.txt
/data/logs/2010.04.08.txt
/data/logs/2010.04.09.txt
/data/logs/2010.04.10.txt
/data/logs/2010.04.11.txt
/data/logs/2010.04.12.txt
/data/logs/2010.04.13.txt
/data/logs/2010.04.14.txt
/data/logs/2010.04.15.txt
/data/logs/2010.04.16.txt
/data/logs/2010.04.17.txt
/data/logs/2010.04.18.txt
/data/logs/2010.04.19.txt
/data/logs/2010.04.20.txt
/data/logs/2010.04.21.txt
/data/logs/2010.04.22.txt
/data/logs/2010.04.23.txt
/data/logs/2010.04.24.txt
/data/logs/2010.04.25.txt
/data/logs/2010.04.26.txt
/data/logs/2010.04.27.txt
/data/logs/2010.04.28.txt
/data/logs/2010.04.29.txt
/data/logs/2010.04.30.txt
/data/logs/2010.05.01.txt
/data/logs/2010.05.02.txt
/data/logs/2010.05.03.txt
/data/logs/2010.05.04.txt
/data/logs/2010.05.05.txt
/data/logs/2010.05.06.txt
/data/logs/2010.05.07.txt
/data/logs/2010.05.08.txt
/data/logs/2010.05.09.txt
/data/logs/2010.05.10.txt
/data/logs/2010.05.11.txt
/data/logs/2010.05.12.txt
/data/logs/2010.05.13.txt
/data/logs/2010.05.14.txt
/data/logs/2010.05.15.txt
/data/logs/2010.05.16.txt
/data/logs/2010.05.17.txt
/data/logs/2010.05.18.txt
/data/logs/2010.05.19.txt
/data/logs/2010.05.20.txt
/data/logs/2010.05.21.txt
/data/logs/2010.05.22.txt
/data/logs/2010.05.23.txt
/data/logs/2010.05.24.txt
/data/logs/2010.05.25.txt
/data/logs/2010.05.26.txt
/data/logs/2010.05.27.txt
/data/logs/2010.05.28.txt
/data/logs/2010.05.29.txt
/data/logs/2010.05.30.txt
/data/logs/2010.05.31.txt
/data/logs/2010.06.01.txt
/data/logs/2010.06.02.txt
/data/logs/2010.06.03.txt
/data/logs/2010.06.04.txt
/data/logs/2010.06.05.txt
/data/logs/2010.06.06.txt
/data/logs/2010.06.07.txt
/data/logs/2010.06.08.txt
/data/logs/2010.06.09.txt
/data/logs/2010.06.10.txt
/data/logs/2010.06.11.txt
/data/logs/2010.06.12.txt
/data/logs/2010.06.13.txt
/data/logs/2010.06.14.txt
/data/logs/2010.06.15.txt
/data/logs/2010.06.16.txt
/data/logs/2010.06.17.txt
/data/logs/2010.06.18.txt
/data/logs/2010.06.19.txt
/data/logs/2010.06.20.txt
/data/logs/2010.06.21.txt
/data/logs/2010.06.22.txt
/data/logs/2010.06.23.txt
/data/logs/2010.06.24.txt
/data/logs/2010.06.25.txt
/data/logs/2010.06.26.txt
/data/logs/2010.06.27.txt
/data/logs/2010.06.28.txt
/data/logs/2010.06.29.txt
/data/logs/2010.06.30.txt
/data/logs/2010.07.01.txt
/data/logs/2010.07.02.txt
/data/logs/2010.07.03.txt
/data/logs/2010.07.04.txt
/data/logs/2010.07.05.txt
/data/logs/2010.07.06.txt
/data/logs/2010.07.07.txt
/data/logs/2010.07.08.txt
/data/logs/2010.07.09.txt
/data/logs/2010.07.10.txt
/data/logs/2010.07.11.txt
/data/logs/2010.07.12.txt
/data/logs/2010.07.13.txt
/data/logs/2010.07.14.txt
/data/logs/2010.07.15.txt
/data/logs/2010.07.16.txt
/data/logs/2010.07.17.txt
/data/logs/2010.07.18.txt
/data/logs/2010.07.19.txt
/data/logs/2010.07.20.txt
/data/logs/2010.07.21.txt
/data/logs/2010.07.22.txt
/data/logs/2010.07.23.txt
/data/logs/2010.07.24.txt
/data/logs/2010.07.25.txt
/data/logs/2010.07.26.txt
/data/logs/2010.07.27.txt
/data/logs/2010.07.28.txt
/data/logs/2010.07.29.txt
/data/logs/2010.07.30.txt
/data/logs/2010.07.31.txt
/data/logs/2010.08.01.txt
/data/logs/2010.08.02.txt
/data/logs/2010.08.03.txt
/data/logs/2010.08.04.txt
/data/logs/2010.08.05.txt
/data/logs/2010.08.06.txt
/data/logs/2010.08.07.txt
/data/logs/2010.08.08.txt
/data/logs/2010.08.09.txt
/data/logs/2010.08.10.txt
/data/logs/2010.08.11.txt
/data/logs/2010.08.12.txt
/data/logs/2010.08.13.txt
/data/logs/2010.08.14.txt
/data/logs/2010.08.15.txt
/data/logs/2010.08.16.txt
/data/logs/2010.08.17.txt
/data/logs/2010.08.18.txt
/data/logs/2010.08.19.txt
/data/logs/2010.08.20.txt
/data/logs/2010.08.21.txt
/data/logs/2010.08.22.txt
/data/logs/2010.08.23.txt
/data/logs/2010.08.24.txt
/data/logs/2010.08.25.txt
/data/logs/2010.08.26.txt
/data/logs/2010.08.27.txt
/data/logs/2010.08.28.txt
/data/logs/2010.08.29.txt
/data/logs/2010.08.30.txt
/data/logs/2010.08.31.txt
/data/logs/2010.09.01.txt
/data/logs/2010.09.02.txt
/data/logs/2010.09.03.txt
/data/logs/2010.09.04.txt
/data/logs/2010.09.05.txt
/data/logs/2010.09.06.txt
/data/logs/2010.09.07.txt
/data/logs/2010.09.08.txt
/data/logs/2010.09.09.txt
/data/logs/2010.09.10.txt
/data/logs/2010.09.11.txt
/data/logs/2010.09.12.txt
/data/logs/2010.09.13.txt
/data/logs/2010.09.14.txt
/data/logs/2010.09.15.txt
/data/logs/2010.09.16.txt
/data/logs/2010.09.17.txt
/data/logs/2010.09.18.txt
/data/logs/2010.09.19.txt
/data/logs/2010.09.20.txt
/data/logs/2010.09.21.txt
/data/logs/2010.09.22.txt
/data/logs/2010.09.23.txt
/data/logs/2010.09.24.txt
/data/logs/2010.09.25.txt
/data/logs/2010.09.26.txt
/data/logs/2010.09.27.txt
/data/logs/2010.09.28.txt
/data/logs/2010.09.29.txt
/data/logs/2010.09.30.txt
/data/logs/2010.10.01.txt
/data/logs/2010.10.02.txt
/data/logs/2010.10.03.txt
/data/logs/2010.10.04.txt
/data/logs/2010.10.05.txt
/data/logs/2010.10.06.txt
/data/logs/2010.10.07.txt
/data/logs/2010.10.08.txt
/data/logs/2010.10.09.txt
/data/logs/2010.10.10.txt
/data/logs/2010.10.11.txt
/data/logs/2010.10.12.txt
/data/logs/2010.10.13.txt
/data/logs/2010.10.14.txt
/data/logs/2010.10.15.txt
/data/logs/2010.10.16.txt
/data/logs/2010.10.17.txt
/data/logs/2010.10.18.txt
/data/logs/2010.10.19.txt
/data/logs/2010.10.20.txt
/data/logs/2010.10.21.txt
/data/logs/2010.10.22.txt
/data/logs/2010.10.23.txt
/data/logs/2010.10.24.txt
/data/logs/2010.10.25.txt
/data/logs/2010.10.26.txt
/data/logs/2010.10.27.txt
/data/logs/2010.10.28.txt
/data/logs/2010.10.29.txt
/data/logs/2010.10.30.txt
/data/logs/2010.10.31.txt
/data/logs/2010.11.01.txt
/data/logs/2010.11.02.txt
/data/logs/2010.11.03.txt
/data/logs/2010.11.04.txt
/data/logs/2010.11.05.txt
/data/logs/2010.11.06.txt
/data/logs/2010.11.07.txt
/data/logs/2010.11.08.txt
/data/logs/2010.11.09.txt
/data/logs/2010.11.10.txt
/data/logs/2010.11.11.txt
/data/logs/2010.11.12.txt
/data/logs/2010.11.13.txt
/data/logs/2010.11.14.txt
/data/logs/2010.11.15.txt
/data/logs/2010.11.16.txt
/data/logs/2010.11.17.txt
/data/logs/2010.11.18.txt
/data/logs/2010.11.19.txt
/data/logs/2010.11.20.txt
/data/logs/2010.11.21.txt
/data/logs/2010.11.22.txt
/data/logs/2010.11.23.txt
/data/logs/2010.11.24.txt
/data/logs/2010.11.25.txt
/data/logs/2010.11.26.txt
/data/logs/2010.11.27.txt
/data/logs/2010.11.28.txt
/data/logs/2010.11.29.txt
/data/logs/2010.11.30.txt
/data/logs/2010.12.01.txt
/data/logs/2010.12.02.txt
/data/logs/2010.12.03.txt
/data/logs/2010.12.04.txt
/data/logs/2010.12.05.txt
/data/logs/2010.12.06.txt
/data/logs/2010.12.07.txt
/data/logs/2010.12.08.txt
/data/logs/2010.12.09.txt
/data/logs/2010.12.10.txt
/data/logs/2010.12.11.txt
/data/logs/2010.12.12.txt
/data/logs/2010.12.13.txt
/data/logs/2010.12.14.txt
/data/logs/2010.12.15.txt
/data/logs/2010.12.16.txt
/data/logs/2010.12.17.txt
/data/logs/2010.12.18.txt
/data/logs/2010.12.19.txt
/data/logs/2010.12.20.txt
/data/logs/2010.12.21.txt
/data/logs/2010.12.22.txt
/data/logs/2010.12.23.txt
/data/logs/2010.12.24.txt
/data/logs/2010.12.25.txt
/data/logs/2010.12.26.txt
/data/logs/2010.12.27.txt
/data/logs/2010.12.28.txt
/data/logs/2010.12.29.txt
/data/logs/2010.12.30.txt
/data/logs/2010.12.31.txt
/data/logs/2011.01.01.txt
/data/logs/2011.01.02.txt
/data/logs/2011.01.03.txt
/data/logs/2011.01.04.txt
/data/logs/2011.01.05.txt
/data/logs/2011.01.06.txt
/data/logs/2011.01.07.txt
/data/logs/2011.01.08.txt
/data/logs/2011.01.09.txt
/data/logs/2011.01.10.txt
/data/logs/2011.01.11.txt
/data/logs/2011.01.12.txt
/data/logs/2011.01.13.txt
/data/logs/2011.01.14.txt
/data/logs/2011.01.15.txt
/data/logs/2011.01.16.txt
/data/logs/2011.01.17.txt
/data/logs/2011.01.18.txt
/data/logs/2011.01.19.txt
/data/logs/2011.01.20.txt
/data/logs/2011.01.21.txt
/data/logs/2011.01.22.txt
/data/logs/2011.01.23.txt
/data/logs/2011.01.24.txt
/data/logs/2011.01.25.txt
/data/logs/2011.01.26.txt
/data/logs/2011.01.27.txt
/data/logs/2011.01.28.txt
/data/logs/2011.01.29.txt
/data/logs/2011.01.30.txt
/data/logs/2011.01.31.txt
/data/logs/2011.02.01.txt
/data/logs/2011.02.02.txt
/data/logs/2011.02.03.txt
/data/logs/2011.02.04.txt
/data/logs/2011.02.05.txt
/data/logs/2011.02.06.txt
/data/logs/2011.02.07.txt
/data/logs/2011.02.08.txt
/data/logs/2011.02.09.txt
/data/logs/2011.02.10.txt
/data/logs/2011.02.11.txt
/data/logs/2011.02.12.txt
/data/logs/2011.02.13.txt
/data/logs/2011.02.14.txt
/data/logs/2011.02.15.txt
/data/logs/2011.02.16.txt
/data/logs/2011.02.17.txt
/data/logs/2011.02.18.txt
/data/logs/2011.02.19.txt
/data/logs/2011.02.20.txt
/data/logs/2011.02.21.txt
/data/logs/2011.02.22.txt
/data/logs/2011.02.23.txt
/data/logs/2011.02.24.txt
/data/logs/2011.02.25.txt
/data/logs/2011.02.26.txt
/data/logs/2011.02.27.txt
/data/logs/2011.02.28.txt
/data/logs/2011.03.01.txt
/data/logs/2011.03.02.txt
/data/logs/2011.03.03.txt
/data/logs/2011.03.04.txt
/data/logs/2011.03.05.txt
/data/logs/2011.03.06.txt
/data/logs/2011.03.07.txt
/data/logs/2011.03.08.txt
/data/logs/2011.03.09.txt
/data/logs/2011.03.10.txt
/data/logs/2011.03.11.txt
/data/logs/2011.03.12.txt
/data/logs/2011.03.13.txt
/data/logs/2011.03.14.txt
/data/logs/2011.03.15.txt
/data/logs/2011.03.16.txt
/data/logs/2011.03.17.txt
/data/logs/2011.03.18.txt
/data/logs/2011.03.19.txt
/data/logs/2011.03.20.txt
/data/logs/2011.03.21.txt
/data/logs/2011.03.22.txt
/data/logs/2011.03.23.txt
/data/logs/2011.03.24.txt
/data/logs/2011.03.25.txt
/data/logs/2011.03.26.txt
/data/logs/2011.03.27.txt
/data/logs/2011.03.28.txt
/data/logs/2011.03.29.txt
/data/logs/2011.03.30.txt
/data/logs/2011.03.31.txt
/data/logs/2011.04.01.txt
/data/logs/2011.04.02.txt
/data/logs/2011.04.03.txt
/data/logs/2011.04.04.txt
/data/logs/2011.04.05.txt
/data/logs/2011.04.06.txt
/data/logs/2011.04.07.txt
/data/logs/2011.04.08.txt
/data/logs/2011.04.09.txt
/data/logs/2011.04.10.txt
/data/logs/2011.04.11.txt
/data/logs/2011.04.12.txt
/data/logs/2011.04.13.txt
/data/logs/2011.04.14.txt
/data/logs/2011.04.15.txt
/data/logs/2011.04.16.txt
/data/logs/2011.04.17.txt
/data/logs/2011.04.18.txt
/data/logs/2011.04.19.txt
/data/logs/2011.04.20.txt
/data/logs/2011.04.21.txt
/data/logs/2011.04.22.txt
/data/logs/2011.04.23.txt
/data/logs/2011.04.24.txt
/data/logs/2011.04.25.txt
/data/logs/2011.04.26.txt
/data/logs/2011.04.27.txt
/data/logs/2011.04.28.txt
/data/logs/2011.04.29.txt
/data/logs/2011.04.30.txt
/data/logs/2011.05.01.txt
/data/logs/2011.05.02.txt
/data/logs/2011.05.03.txt
/data/logs/2011.05.04.txt
/data/logs/2011.05.05.txt
/data/logs/2011.05.06.txt
/data/logs/2011.05.07.txt
/data/logs/2011.05.08.txt
/data/logs/2011.05.09.txt
/data/logs/2011.05.10.txt
/data/logs/2011.05.11.txt
/data/logs/2011.05.12.txt
/data/logs/2011.05.13.txt
/data/logs/2011.05.14.txt
/data/logs/2011.05.15.txt
/data/logs/2011.05.16.txt
/data/logs/2011.05.17.txt
/data/logs/2011.05.18.txt
/data/logs/2011.05.19.txt
/data/logs/2011.05.20.txt
/data/logs/2011.05.21.txt
/data/logs/2011.05.22.txt
/data/logs/2011.05.23.txt
/data/logs/2011.05.24.txt
/data/logs/2011.05.25.txt
/data/logs/2011.05.26.txt
/data/logs/2011.05.27.txt
/data/logs/2011.05.28.txt
/data/logs/2011.05.29.txt
/data/logs/2011.05.30.txt
/data/logs/2011.05.31.txt
/data/logs/2011.06.01.txt
/data/logs/2011.06.02.txt
/data/logs/2011.06.03.txt
/data/logs/2011.06.04.txt
/data/logs/2011.06.05.txt
/data/logs/2011.06.06.txt
/data/logs/2011.06.07.txt
/data/logs/2011.06.08.txt
/data/logs/2011.06.09.txt
/data/logs/2011.06.10.txt
/data/logs/2011.06.11.txt
/data/logs/2011.06.12.txt
/data/logs/2011.06.13.txt
/data/logs/2011.06.14.txt
/data/logs/2011.06.15.txt
/data/logs/2011.06.16.txt
/data/logs/2011.06.17.txt
/data/logs/2011.06.18.txt
/data/logs/2011.06.19.txt
/data/logs/2011.06.20.txt
/data/logs/2011.06.21.txt
/data/logs/2011.06.22.txt
/data/logs/2011.06.23.txt
/data/logs/2011.06.24.txt
/data/logs/2011.06.25.txt
/data/logs/2011.06.26.txt
/data/logs/2011.06.27.txt
/data/logs/2011.06.28.txt
/data/logs/2011.06.29.txt
/data/logs/2011.06.30.txt
/data/logs/2011.07.01.txt
/data/logs/2011.07.02.txt
/data/logs/2011.07.03.txt
/data/logs/2011.07.04.txt
/data/logs/2011.07.05.txt
/data/logs/2011.07.06.txt
/data/logs/2011.07.07.txt
/data/logs/2011.07.08.txt
/data/logs/2011.07.09.txt
/data/logs/2011.07.10.txt
/data/logs/2011.07.11.txt
/data/logs/2011.07.12.txt
/data/logs/2011.07.13.txt
/data/logs/2011.07.14.txt
/data/logs/2011.07.15.txt
/data/logs/2011.07.16.txt
/data/logs/2011.07.17.txt
/data/logs/2011.07.18.txt
/data/logs/2011.07.19.txt
/data/logs/2011.07.20.txt
/data/logs/2011.07.21.txt
/data/logs/2011.07.22.txt
/data/logs/2011.07.23.txt
/data/logs/2011.07.24.txt
/data/logs/2011.07.25.txt
/data/logs/2011.07.26.txt
/data/logs/2011.07.27.txt
/data/logs/2011.07.28.txt
/data/logs/2011.07.29.txt
/data/logs/2011.07.30.txt
/data/logs/2011.07.31.txt
/data/logs/2011.08.01.txt
/data/logs/2011.08.02.txt
/data/logs/2011.08.03.txt
/data/logs/2011.08.04.txt
/data/logs/2011.08.05.txt
/data/logs/2011.08.06.txt
/data/logs/2011.08.07.txt
/data/logs/2011.08.08.txt
/data/logs/2011.08.09.txt
/data/logs/2011.08.10.txt
/data/logs/2011.08.11.txt
/data/logs/2011.08.12.txt
/data/logs/2011.08.13.txt
/data/logs/2011.08.14.txt
/data/logs/2011.08.15.txt
/data/logs/2011.08.16.txt
/data/logs/2011.08.17.txt
/data/logs/2011.08.18.txt
/data/logs/2011.08.19.txt
/data/logs/2011.08.20.txt
/data/logs/2011.08.21.txt
/data/logs/2011.08.22.txt
/data/logs/2011.08.23.txt
/data/logs/2011.08.24.txt
/data/logs/2011.08.25.txt
/data/logs/2011.08.26.txt
/data/logs/2011.08.27.txt
/data/logs/2011.08.28.txt
/data/logs/2011.08.29.txt
/data/logs/2011.08.30.txt
/data/logs/2011.08.31.txt
/data/logs/2011.09.01.txt
/data/logs/2011.09.02.txt
/data/logs/2011.09.03.txt
/data/logs/2011.09.04.txt
/data/logs/2011.09.05.txt
/data/logs/2011.09.06.txt
/data/logs/2011.09.07.txt
/data/logs/2011.09.08.txt
/data/logs/2011.09.09.txt
/data/logs/2011.09.10.txt
/data/logs/2011.09.11.txt
/data/logs/2011.09.12.txt
/data/logs/2011.09.13.txt
/data/logs/2011.09.14.txt
/data/logs/2011.09.15.txt
/data/logs/2011.09.16.txt
/data/logs/2011.09.17.txt
/data/logs/2011.09.18.txt
/data/logs/2011.09.19.txt
/data/logs/2011.09.20.txt
/data/logs/2011.09.21.txt
/data/logs/2011.09.22.txt
/data/logs/2011.09.23.txt
/data/logs/2011.09.24.txt
/data/logs/2011.09.25.txt
/data/logs/2011.09.26.txt
/data/logs/2011.09.27.txt
/data/logs/2011.09.28.txt
/data/logs/2011.09.29.txt
/data/logs/2011.09.30.txt
/data/logs/2011.10.01.txt
/data/logs/2011.10.02.txt
/data/logs/2011.10.03.txt
/data/logs/2011.10.04.txt
/data/logs/2011.10.05.txt
/data/logs/2011.10.06.txt
/data/logs/2011.10.07.txt
/data/logs/2011.10.08.txt
/data/logs/2011.10.09.txt
/data/logs/2011.10.10.txt
/data/logs/2011.10.11.txt
/data/logs/2011.10.12.txt
/data/logs/2011.10.13.txt
/data/logs/2011.10.14.txt
/data/logs/2011.10.15.txt
/data/logs/2011.10.16.txt
/data/logs/2011.10.17.txt
/data/logs/2011.10.18.txt
/data/logs/2011.10.19.txt
/data/logs/2011.10.20.txt
/data/logs/2011.10.21.txt
/data/logs/2011.10.22.txt
/data/logs/2011.10.23.txt
/data/logs/2011.10.24.txt
/data/logs/2011.10.25.txt
/data/logs/2011.10.26.txt
/data/logs/2011.10.27.txt
/data/logs/2011.10.28.txt
/data/logs/2011.10.29.txt
/data/logs/2011.10.30.txt
/data/logs/2011.10.31.txt
/data/logs/2011.11.01.txt
/data/logs/2011.11.02.txt
/data/logs/2011.11.03.txt
/data/logs/2011.11.04.txt
/data/logs/2011.11.05.txt
/data/logs/2011.11.06.txt
/data/logs/2011.11.07.txt
/data/logs/2011.11.08.txt
/data/logs/2011.11.09.txt
/data/logs/2011.11.10.txt
/data/logs/2011.11.11.txt
/data/logs/2011.11.12.txt
/data/logs/2011.11.13.txt
/data/logs/2011.11.14.txt
/data/logs/2011.11.15.txt
/data/logs/2011.11.16.txt
/data/logs/2011.11.17.txt
/data/logs/2011.11.18.txt
/data/logs/2011.11.19.txt
/data/logs/2011.11.20.txt
/data/logs/2011.11.21.txt
/data/logs/2011.11.22.txt
/data/logs/2011.11.23.txt
/data/logs/2011.11.24.txt
/data/logs/2011.11.25.txt
/data/logs/2011.11.26.txt
/data/logs/2011.11.27.txt
/data/logs/2011.11.28.txt
/data/logs/2011.11.29.txt
/data/logs/2011.11.30.txt
/data/logs/2011.12.01.txt
/data/logs/2011.12.02.txt
/data/logs/2011.12.03.txt
/data/logs/2011.12.04.txt
/data/logs/2011.12.05.txt
/data/logs/2011.12.06.txt
/data/logs/2011.12.07.txt
/data/logs/2011.12.08.txt
/data/logs/2011.12.09.txt
/data/logs/2011.12.10.txt
/data/logs/2011.12.11.txt
/data/logs/2011.12.12.txt
/data/logs/2011.12.13.txt
/data/logs/2011.12.14.txt
/data/logs/2011.12.15.txt
/data/logs/2011.12.16.txt
/data/logs/2011.12.17.txt
/data/logs/2011.12.18.txt
/data/logs/2011.12.19.txt
/data/logs/2011.12.20.txt
/data/logs/2011.12.21.txt
/data/logs/2011.12.22.txt
/data/logs/2011.12.23.txt
/data/logs/2011.12.24.txt
/data/logs/2011.12.25.txt
/data/logs/2011.12.26.txt
/data/logs/2011.12.27.txt
/data/logs/2011.12.28.txt
/data/logs/2011.12.29.txt
/data/logs/2011.12.30.txt
/data/logs/2011.12.31.txt
/data/logs/2012.01.01.txt
/data/logs/2012.01.02.txt
/data/logs/2012.01.03.txt
/data/logs/2012.01.04.txt
/data/logs/2012.01.05.txt
/data/logs/2012.01.06.txt
/data/logs/2012.01.07.txt
/data/logs/2012.01.08.txt
/data/logs/2012.01.09.txt
/data/logs/2012.01.10.txt
/data/logs/2012.01.11.txt
/data/logs/2012.01.12.txt
/data/logs/2012.01.13.txt
/data/logs/2012.01.14.txt
/data/logs/2012.01.15.txt
/data/logs/2012.01.16.txt
/data/logs/2012.01.17.txt
/data/logs/2012.01.18.txt
/data/logs/2012.01.19.txt
/data/logs/2012.01.20.txt
/data/logs/2012.01.21.txt
/data/logs/2012.01.22.txt
/data/logs/2012.01.23.txt
/data/logs/2012.01.24.txt
/data/logs/2012.01.25.txt
/data/logs/2012.01.26.txt
/data/logs/2012.01.27.txt
/data/logs/2012.01.28.txt
/data/logs/2012.01.29.txt
/data/logs/2012.01.30.txt
/data/logs/2012.01.31.txt
/data/logs/2012.02.01.txt
/data/logs/2012.02.02.txt
/data/logs/2012.02.03.txt
/data/logs/2012.02.04.txt
/data/logs/2012.02.05.txt
/data/logs/2012.02.06.txt
/data/logs/2012.02.07.txt
/data/logs/2012.02.08.txt
/data/logs/2012.02.09.txt
/data/logs/2012.02.10.txt
/data/logs/2012.02.11.txt
/data/logs/2012.02.12.txt
/data/logs/2012.02.13.txt
/data/logs/2012.02.14.txt
/data/logs/2012.02.15.txt
/data/logs/2012.02.16.txt
/data/logs/2012.02.17.txt
/data/logs/2012.02.18.txt
/data/logs/2012.02.19.txt
/data/logs/2012.02.20.txt
/data/logs/2012.02.21.txt
/data/logs/2012.02.22.txt
/data/logs/2012.02.23.txt
/data/logs/2012.02.24.txt
/data/logs/2012.02.25.txt
/data/logs/2012.02.26.txt
/data/logs/2012.02.27.txt
/data/logs/2012.02.28.txt
/data/logs/2012.02.29.txt
/data/logs/2012.03.01.txt
/data/logs/2012.03.02.txt
/data/logs/2012.03.03.txt
/data/logs/2012.03.04.txt
/data/logs/2012.03.05.txt
/data/logs/2012.03.06.txt
/data/logs/2012.03.07.txt
/data/logs/2012.03.08.txt
/data/logs/2012.03.09.txt
/data/logs/2012.03.10.txt
/data/logs/2012.03.11.txt
/data/logs/2012.03.12.txt
/data/logs/2012.03.13.txt
/data/logs/2012.03.14.txt
/data/logs/2012.03.15.txt
/data/logs/2012.03.16.txt
/data/logs/2012.03.17.txt
/data/logs/2012.03.18.txt
/data/logs/2012.03.19.txt
/data/logs/2012.03.20.txt
/data/logs/2012.03.21.txt
/data/logs/2012.03.22.txt
/data/logs/2012.03.23.txt
/data/logs/2012.03.24.txt
/data/logs/2012.03.25.txt
/data/logs/2012.03.26.txt
/data/logs/2012.03.27.txt
/data/logs/2012.03.28.txt
/data/logs/2012.03.29.txt
/data/logs/2012.03.30.txt
/data/logs/2012.03.31.txt
/data/logs/2012.04.01.txt
/data/logs/2012.04.02.txt
/data/logs/2012.04.03.txt
/data/logs/2012.04.04.txt
/data/logs/2012.04.05.txt
/data/logs/2012.04.06.txt
/data/logs/2012.04.07.txt
/data/logs/2012.04.08.txt
/data/logs/2012.04.09.txt
/data/logs/2012.04.10.txt
/data/logs/2012.04.11.txt
/data/logs/2012.04.12.txt
/data/logs/2012.04.13.txt
/data/logs/2012.04.14.txt
/data/logs/2012.04.15.txt
/data/logs/2012.04.16.txt
/data/logs/2012.04.17.txt
/data/logs/2012.04.18.txt
/data/logs/2012.04.19.txt
/data/logs/2012.04.20.txt
/data/logs/2012.04.21.txt
/data/logs/2012.04.22.txt
/data/logs/2012.04.23.txt
/data/logs/2012.04.24.txt
/data/logs/2012.04.25.txt
/data/logs/2012.04.26.txt
/data/logs/2012.04.27.txt
/data/logs/2012.04.28.txt
/data/logs/2012.04.29.txt
/data/logs/2012.04.30.txt
/data/logs/2012.05.01.txt
/data/logs/2012.05.02.txt
/data/logs/2012.05.03.txt
/data/logs/2012.05.04.txt
/data/logs/2012.05.05.txt
/data/logs/2012.05.06.txt
/data/logs/2012.05.07.txt
/data/logs/2012.05.08.txt
/data/logs/2012.05.09.txt
/data/logs/2012.05.10.txt
/data/logs/2012.05.11.txt
/data/logs/2012.05.12.txt
/data/logs/2012.05.13.txt
/data/logs/2012.05.14.txt
/data/logs/2012.05.15.txt
/data/logs/2012.05.16.txt
/data/logs/2012.05.17.txt
/data/logs/2012.05.18.txt
/data/logs/2012.05.19.txt
/data/logs/2012.05.20.txt
/data/logs/2012.05.21.txt
/data/logs/2012.05.22.txt
/data/logs/2012.05.23.txt
/data/logs/2012.05.24.txt
/data/logs/2012.05.25.txt
/data/logs/2012.05.26.txt
/data/logs/2012.05.27.txt
/data/logs/2012.05.28.txt
/data/logs/2012.05.29.txt
/data/logs/2012.05.30.txt
/data/logs/2012.05.31.txt
/data/logs/2012.06.01.txt
/data/logs/2012.06.02.txt
/data/logs/2012.06.03.txt
/data/logs/2012.06.04.txt
/data/logs/2012.06.05.txt
/data/logs/2012.06.06.txt
/data/logs/2012.06.07.txt
/data/logs/2012.06.08.txt
/data/logs/2012.06.09.txt
/data/logs/2012.06.10.txt
/data/logs/2012.06.11.txt
/data/logs/2012.06.12.txt
/data/logs/2012.06.13.txt
/data/logs/2012.06.14.txt
/data/logs/2012.06.15.txt
/data/logs/2012.06.16.txt
/data/logs/2012.06.17.txt
/data/logs/2012.06.18.txt
/data/logs/2012.06.19.txt
/data/logs/2012.06.20.txt
/data/logs/2012.06.21.txt
/data/logs/2012.06.22.txt
/data/logs/2012.06.23.txt
/data/logs/2012.06.24.txt
/data/logs/2012.06.25.txt
/data/logs/2012.06.26.txt
/data/logs/2012.06.27.txt
/data/logs/2012.06.28.txt
/data/logs/2012.06.29.txt
/data/logs/2012.06.30.txt
/data/logs/2012.07.01.txt
/data/logs/2012.07.02.txt
/data/logs/2012.07.03.txt
/data/logs/2012.07.04.txt
/data/logs/2012.07.05.txt
/data/logs/2012.07.06.txt
/data/logs/2012.07.07.txt
/data/logs/2012.07.08.txt
/data/logs/2012.07.09.txt
/data/logs/2012.07.10.txt
/data/logs/2012.07.11.txt
/data/logs/2012.07.12.txt
/data/logs/2012.07.13.txt
/data/logs/2012.07.14.txt
/data/logs/2012.07.15.txt
/data/logs/2012.07.16.txt
/data/logs/2012.07.17.txt
/data/logs/2012.07.18.txt
/data/logs/2012.07.19.txt
/data/logs/2012.07.20.txt
/data/logs/2012.07.21.txt
/data/logs/2012.07.22.txt
/data/logs/2012.07.23.txt
/data/logs/2012.07.24.txt
/data/logs/2012.07.25.txt
/data/logs/2012.07.26.txt
/data/logs/2012.07.27.txt
/data/logs/2012.07.28.txt
/data/logs/2012.07.29.txt
/data/logs/2012.07.30.txt
/data/logs/2012.07.31.txt
/data/logs/2012.08.01.txt
/data/logs/2012.08.02.txt
/data/logs/2012.08.03.txt
/data/logs/2012.08.04.txt
/data/logs/2012.08.05.txt
/data/logs/2012.08.06.txt
/data/logs/2012.08.07.txt
/data/logs/2012.08.08.txt
/data/logs/2012.08.09.txt
/data/logs/2012.08.10.txt
/data/logs/2012.08.11.txt
/data/logs/2012.08.12.txt
/data/logs/2012.08.13.txt
/data/logs/2012.08.14.txt
/data/logs/2012.08.15.txt
/data/logs/2012.08.16.txt
/data/logs/2012.08.17.txt
/data/logs/2012.08.18.txt
/data/logs/2012.08.19.txt
/data/logs/2012.08.20.txt
/data/logs/2012.08.21.txt
/data/logs/2012.08.22.txt
/data/logs/2012.08.23.txt
/data/logs/2012.08.24.txt
/data/logs/2012.08.25.txt
/data/logs/2012.08.26.txt
/data/logs/2012.08.27.txt
/data/logs/2012.08.28.txt
/data/logs/2012.08.29.txt
/data/logs/2012.08.30.txt
/data/logs/2012.08.31.txt
/data/logs/2012.09.01.txt
/data/logs/2012.09.02.txt
/data/logs/2012.09.03.txt
/data/logs/2012.09.04.txt
/data/logs/2012.09.05.txt
/data/logs/2012.09.06.txt
/data/logs/2012.09.07.txt
/data/logs/2012.09.08.txt
/data/logs/2012.09.09.txt
/data/logs/2012.09.10.txt
/data/logs/2012.09.11.txt
/data/logs/2012.09.12.txt
/data/logs/2012.09.13.txt
/data/logs/2012.09.14.txt
/data/logs/2012.09.15.txt
/data/logs/2012.09.16.txt
/data/logs/2012.09.17.txt
/data/logs/2012.09.18.txt
/data/logs/2012.09.19.txt
/data/logs/2012.09.20.txt
/data/logs/2012.09.21.txt
/data/logs/2012.09.22.txt
/data/logs/2012.09.23.txt
/data/logs/2012.09.24.txt
/data/logs/2012.09.25.txt
/data/logs/2012.09.26.txt
/data/logs/2012.09.27.txt
/data/logs/2012.09.28.txt
/data/logs/2012.09.29.txt
/data/logs/2012.09.30.txt
/data/logs/2012.10.01.txt
/data/logs/2012.10.02.txt
/data/logs/2012.10.03.txt
/data/logs/2012.10.04.txt
/data/logs/2012.10.05.txt
/data/logs/2012.10.06.txt
/data/logs/2012.10.07.txt
/data/logs/2012.10.08.txt
/data/logs/2012.10.09.txt
/data/logs/2012.10.10.txt
/data/logs/2012.10.11.txt
/data/logs/2012.10.12.txt
/data/logs/2012.10.13.txt
/data/logs/2012.10.14.txt
/data/logs/2012.10.15.txt
/data/logs/2012.10.16.txt
/data/logs/2012.10.17.txt
/data/logs/2012.10.18.txt
/data/logs/2012.10.19.txt
/data/logs/2012.10.20.txt
/data/logs/2012.10.21.txt
/data/logs/2012.10.22.txt
/data/logs/2012.10.23.txt
/data/logs/2012.10.24.txt
/data/logs/2012.10.25.txt
/data/logs/2012.10.26.txt
/data/logs/2012.10.27.txt
/data/logs/2012.10.28.txt
/data/logs/2012.10.29.txt
/data/logs/2012.10.30.txt
/data/logs/2012.10.31.txt
/data/logs/2012.11.01.txt
/data/logs/2012.11.02.txt
/data/logs/2012.11.03.txt
/data/logs/2012.11.04.txt
/data/logs/2012.11.05.txt
/data/logs/2012.11.06.txt
/data/logs/2012.11.07.txt
/data/logs/2012.11.08.txt
/data/logs/2012.11.09.txt
/data/logs/2012.11.10.txt
/data/logs/2012.11.11.txt
/data/logs/2012.11.12.txt
/data/logs/2012.11.13.txt
/data/logs/2012.11.14.txt
/data/logs/2012.11.15.txt
/data/logs/2012.11.16.txt
/data/logs/2012.11.17.txt
/data/logs/2012.11.18.txt
/data/logs/2012.11.19.txt
/data/logs/2012.11.20.txt
/data/logs/2012.11.21.txt
/data/logs/2012.11.22.txt
/data/logs/2012.11.23.txt
/data/logs/2012.11.24.txt
/data/logs/2012.11.25.txt
/data/logs/2012.11.26.txt
/data/logs/2012.11.27.txt
/data/logs/2012.11.28.txt
/data/logs/2012.11.29.txt
/data/logs/2012.11.30.txt
/data/logs/2012.12.01.txt
/data/logs/2012.12.02.txt
/data/logs/2012.12.03.txt
/data/logs/2012.12.04.txt
/data/logs/2012.12.05.txt
/data/logs/2012.12.06.txt
/data/logs/2012.12.07.txt
/data/logs/2012.12.08.txt
/data/logs/2012.12.09.txt
/data/logs/2012.12.10.txt
/data/logs/2012.12.11.txt
/data/logs/2012.12.12.txt
/data/logs/2012.12.13.txt
/data/logs/2012.12.14.txt
/data/logs/2012.12.15.txt
/data/logs/2012.12.16.txt
/data/logs/2012.12.17.txt
/data/logs/2012.12.18.txt
/data/logs/2012.12.19.txt
/data/logs/2012.12.20.txt
/data/logs/2012.12.21.txt
/data/logs/2012.12.22.txt
/data/logs/2012.12.23.txt
/data/logs/2012.12.24.txt
/data/logs/2012.12.25.txt
/data/logs/2012.12.26.txt
/data/logs/2012.12.27.txt
/data/logs/2012.12.28.txt
/data/logs/2012.12.29.txt
/data/logs/2012.12.30.txt
/data/logs/2012.12.31.txt
/data/logs/ip_blacklist.csv
/data/tpch/q19_preprocessed/lineitem-sf-10.tbl
/data/tpch/q19_preprocessed/part-sf-10.tbl
/data/tpch/q6/string/lineitem.tbl
/data/tpch/q6_preprocessed/lineitem-sf-10.tbl
/data/zillow/Z1_preprocessed/zillow_Z1_10G.csv
/data/zillow/Z2_preprocessed/zillow_Z2_10G.csv
/data/zillow/Zdirty/zillow_dirty@10G.csv
/data/zillow/Zdirty/zillow_dirty_synthetic@10G.csv'''.split()

    # check that tuplex exists python3.6 -c "import tuplex"
    cmd = 'python3.6 -c "import tuplex"'
    exit_code, output = container.exec_run(cmd, stderr=True, stdout=True, detach=detach)
    if 0 != exit_code:
        logging.error("Did not find tuplex module, please run first build subcommand.")
        sys.exit(1)

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
    logging.info('Plotting Figure10 (Flights breakdown)')
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
    DEFAULT_FLIGHTS_BREAKDOWN_PATH = input_path + '/flights/flights_breakdown'
    DEFAULT_LOGS_PATH = input_path + '/logs'
    DEFAULT_311_PATH = input_path + '/311'
    DEFAULT_TPCH_PATH = input_path + '/tpch'

    PLOT_ALL = False
    if 'all' == target.lower():
        PLOT_ALL = True

    logging.info('Plotting output for target {}'.format(target))

    # go through list and plot whatever was selected
    if 'table3' == target.lower() or PLOT_ALL:
        try:
            plot_table3(DEFAULT_ZILLOW_PATH, output_path)
        except Exception as e:
            logging.error("Failed to compute table3")
    if 'figure3' == target.lower() or PLOT_ALL:
        try:
            plot_figure3(DEFAULT_ZILLOW_PATH, output_path)
        except Exception as e:
            logging.error("Failed to plot figure3")
    if 'figure4' == target.lower() or PLOT_ALL:
        try:
            plot_figure4(DEFAULT_FLIGHTS_PATH, output_path)
        except Exception as e:
            logging.error("Failed to plot figure4")
    if 'figure5' == target.lower() or PLOT_ALL:
        try:
            if not os.path.isdir(DEFAULT_LOGS_PATH):
                DEFAULT_LOGS_PATH = input_path + '/weblogs'
            plot_figure5(DEFAULT_LOGS_PATH, output_path)
        except Exception as e:
            logging.error("Failed to plot figure5")
    if 'figure6' == target.lower() or PLOT_ALL:
        try:
            plot_figure6(DEFAULT_ZILLOW_PATH, output_path)
        except Exception as e:
            logging.error("Failed to plot figure6")
    if 'figure7' == target.lower() or PLOT_ALL:
        try:
            plot_figure7(DEFAULT_ZILLOW_PATH, output_path)
        except Exception as e:
            logging.error("Failed to plot figure7")
    if 'figure8' == target.lower() or PLOT_ALL:
        try:
            plot_figure8(DEFAULT_311_PATH, output_path)
        except Exception as e:
            logging.error("Failed to plot figure8")
    if 'figure9' == target.lower() or PLOT_ALL:
        try:
            plot_figure9(DEFAULT_TPCH_PATH, output_path)
        except Exception as e:
            logging.error("Failed to plot figure9")
    if 'figure10' == target.lower() or PLOT_ALL:
        try:
            plot_figure10(DEFAULT_FLIGHTS_BREAKDOWN_PATH, output_path)
        except Exception as e:
            logging.error("Failed to plot figure10")
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
