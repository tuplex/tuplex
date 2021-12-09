#!/usr/bin/env python3

# CLI to run experiments/reproduce easily
import click
import logging

experiment_targets = ['all', 'figure3', 'figure4', 'figure5',
'figure6', 'figure7', 'figure8', 'figure9', 'figure10', 'table3']

@click.group()
def commands():
    pass

@click.command()
@click.argument('target', type=click.Choice(experiment_targets, case_sensitive=False))
def run(target):
    logging.info('Running experiments for target {}'.format(target))


@click.command()
@click.argument('target', type=click.Choice(experiment_targets, case_sensitive=False))
def plot(target):
    logging.info('Plotting output for target {}'.format(target))

commands.add_command(run)
commands.add_command(plot)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s: %(message)s',
                    handlers=[logging.FileHandler("experiment.log", mode='w'),
                              logging.StreamHandler()])
    stream_handler = [h for h in logging.root.handlers if isinstance(h , logging.StreamHandler)][0]
    stream_handler.setLevel(logging.INFO)


    commands()
