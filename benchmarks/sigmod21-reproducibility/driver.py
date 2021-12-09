import os
import logging

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

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s: %(message)s',
                    handlers=[logging.FileHandler("experiment.log", mode='w'),
                              logging.StreamHandler()])
    stream_handler = [h for h in logging.root.handlers if isinstance(h , logging.StreamHandler)][0]
    stream_handler.setLevel(logging.INFO)

    plot_figure9()
    #plot_figure8()

    #plot_figure5()
    #
    # plot_figure4()
    #
    # plot_table3()

    # plot_figure7()
    #
    # plot_figure3()