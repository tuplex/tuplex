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

def plot_figure7(zillow_path='r5d.8xlarge/zillow', output_folder='plots'):
    logging.info('Plotting Figure7 (Tuplex vs. other JITs experiment)')
    logging.info('Benchmark result folder specified as {}'.format(zillow_path))
    from plot_scripts.zillow_plots import figure7, load_data

    logging.info('Loading data...')
    df_Z1, df_Z2 = load_data(zillow_path)
    logging.info('Plotting Z1 (Figure7)')
    figure7(df_Z1, output_folder)
    logging.info('Plots saved in {}'.format(output_folder))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s: %(message)s',
                    handlers=[logging.FileHandler("experiment.log", mode='w'),
                              logging.StreamHandler()])
    stream_handler = [h for h in logging.root.handlers if isinstance(h , logging.StreamHandler)][0]
    stream_handler.setLevel(logging.INFO)

    plot_table3()

    # plot_figure7()
    #
    # plot_figure3()