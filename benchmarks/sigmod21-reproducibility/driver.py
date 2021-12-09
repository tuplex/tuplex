import os
import logging

def plot_figure3(zillow_path='benchmark_results/zillow', output_folder='plots'):
    logging.info('Plotting Figure3 (Zillow experiment)')
    logging.info('Benchmark result folder specified as {}'.format(zillow_path))
    from plot_scripts.figure3 import plot_Z1, plot_Z2

    logging.info('Plotting Z1 query')
    plot_Z1(os.path.join(zillow_path, 'Z1'), output_folder)
    logging.info('Plotting Z2 query')
    plot_Z2(os.path.join(zillow_path, 'Z2'), output_folder)
    logging.info('Plots saved in {}'.format(output_folder))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(levelname)s: %(message)s',
                    handlers=[logging.FileHandler("experiment.log", mode='w'),
                              logging.StreamHandler()])
    stream_handler = [h for h in logging.root.handlers if isinstance(h , logging.StreamHandler)][0]
    stream_handler.setLevel(logging.INFO)
    plot_figure3()