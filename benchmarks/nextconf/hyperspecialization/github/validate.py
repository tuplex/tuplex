import glob
import os
from typing import Optional
import logging
import argparse
import pandas as pd

def setup_logging(log_path: Optional[str]) -> None:

    LOG_FORMAT="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
    LOG_DATE_FORMAT="%d/%b/%Y %H:%M:%S"

    handlers = []

    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter(LOG_FORMAT)
    # tell the handler to use this format
    console.setFormatter(formatter)

    handlers.append(console)

    # add file handler to root logger
    if log_path:
        handler = logging.FileHandler(log_path)
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        handlers.append*handler

        # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.INFO,
                        format=LOG_FORMAT,
                        datefmt=LOG_DATE_FORMAT,
                        handlers=handlers)


def compare_dataframes_order_independent(df1, df2, **kwargs):
    # check same length
    if len(df1) != len(df2):
        raise ValueError(f'length of dataframes do not match {len(df1)} != {len(df2)}')
    # check columns
    columns1 = list(df1.columns)
    columns2 = list(df2.columns)

    if columns1 != columns2:
        raise ValueError(f'column labels of dataframes do not match {columns1} != {columns2}')

    # sort each dataframe
    def sort_by_all_columns(df):
        return df.sort_values(by=list(df.columns)).reset_index(drop=True)

    df1 = sort_by_all_columns(df1)
    df2 = sort_by_all_columns(df2)

    # compare dataframes using pandas helper
    pd.testing.assert_frame_equal(df1, df2, **kwargs)

    return True
def load_results_from_output_dir(data_path: str) -> pd.DataFrame:
    if not os.path.isdir(data_path):
        logging.error(f"Path {data_path} does not exist")
        return pd.DataFrame()
    output_paths = sorted(glob.glob(os.path.join(data_path, "*.csv")))
    logging.info('Found {} results'.format(len(output_paths)))
    logging.info(f"Loading {len(output_paths)} results from {data_path}")
    df = pd.DataFrame()
    for path in output_paths:
        next_df = pd.read_csv(path)
        # pd.concat is expensive when adding a 0-length df. Skip here to improve validation performance.
        if 0 == len(next_df):
            continue
        df = pd.concat((df, next_df))
    logging.info(f"Loaded {len(df)} rows.")
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Github query validation script')
    parser.add_argument('dirA', metavar='dirA', type=str, help='first directory to read results from')
    parser.add_argument('dirB', metavar='dirB', type=str, help='second directory to read results from')
    args = parser.parse_args()

    dirA = args.dirA
    dirB = args.dirB

    # set up logging, by default always render to console. If log path is present, store file as well
    setup_logging(None)

    logging.info(f"Validating results from directory {dirA} vs. {dirB}")

    df_A = load_results_from_output_dir(dirA)
    df_B = load_results_from_output_dir(dirB)

    if len(df_A) != len(df_B):
        # count per year how many rows exist
        print(df_B['year'].value_counts())
        df_counts = pd.DataFrame({'counts_A': df_A['year'].value_counts(), 'counts_B': df_B['year'].value_counts()})
        print(df_counts)
    compare_dataframes_order_independent(df_A, df_B, check_exact=False) # use check_dtype to not check types exactly.
    logging.info(f"Validated successfully: results in {dirA} and {dirB} are identical.")

