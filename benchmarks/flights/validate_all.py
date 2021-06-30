#!/usr/bin/env python3
# (c) 2017-2020 L.Spiegelberg
# validates output of flights query


import pandas as pd
import os
import glob
import numpy as np
import json
import re
from tqdm import tqdm

root_path = '.'


def compare_dfs(dfA, dfB):
    if len(dfA) != len(dfB):
        print('not equal, lengths do not coincide {} != {}'.format(len(dfA), len(dfB)))
        return False

    if len(dfA.columns) != len(dfB.columns):
        print('number of columns do not coincide')
        return False

    str_cols = list(dfA.select_dtypes([object]).columns)
    numeric_cols = list(dfA.select_dtypes([bool, int, float]).columns)

    # print(numeric_cols)
    # print(str_cols)

    if len(str_cols) + len(numeric_cols) != len(dfA.columns):
        print('column separation wrong')
        return False

    # go over each single row (will take a lot of time)
    for i in tqdm(range(len(dfA))):
        rowA = dfA.iloc[i].copy()
        rowB = dfB.iloc[i].copy()

        num_valsA = rowA[numeric_cols].astype(np.float64)
        num_valsB = rowB[numeric_cols].astype(np.float64)
        if str(rowA[str_cols].values) != str(rowB[str_cols].values):
            print('{} != {}'.format(str(rowA[str_cols].values), str(rowB[str_cols].values)))
            print(i)
            return False

        if not np.allclose(num_valsA, num_valsB, rtol=1e-3, atol=1e-3, equal_nan=True):
            print('{} != {}'.format(num_valsA, num_valsB))
            print(i)
            return False

    return True

def main():
    spark_folder = 'pyspark_output'
    dask_folder = 'dask_output'

    root_path = '.'

    paths = os.listdir(root_path)
    paths_to_verify = []
    spark_paths = []
    dask_paths = []
    if spark_folder in paths:
        spark_paths = glob.glob(os.path.join(root_path, spark_folder, '*.csv'))
        paths_to_verify += spark_paths
    if dask_folder in paths:
        dask_paths = glob.glob(os.path.join(root_path, dask_folder, '*part*'))
        dask_paths = sorted(dask_paths, key=lambda p: int(re.sub('[^0-9]', '', os.path.basename(p))))
        paths_to_verify += dask_paths

    print('>>> loading dask files ({} found)'.format(len(dask_paths)))
    df_dask = pd.DataFrame()
    for path in dask_paths:
        df_dask = pd.concat((df_dask, pd.read_csv(path, low_memory=False)))

    print('>>> loading spark files ({} found)'.format(len(spark_paths)))
    df_spark = pd.DataFrame()
    for path in spark_paths:
        df = pd.read_csv(path, low_memory=False).replace('\\u0000', np.nan)
        df_spark = pd.concat((df_spark, df))

    tplx_paths = glob.glob(os.path.join(root_path, 'tuplex_output', '*.csv'))
    tplx_paths = sorted(tplx_paths, key=lambda p: int(re.sub('[^0-9]', '', os.path.basename(p))))
    print('>>> loading tuplex paths ({} found)'.format(len(tplx_paths)))
    df_tplx = pd.DataFrame()
    for path in tplx_paths:
        df_tplx = pd.concat((df_tplx, pd.read_csv(path, low_memory=False)))


    # align data types
    cols_to_force = []
    cols = list(df_dask.columns)
    dask_types = list(df_dask.head().dtypes)
    spark_types = list(df_spark.head().dtypes)
    tplx_types = list(df_tplx.head().dtypes)
    for c, d, s, t in zip(cols, dask_types, spark_types, tplx_types):

        if d != s or s != t:
            print('need to align columns   ',c, d, s, t)
            cols_to_force.append(c)

    # set all this cols to np.float64!
    for c in cols_to_force:
        df_spark[c] = df_spark[c].astype(np.float64)
        df_dask[c] = df_dask[c].astype(np.float64)
        df_tplx[c] = df_tplx[c].astype(np.float64)

    cols_to_force = []
    dask_types = list(df_dask.head().dtypes)
    spark_types = list(df_spark.head().dtypes)
    tplx_types = list(df_tplx.head().dtypes)
    for c, d, s, t in zip(cols, dask_types, spark_types, tplx_types):

        if d != s or s != t:
            print(c, d, s, t)
            cols_to_force.append(c)
    assert len(cols_to_force) == 0, 'types not forced to same ones!'

    # sort dataframes (join destroys order)
    key_cols = list(
        df_tplx.columns)  # ['Day', 'Month', 'Year', 'FlightNumber', 'CarrierCode', 'OriginState', 'OriginLongitude', 'DestLongitude']

    df_tplx = df_tplx.sort_values(by=key_cols).reset_index(drop=True)
    df_spark = df_spark.sort_values(by=key_cols).reset_index(drop=True)
    df_dask = df_dask.sort_values(by=key_cols).reset_index(drop=True)

    print('>>> comparing tuplex to spark')
    res = compare_dfs(df_spark, df_tplx)
    print('==> passed' if res else '==> failed')

    print('>>> comparing dask to spark')
    res = compare_dfs(df_spark, df_dask)
    print('==> passed' if res else '==> failed')
    print('done!')

if __name__ == '__main__':
    main()