#!/usr/bin/env python3
# (c) L.Spiegelberg jw17-jwjw

# validates all outputs. Run in validate.sh


import pandas as pd
import os
import glob
import fileinput
import sys
import re

if __name__ == '__main__':
    failed = False

    jw = 25

    print('>>> verifying frameworks...')
    paths_to_verify = []
    root_path = '.'
    paths_to_verify.append(os.path.join(root_path, 'pypy3_tuple/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'pypy3_dict/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'python3_tuple/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'python3_dict/zillow_out.csv'))

    paths_to_verify = list(zip(['pypy3+tuple', 'pypy3+dict', 'python3+tuple', 'python3+dict', 'tuplex'], paths_to_verify))

    # load first file
    df = pd.read_csv(paths_to_verify[0][1])

    for name, path in paths_to_verify:
        df_test = pd.read_csv(path)

        if df_test.equals(df):
            print('{} => passed'.format(name.ljust(jw)))
        else:
            print('{} => failed!!!'.format(name.ljust(jw)))
            failed = True


    # pyspark paths
    def cat_pyspark(path, output_path):
        """ concatenates part files of pyspark"""
        filenames = sorted(glob.glob(os.path.join(path, '*part*')))
        with open(output_path, 'w') as fout, fileinput.input(filenames) as fin:
            for line in fin:
                fout.write(line)


    for pyspark_path in sorted(glob.glob(os.path.join(root_path, 'pyspark*'))):
        name = os.path.basename(pyspark_path).replace('_', '+')

        cat_path = os.path.join(pyspark_path, 'all.csv')
        cat_pyspark(pyspark_path, cat_path)
        df_test = pd.read_csv(cat_path)

        if df_test.equals(df):
            print('{} => passed'.format(name.ljust(jw)))
        else:
            print('{} => failed!!!'.format(name.ljust(jw)))
            failed = True


    def merge_equality_test(df1, df2):
        df_all = df1.merge(df2.drop_duplicates(),
                           how='left', indicator=True)
        # if all are both, then ok.
        return len(df_all[df_all['_merge'] != 'both']) == 0 and len(df_all) == len(df1) and len(df_all) == len(df2)

    # Tuplex, Dask, Pandas, ...
    def load_paths(path):
        """ load all csvs in folder """
        filenames = sorted(glob.glob(os.path.join(path, '*part*')), key=lambda p: int(re.sub('[^0-9]', '', os.path.basename(p))))
        df = pd.DataFrame()
        for path in filenames:
            df = pd.concat((df, pd.read_csv(path)))
        return df

    name = 'dask'
    dask_path = os.path.join(root_path, 'dask_output')
    df_test = load_paths(dask_path)

    if merge_equality_test(df, df_test):
        print('{} => passed'.format(name.ljust(jw)))
    else:
        print('{} => failed!!!'.format(name.ljust(jw)))
        failed = True

    name = 'pandas'
    pandas_path = os.path.join(root_path, 'pandas_output', 'zillow_out.csv')
    df_test = pd.read_csv(pandas_path)

    if merge_equality_test(df, df_test):
        print('{} => passed'.format(name.ljust(jw)))
    else:
        print('{} => failed!!!'.format(name.ljust(jw)))
        failed = True

    name = 'tuplex'
    tuplex_path = os.path.join(root_path, 'tuplex_output')
    df_test = load_paths(tuplex_path)

    if merge_equality_test(df, df_test):
        print('{} => passed'.format(name.ljust(jw)))
    else:
        print('{} => failed!!!'.format(name.ljust(jw)))
        failed = True

    print('done!')
    sys.exit(failed)