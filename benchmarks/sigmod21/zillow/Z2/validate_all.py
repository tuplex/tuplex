#!/usr/bin/env python3
# (c) L.Spiegelberg jw17-jwjw

# validates all outputs. Run in validate.sh


import pandas as pd
import os
import glob
import fileinput
import sys
import re
import numpy as np

if __name__ == '__main__':
    failed = False

    jw = 25

    print('>>> overview of output for different frameworks:')
    root_path = './validation_output'

    # first, count lines for each framework (excl. header)
    g = glob.glob(os.path.join(root_path, '*'))
    w_name = max(map(lambda x: len(os.path.basename(x)), g))
    for dir in filter(os.path.isdir, sorted(g)):
        name = os.path.basename(dir)

        files = glob.glob(os.path.join(dir, '*.csv'))
        # dask + pyspark output schemes
        pyspark_files = glob.glob(os.path.join(dir, 'part-*'))
        files += pyspark_files
        dask_files = glob.glob(os.path.join(dir, '*.part'))
        files += dask_files
        files = list(set(files))
        num_files = len(files)

        # count lines (excl. header)
        num_lines = 0
        total_size = 0
        for f in files:
            total_size += os.path.getsize(f)
            # fast line count, hack from https://stackoverflow.com/questions/9629179/python-counting-lines-in-a-huge-10gb-file-as-fast-as-possible
            line_count = sum(1 for i in open(f, 'rb'))
            # check for header and ignore it
            with open(f, 'r') as fp:
                header = fp.readline()
                if header == 'url,zipcode,address,city,state,bedrooms,bathrooms,sqft,offer,type,price\n':
                    line_count -= 1

            num_lines += line_count

        print('{} {} files {} rows {} bytes'.format(name.ljust(w_name), str(num_files).rjust(8), str(num_lines).rjust(12), str(total_size).rjust(12)))


    print('>>> merging output contents...')
    output_schema = {'url': str, 'zipcode': str, 'address': str,
                     'city': str, 'state': str, 'bedrooms': np.int64,
                     'bathrooms': np.float64, 'sqft': np.int64,
                     'offer': str, 'type': str, 'price': np.int64}

    print('-- creating concatenated versions for tuplex, pyspark, dask')
    # Tuplex
    tplx_dest_path = os.path.join(root_path, 'tuplex/zillow_out.csv')
    tplx_parts = glob.glob(os.path.join(root_path, 'tuplex/part*.csv'))
    if not os.path.exists(tplx_dest_path):
        with open(tplx_dest_path, 'w') as fp_out:
            for i, path in enumerate(sorted(tplx_parts)):
                with open(path, 'r') as fp:
                    lines = fp.readlines()
                if i == 0:
                    fp_out.writelines(lines)
                else:
                    fp_out.writelines(lines[1:])
            # remove original files
            for file_path in tplx_parts:
                os.remove(file_path)
    print(' -- tuplex parts merged')

    # PySpark
    def cat_pyspark(path, output_path):
        """ concatenates part files of pyspark"""
        if not os.path.exists(output_path):
            filenames = sorted(glob.glob(os.path.join(path, '*part*')))
            with open(output_path, 'w') as fout, fileinput.input(filenames) as fin:

                # write header once, then neverever again
                fout.write('url,zipcode,address,city,state,bedrooms,bathrooms,sqft,offer,type,price\n')
                for line in fin:
                    if line != 'url,zipcode,address,city,state,bedrooms,bathrooms,sqft,offer,type,price\n':
                        fout.write(line)

            # remove original files
            for file_path in filenames:
                os.remove(file_path)

    for pyspark_path in sorted(glob.glob(os.path.join(root_path, '*spark*'))):
        name = os.path.basename(pyspark_path).replace('_', '+')

        cat_path = os.path.join(pyspark_path, 'zillow_out.csv')
        cat_pyspark(pyspark_path, cat_path)
    print(' -- pyspark parts merged')

    # merge dask parts
    def merge_dask_paths(path, output_path):
        """ load all csvs in folder """
        if not os.path.exists(output_path):
            filenames = sorted(glob.glob(os.path.join(path, '*part*')), key=lambda p: int(re.sub('[^0-9]', '', os.path.basename(p))))
            df = pd.DataFrame()
            for path in filenames:
                df = pd.concat((df, pd.read_csv(path, header=0, dtype=output_schema)))
            df.to_csv(output_path, index=None)

    dask_path = os.path.join(root_path, 'dask_output')
    merge_dask_paths(dask_path, os.path.join(dask_path, 'zillow_out.csv'))
    print(' -- dask paths merged')

    print('>>> verifying output contents...')

    # check contents for python, ...
    paths_to_verify = []
    paths_to_verify.append(os.path.join(root_path, 'pypy3_tuple/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'pypy3_dict/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'python3_tuple/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'python3_dict/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'tuplex/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'cc_no_preload_output/part0.csv'))
    paths_to_verify.append(os.path.join(root_path, 'cc_preload_output/part0.csv'))
    paths_to_verify.append(os.path.join(root_path, 'scala/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'cython3_tuple/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'cython3_dict/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'nuitka_tuple/zillow_out.csv'))
    paths_to_verify.append(os.path.join(root_path, 'nuitka_dict/zillow_out.csv'))

    paths_to_verify = list(zip(['pypy3+tuple', 'pypy3+dict', 'python3+tuple', 'python3+dict',
                                'tuplex', 'c++ (no preload)', 'c++ (preload)', 'pure scala',
                                'cython+tuple', 'cython+dict', 'nuitka+tuple', 'nuitka+dict'], paths_to_verify))

    # load first file
    df = pd.read_csv(paths_to_verify[0][1])

    for name, path in paths_to_verify:
        df_test = pd.read_csv(path)

        if df_test.equals(df):
            print('{} => passed'.format(name.ljust(jw)))
        else:
            print('{} => failed!!!'.format(name.ljust(jw)))
            failed = True

    # load tuplex as reference dataframe
    df = pd.read_csv(os.path.join(root_path, 'tuplex', 'zillow_out.csv'), dtype=output_schema, header=0)

    def merge_equality_test(df1, df2):
        df_all = df1.merge(df2.drop_duplicates(),
                           how='left', indicator=True)
        # if all are both, then ok.
        return len(df_all[df_all['_merge'] != 'both']) == 0 and len(df_all) == len(df1) and len(df_all) == len(df2)

    # Check puspark equality
    for pyspark_path in sorted(glob.glob(os.path.join(root_path, '*spark*'))):
        name = os.path.basename(pyspark_path).replace('_', '+')

        df_test = pd.read_csv(os.path.join(pyspark_path, 'zillow_out.csv'), header=0, dtype=output_schema)
        if merge_equality_test(df, df_test):
            print('{} => passed'.format(name.ljust(jw)))
        else:
            print('{} => failed!!!'.format(name.ljust(jw)))
            failed = True

    # Tuplex, Dask, Pandas, ...
    name = 'dask'
    dask_path = os.path.join(root_path, 'dask_output', 'zillow_out.csv')
    df_test = pd.read_csv(dask_path, header=0, dtype=output_schema)

    if merge_equality_test(df, df_test):
        print('{} => passed'.format(name.ljust(jw)))
    else:
        print('{} => failed!!!'.format(name.ljust(jw)))
        failed = True

    name = 'pandas'
    pandas_path = os.path.join(root_path, 'pandas_output', 'zillow_out.csv')
    df_test = pd.read_csv(pandas_path, header=0, dtype=output_schema)

    if merge_equality_test(df, df_test):
        print('{} => passed'.format(name.ljust(jw)))
    else:
        print('{} => failed!!!'.format(name.ljust(jw)))
        failed = True

    print('done!')

    if not failed:
        print('===> All outputs fine!')

    sys.exit(failed)