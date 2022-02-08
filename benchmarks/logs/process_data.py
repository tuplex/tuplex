#!/usr/bin/env python3
import glob
import os
import argparse

if __name__ == '__main__':
    # parse the arguments
    parser = argparse.ArgumentParser(description="Apache data cleaning + join")
    parser.add_argument(
        "--input-path",
        type=str,
        dest="input_path",
        default="/data/logs",
        help="raw logs path",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        dest="output_path",
        default="/data/logs_clean",
        help="raw logs path",
    )

    args = parser.parse_args()

    input_dir = os.path.join(args.input_path, "*.*.*.txt")
    output_dir = args.output_path

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    num_skipped = 0
    for filename in glob.glob(input_dir):
        new_filename = f'{output_dir}/{filename[filename.rfind("/")+1:]}'

        if os.path.isfile(new_filename):
            num_skipped += 1
        else:
            print(f'{filename} -> {new_filename}')
            with open(filename, "r", encoding='latin_1') as f:
                with open(new_filename, 'w') as fo:
                    for l in f:
                        test = l.encode('ascii', 'replace').decode('ascii') # make it ascii
                        test = test.replace('\0', '?') # get rid of null characters
                        fo.write(test)
    if num_skipped > 0:
        print('skipped {} files'.format(num_skipped))
    print('Done.')
