import glob
import os
import shutil
import argparse

parser = argparse.ArgumentParser(description='create data folders so dask can work')
parser.add_argument('--src', dest='src_path', help='src data dir of flights')
parser.add_argument('--dest', dest='dest_folder', help='target directory')

args = parser.parse_args()

assert args.src_path and args.dest_folder

paths = sorted(glob.glob(os.path.join(args.src_path, 'flights*.csv')))

for i in range(len(paths)):
    dest_dir = os.path.join(args.dest_folder, 'flights-months-{}'.format(i))
    os.mkdir(dest_dir)
    print('>>> copying files to {}'.format(dest_dir))
    
    [shutil.copyfile(path, os.path.join(dest_dir, os.path.basename(path))) for path in paths[:i]]
