#!/usr/bin/env python3
# creates the zip file to deploy to Lambda, adapted from https://github.com/awslabs/aws-lambda-cpp/blob/9df704157539388b091ff0936f79c34d4ca6993d/packaging/packager
# python script is easier to read though/adapt

import os
import sys
import zipfile
import subprocess
import tempfile
import logging
import shutil
import re
import glob
import stat
import argparse
from tqdm import tqdm

def cmd_exists(cmd):
    """
    checks whether command `cmd` exists or not
    Args:
        cmd: executable or script to check for existence

    Returns: True if it exists else False

    """

    #TODO: better use type pacman > /dev/null 2>&1?
    return shutil.which(cmd) is not None

def get_list_result_from_cmd(cmd, timeout=2):
    p = subprocess.Popen(cmd, stdin=None, close_fds=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate(timeout=timeout)

    if stderr is not None and len(stderr) > 0:
        logging.error("FAILURE")
        logging.error(stderr)
        return []

    if stdout is None or 0 == len(stdout):
        return []

    return stdout.decode().split('\n')

def query_libc_shared_objects(NO_LIBC):
    # use pacman, dpkg, rpm to query libc files...
    libc_files = []

    # for each command check whether it exists
    pacman_files = get_list_result_from_cmd(['pacman', '--files', '--list', '--quiet', 'glibc']) if cmd_exists('pacman') else []
    dpkg_files = get_list_result_from_cmd(['dpkg-query', '--listfiles', 'libc6']) if cmd_exists(
        'dpkg-query') else []
    rpm_files = get_list_result_from_cmd(['rpm', '--query', '--list', 'glibc']) if cmd_exists(
        'rpm') else []

    # filter so only shared objects are contained...
    libc_files = pacman_files + dpkg_files + rpm_files
    libc_files = list(filter(lambda path: re.search(r"\.so$|\.so\.[0-9]+$", path), libc_files))

    if not NO_LIBC:
        assert len(libc_files) > 0, 'Could not retrieve any LIBC files. Broken?'

    return libc_files


def main():
    # set logging level here
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    parser = argparse.ArgumentParser(description='Lambda zip packager')
    parser.add_argument('-o', '--output', type=str, dest='OUTPUT_FILE_NAME', default='tplxlam.zip',
                        help='output path where to write zip file')
    parser.add_argument('-i', '--input', type=str, dest='TPLXLAM_BINARY', default=os.path.join('dist/bin', 'tplxlam'),
                        help='input path of tplx binary')
    parser.add_argument('-r', '--runtime', dest='TPLX_RUNTIME_LIBRARY', type=str, default=os.path.join('dist/bin', 'tuplex_runtime.so'),
                        help="whether to resolve exceptions in order")
    parser.add_argument('-p', '--python', dest='PYTHON3_EXECUTABLE', type=str,
                        default='/opt/lambda-python/bin/python3.8',
                        help='path to python executable from which to package stdlib.')
    parser.add_argument('--nolibc', dest='NO_LIBC', action="store_false",
                        help="whether to skip packaging libc files or not")
    args = parser.parse_args()


    OUTPUT_FILE_NAME=args.OUTPUT_FILE_NAME
    TPLXLAM_BINARY=args.TPLXLAM_BINARY
    TPLX_RUNTIME_LIBRARY=args.TPLX_RUNTIME_LIBRARY
    ## why is python3 needed?
    PYTHON3_EXECUTABLE=args.PYTHON3_EXECUTABLE
    NO_LIBC=args.NO_LIBC

    INCLUDE_LIBC= not NO_LIBC

    # bootstrap scripts

    # use this script here when libc is included => requires package loader
    bootstrap_script="""#!/bin/bash
    set -euo pipefail
    export AWS_EXECUTION_ENV=lambda-cpp
    exec $LAMBDA_TASK_ROOT/lib/{} --library-path $LAMBDA_TASK_ROOT/lib $LAMBDA_TASK_ROOT/bin/tplxlam ${_HANDLER}
    """

    # use this script when libc is not included
    bootstrap_script_nolibc="""#!/bin/bash
    set -euo pipefail
    export AWS_EXECUTION_ENV=lambda-cpp
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LAMBDA_TASK_ROOT/lib
    exec $LAMBDA_TASK_ROOT/bin/$PKG_BIN_FILENAME ${_HANDLER}
    """

    pkg_loader = 'ld-linux-x86-64.so.2' # change to whatever is in dependencies...

    # find python files
    logging.info('Python3 executable: {}'.format(PYTHON3_EXECUTABLE))
    py_stdlib_path = get_list_result_from_cmd([PYTHON3_EXECUTABLE, '-c', 'import sysconfig; print(sysconfig.get_path(\'stdlib\'))'])[0]
    py_site_packages_path = get_list_result_from_cmd([PYTHON3_EXECUTABLE, '-c', 'import sysconfig; print(sysconfig.get_path(\'purelib\'))'])[0]
    py_version = get_list_result_from_cmd([PYTHON3_EXECUTABLE, '-c', 'import sys; print(\'{}.{}\'.format(sys.version_info.major,sys.version_info.minor))'])[0]
    logging.info('Found Python standard lib in {}'.format(py_stdlib_path))
    logging.info('Found Python packages in {}'.format(py_site_packages_path))
    logging.info('Version of Python to package is {}'.format(py_version))

    # find all libc dependencies
    libc_libs = []
    if not NO_LIBC:
        libc_libs = query_libc_shared_objects(NO_LIBC)
        logging.info('Found {} files comprising LIBC'.format(len(libc_libs)))
    else:
        logging.info('NO_LIBC passed, make sure to have built everything on Amazon Linux 2 machine.')

    # use file with ld- as loader!

    # find dependencies using ldd
    # -> for both binary AND runtime

    ldd_dependencies = get_list_result_from_cmd(['ldd', TPLXLAM_BINARY])
    ldd_dependencies = list(map(lambda line: line.strip(), ldd_dependencies))

    # for each line, extract name, original_path
    def extract_from_ldd(line):
        if '=>' not in line:
            return '', ''

        parts = line.split('=>')
        head = parts[0]
        tail = parts[-1]
        name = head.strip()
        path = tail[:tail.find('(')].strip()

        return name, path

    # get pkg_loader name
    for line in ldd_dependencies:
        line = line.strip()
        if line == '':
            continue
        head = line.split()[0]
        if os.path.basename(head).startswith('ld-'):
            pkg_loader = os.path.basename(head)

    logging.info('Found package loader {}'.format(pkg_loader))

    # exclude where no files are (i.e. linux-vdso)
    ldd_dependencies = list(filter(lambda t: t[1] != '', map(extract_from_ldd, ldd_dependencies)))

    logging.info('Found {} dependencies'.format(len(ldd_dependencies)))
    #
    # # find pkg loader
    # for path in libc_libs:
    #     filename = os.path.basename(path)
    #     if filename.startswith('ld-'):
    #         logging.info('Found package loader {}'.format(filename))
    #         pkg_loader = filename

    #compression=zipfile.ZIP_BZIP2
    #compression=zipfile.ZIP_DEFLATED # this is the only one that works for MacOS
    compression=zipfile.ZIP_LZMA # use this for final file, because smaller!

    def create_zip_link(zip, link_source, link_target):
        zipInfo = zipfile.ZipInfo(link_source)
        zipInfo.create_system = 3  # System which created ZIP archive, 3 = Unix; 0 = Windows
        unix_st_mode = stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH
        zipInfo.external_attr = unix_st_mode << 16  # The Python zipfile module accepts the 16-bit "Mode" field (that stores st_mode field from struct stat, containing user/group/other permissions, setuid/setgid and symlink info, etc) of the ASi extra block for Unix as bits 16-31 of the external_attr
        zip.writestr(zipInfo, link_target)

    with zipfile.ZipFile(OUTPUT_FILE_NAME, 'w', compression=compression) as zip:
        logging.info('Writing bootstrap script {}'.format('NO_LIBC=True' if NO_LIBC else ''))
        if INCLUDE_LIBC:
            zip.writestr('bootstrap', bootstrap_script)
        else:
            zip.writestr('bootstrap', bootstrap_script_nolibc.format(pkg_loader))

        # adding actual execution scripts
        logging.info('Writing C++ binary')
        zip.write(TPLXLAM_BINARY, 'bin/' + os.path.basename(TPLXLAM_BINARY))

        # copy libc
        if INCLUDE_LIBC:
            logging.info('Writing libc files')
            for path in libc_libs:
                try:
                    # # for links, just write linked version to decrease size...
                    # # if that fails, simply only go for the else branch...
                    # if os.path.islink(path):
                    #     # cf. https://stackoverflow.com/questions/35782941/archiving-symlinks-with-python-zipfile on optimization
                    #     link_source = path
                    #     link_target = os.readlink(path)
                    #     logging.debug('Found Link: {} -> {}, writing link to archive...'.format(link_source, link_target))
                    #     create_zip_link(zip, link_source, link_target)
                    # else:
                    #     zip.write(path, os.path.join('lib/', os.path.basename(path)))

                    zip.write(path, os.path.join('lib/', os.path.basename(path)))
                except FileNotFoundError as e:
                    logging.warning('Could not find libc file {}, details: {}'.format(os.path.basename(path), e))

        logging.info('writing dependencies...')
        # write dependencies, skip whatever is in libc

        libc_libnames = set(map(lambda path: os.path.basename(path), libc_libs))

        for name, path in set(ldd_dependencies):
            if name in libc_libnames:
                continue

            # if os.path.islink(path):
            #     # cf. https://stackoverflow.com/questions/35782941/archiving-symlinks-with-python-zipfile on optimization
            #     link_source = path
            #     link_target = os.readlink(path)
            #     logging.debug('Found Link: {} -> {}, writing link to archive...'.format(link_source, link_target))
            #     create_zip_link(zip, link_source, link_target)
            # else:
            #     zip.write(path, os.path.join('lib', name))
            zip.write(path, os.path.join('lib', name))


        # now copy in Python lib from specified python executable!
        # TODO: compile them to pyc files, this should lead to smaller size...

        logging.info('Writing Python stdlib from {}'.format(py_stdlib_path))
        root_dir = py_stdlib_path

        paths = list(filter(os.path.isfile, glob.iglob(root_dir + '**/**', recursive=True)))

        # exclude numpy files...
        paths = list(filter(lambda path: 'numpy' not in path, paths))

        # TODO: exclude more files here to make this smaller and still keep it executable!!!

        logging.info('Found {} files in python stdlib to ship'.format(len(paths)))
        # for path in glob.iglob(root_dir + '**/**', recursive=True):
        #     if not os.path.isfile(path):
        #         continue

        py_arch_root = os.path.join('lib', 'python{}'.format(py_version))
        logging.info('Writing Python stdlib to path {} in archive'.format(py_arch_root))

        if not root_dir.endswith('/'):
            root_dir += '/'

        # There are a couple large files in the stdlib that should get excluded...
        # -> e.g. libpython3.8.a is 59.1MB
        # -> also the pip whl is 15.4MB
        # # get file sizes, list top5 largest files...
        # file_infos = list(map(lambda path: (path, os.stat(path).st_size), paths))
        # file_infos = sorted(file_infos, key=lambda t: -t[1])
        # file_infos = list(map(lambda t: (t[0], t[1])))
        # print(file_infos[:5])

        def exclude_from_packaging(path):
            if path.endswith('libpython3.8.a'):
                logging.info('Excluding libpython3.8a from runtime')
                return False

            # exclude pyc cached files
            if '__pycache__' in path:
                return False

            # exclude test/ folder
            if 'test/' in path:
                return False

            # exclude turtledemo
            if 'turtledemo/' in path:
                return False

            # keep.
            return True

        # exclude here certain paths
        num_before_exclusion = len(paths)
        paths = list(filter(exclude_from_packaging, paths))
        logging.info('Excluding {} files from runtime...'.format(num_before_exclusion - len(paths)))

        for path in tqdm(paths):
            # perform link optimization??
            # copy to lib/python<maj>.<min>
            target = os.path.join(py_arch_root, path.replace(root_dir, ''))
            logging.debug('{} -> {}'.format(path, target))
            zip.write(path, target)

    logging.info('Done!')

if __name__ == '__main__':
    main()