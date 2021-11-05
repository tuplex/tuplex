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
from tqdm import tqdm

# set logging level here
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

OUTPUT_FILE_NAME='lam.zip'
TPLXLAM_BINARY=os.path.join('dist/bin', 'tplxlam')
TPLX_RUNTIME_LIBRARY=os.path.join('dist/bin', 'tuplex_runtime.so')
## why is python3 needed?
PYTHON3_EXECUTABLE='/opt/lambda-python/bin/python3.8'

# bootstrap scripts
bootstrap_script="""#!/bin/bash
set -euo pipefail
export AWS_EXECUTION_ENV=lambda-cpp
exec $LAMBDA_TASK_ROOT/lib/{} --library-path $LAMBDA_TASK_ROOT/lib $LAMBDA_TASK_ROOT/bin/tplxlam ${_HANDLER}
"""

bootstrap_script_nolibc="""#!/bin/bash
set -euo pipefail
export AWS_EXECUTION_ENV=lambda-cpp
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LAMBDA_TASK_ROOT/lib
exec $LAMBDA_TASK_ROOT/bin/$PKG_BIN_FILENAME ${_HANDLER}
"""

NO_LIBC=False



libc_script="""#!/usr/bin/env bash

function package_libc_via_pacman {
    if [[ $(cat /etc/os-release | sed '/ID_LIKE/!d;s/ID_LIKE=//') == "archlinux" ]]; then
        if type pacman > /dev/null 2>&1; then
            echo "$(pacman --files --list --quiet glibc | sed -E '/\.so$|\.so\.[0-9]+$/!d')"
        fi
    fi
}

function package_libc_via_dpkg() {
    if type dpkg-query > /dev/null 2>&1; then
        if [ $(dpkg-query --listfiles libc6 | wc -l) -gt 0 ]; then
            echo "(dpkg-query --listfiles libc6 | sed -E '/\.so$|\.so\.[0-9]+$/!d')"
        fi
    fi
}

function package_libc_via_rpm() {
    if type rpm > /dev/null 2>&1; then
       if [ $(rpm --query --list --quiet glibc | wc -l) -gt 0 ]; then
           echo "$(rpm --query --list glibc | sed -E '/\.so$|\.so\.[0-9]+$/!d')"
       fi
    fi
}

libc_libs=()
libc_libs+=$(package_libc_via_dpkg)
libc_libs+=$(package_libc_via_rpm)
libc_libs+=$(package_libc_via_pacman)

for i in $libc_libs; do
    if [[ ! -f $i ]]; then # ignore linux-vdso.so.1
        continue
    fi

    # Do not copy libc files which are directly linked
    matched=$(echo $libc_libs | grep --count $i) || true # prevent the non-zero exit status from terminating the script
    if [ $matched -gt 0 ]; then
        continue
    fi

    echo $i
done
"""

pkg_loader = 'ld-linux-x86-64.so.2' # change to whatever is in dependencies...

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

def query_libc_shared_objects():
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
    libc_libs = query_libc_shared_objects()
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

with zipfile.ZipFile(OUTPUT_FILE_NAME, 'w', compression=zipfile.ZIP_LZMA) as zip:
    # logging.info('Writing bootstrap script {}'.format('NO_LIBC=True' if NO_LIBC else ''))
    # if NO_LIBC:
    #     zip.writestr('bootstrap', bootstrap_script_nolibc.format(pkg_loader))
    # else:
    #     zip.writestr('bootstrap', bootstrap_script)
    #
    # # adding actual execution scripts
    # logging.info('Writing C++ binary')
    # zip.write(TPLXLAM_BINARY, 'bin/' + os.path.basename(TPLXLAM_BINARY))
    #
    # # copy libc
    # if not NO_LIBC:
    #     logging.info('Writing libc files')
    #     for path in libc_libs:
    #         # TODO: what about links? --> prob. get dereferenced which increaseses size...
    #
    #         if os.path.islink(path):
    #             # cf. https://stackoverflow.com/questions/35782941/archiving-symlinks-with-python-zipfile on optimization
    #             logging.warning('{} is a link, could be optimized'.format(path))
    #         try:
    #             zip.write(path, os.path.join('lib/', os.path.basename(path)))
    #         except FileNotFoundError as e:
    #             logging.warning('Could not find libc file {}, details: {}'.format(os.path.basename(path), e))
    #
    # logging.info('writing dependencies...')
    # # write dependencies, skip whatever is in libc
    #
    # libc_libnames = set(map(lambda path: os.path.basename(path), libc_libs))
    #
    # for name, path in set(ldd_dependencies):
    #     if name in libc_libnames:
    #         continue
    #
    #     if os.path.islink(path):
    #         # cf. https://stackoverflow.com/questions/35782941/archiving-symlinks-with-python-zipfile on optimization
    #         logging.warning('{} is a link, could be optimized'.format(path))
    #
    #     zip.write(path, os.path.join('lib', name))


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
    for path in tqdm(paths):
        # perform link optimization??
        # copy to lib/python<maj>.<min>
        target = os.path.join('lib', 'python{}'.format(py_version), path.replace(root_dir, ''))
        logging.debug('{} -> {}'.format(path, target))
        zip.write(path, target)


logging.info('Done!')