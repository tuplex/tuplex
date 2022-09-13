#!/usr/bin/env python3
# (c) L.Spiegelberg 2020
# creates a version of the cpython interpreter which allows to have
# lambda expressions with closures.
import sys
import argparse
import os
import requests
import re
import tarfile
from tqdm.auto import tqdm

def download_file(url, path=None, chunk_size=128 * 1024):
    def get_filename_from_cd(url, cd):
        fname = ''
        if url.find('/'):
            fname = url.rsplit('/', 1)[1]
        if fname and fname.find('.'):
            return fname
        if not cd:
            return None
        fname = re.findall('filename=(.+)', cd)
        if len(fname) == 0:
            return None
        return fname[0]

    r = requests.get(url, stream=True, allow_redirects=True)

    if not path:
        path = get_filename_from_cd(url, r.headers.get('content-disposition'))

    # get filename from either url or contents
    header = r.headers
    content_type = header.get('content-type')
    content_length = header.get('content-length', None)

    with tqdm.wrapattr(open(path, "wb"), "write", miniters=1,
                       total=int(content_length),
                       desc=os.path.basename(path)) as fp:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                fp.write(chunk)
        fp.flush()
        fp.close()
    return path


def patch_codeobject(src_path):
    fpath = os.path.join(src_path, 'Objects', 'codeobject.c')
    content = open(fpath).read()
    num_lines = len(content.splitlines())
    func_idx = content.find('PyCode_NewWithPosOnlyArgs')

    subcontent = content[func_idx:]
    before_needle = '    co->co_firstlineno = firstlineno;'
    patch_idx = subcontent.find(before_needle)

    patch = '\n    co->co_firstcolno = 0; // (PATCHED)'
    patched_content = content[:func_idx] + subcontent[:patch_idx] + \
                      before_needle + patch + subcontent[patch_idx + len(before_needle):]

    # use sub now
    needle = """    {"co_firstlineno", T_INT,           OFF(co_firstlineno),     READONLY},"""
    repl = """    {"co_firstlineno",  T_INT,          OFF(co_firstlineno),     READONLY},
    {"co_firstcolno",   T_INT,          OFF(co_firstcolno),      READONLY},"""
    patched_content = patched_content.replace(needle, repl)
    if len(patched_content.splitlines()) != num_lines + 2:
        raise Exception('patch not successfully applied')
    with open(fpath, 'w') as fp:
        fp.write(patched_content)


def patch_codeobject_header(src_path):
    fpath = os.path.join(src_path, 'Include', 'cpython', 'code.h')
    content = open(fpath).read()
    num_lines = len(content.splitlines())

    needle = """    int co_firstlineno;         /* first source line number */"""
    repl   = """    int co_firstlineno;         /* first source line number */
    int co_firstcolno;          /* first source column number (PATCHED) */"""
    patched_content = content.replace(needle, repl)
    if len(patched_content.splitlines()) != num_lines + 1:
        raise Exception('patch not successfully applied')
    with open(fpath, 'w') as fp:
        fp.write(patched_content)


def patch_compile(src_path):
    fpath = os.path.join(src_path, 'Python', 'compile.c')
    content = open(fpath).read()
    num_lines = len(content.splitlines())
    func_idx = content.find('compiler_lambda(struct compiler *c, expr_ty e)')

    subcontent = content[func_idx:]
    before_needle = 'Py_INCREF(qualname);'
    patch_idx = subcontent.find(before_needle)

    patch = '\n    co->co_firstcolno = e->col_offset; // (PATCHED)'
    patched_content = content[:func_idx] + subcontent[:patch_idx] + \
                      before_needle + patch + subcontent[patch_idx + len(before_needle):]

    if len(patched_content.splitlines()) != num_lines + 1:
        raise Exception('patch not successfully applied')
    with open(fpath, 'w') as fp:
        fp.write(patched_content)


def main():
    parser = argparse.ArgumentParser(description='Creates a patched cpython version based on the interpreter which is used to invoke this script.')
    parser.add_argument('--backup', dest='backup',
                        default='python_backup',
                        help='where to store the invoked interpreter')
    parser.add_argument('--patched', dest='patched',
                        default='python_patched',
                        help='where to store patched interpreter. Can be then copied over.')
    args = parser.parse_args()

    patched_folder = args.patched
    backup_folder = args.backup
    print('>>> detected python version:\n{}'.format(sys.version))
    version = sys.version_info

    if version.releaselevel != 'final':
        raise Exception('rc versions not supported. Please fix script for them.')

    # url has form https://www.python.org/ftp/python/3.9.0/Python-3.9.0.tgz
    version_str = '{}.{}.{}'.format(version.major, version.minor, version.micro)
    url = 'https://www.python.org/ftp/python/{}/Python-{}.tgz'.format(version_str, version_str)

    print('>>> downloading python version from {}.{}.{} ({}) from {} to {}/'.format(version.major, version.minor, version.micro, version.releaselevel, url, backup_folder))
    os.makedirs(patched_folder, exist_ok=True)
    os.makedirs(backup_folder, exist_ok=True)
    # this here is slow
    py_archive = 'Python-{}.tgz'.format(version_str)
    if not os.path.exists(py_archive):
        py_archive = download_file(url, py_archive)
    else:
        print(' -- already downloaded, skipping step.')

    # unpack file into patched folder
    print('>>> unpacking python source code into {}'.format(patched_folder))
    src_path = os.path.join(patched_folder, py_archive[:py_archive.rfind('.tgz')])
    tf = tarfile.open(py_archive)
    tf.extractall(patched_folder)
    print(' -- source code is in {}'.format(src_path))
    # if not os.path.exists(src_path):
    #
    # else:
    #     print(' -- already extracted, skipping step.')

    # patching code
    print('>>> patching code')

    # add new co_firstcolno attribute to code object
    # there is also the option to use the patch tool together with diff
    # to apply individual patches.
    # Here, a super simple search&replace version is used
    patch_codeobject(src_path)
    patch_codeobject_header(src_path)
    patch_compile(src_path)
    print(' -- files patched (4 lines total in Objects/codeobject.c, Python/compile.c and Include/cpython/code.h')



    # cf. for how to adapt for your platform
    # https://devguide.python.org/setup/
    # to build python, use
    #  ./configure --with-openssl=/usr/local/opt/openssl --prefix=/Users/leonhards/projects/Tuplex/scripts/python_patched/python-3.9.0-dist --enable-shared --enable-optimizations

    print('TODO: compile & install everythgin. For this, make sure paths & co are setup properly...')
    # then, we can copy the resources to the root.
    # for this, create a shell file

    print('hello world')
    print("Python version")
    print (sys.version)
    print("Version info.")
    print (sys.version_info)
    print("Implementation:")
    print(sys.implementation)

    # python 3.9 feature
    # print("system platform dir")
    # print(sys.platlibdir)

    # i.e. this here is what is to backup
    print('prefix: ')
    print(sys.prefix)

    print('C API version')
    print(sys.api_version)


if __name__ == '__main__':
    main()
