#!/usr/bin/env python3
# top-level setuo file to create package uploadable to pypi.
# -*- coding: utf-8 -*-
import os
import sys
import sysconfig as pyconfig
import subprocess
import logging
import shutil
import distutils
import distutils.dir_util
import platform
import shlex
import shutil

from setuptools import setup, Extension, find_packages
from setuptools.command.build_ext import build_ext
from distutils import sysconfig

import fnmatch
import re
import atexit

# configure logging here
logging.basicConfig(level=logging.INFO)


# TODO: add option to install these
test_dependencies = [
'jupyter',
'nbformat',
'prompt_toolkit>=2.0.7',
'pytest>=5.3.2',
]

# Also requires to install MongoDB
webui_dependencies = [
    'gunicorn',
    'eventlet==0.30.0', # newer versions of eventlet have a bug under MacOS
    'flask',
    'flask-socketio',
    'flask-pymongo',
    'iso8601'
]

install_dependencies = [
    'attrs>=19.2.0',
    'dill>=0.2.7.1',
    'pluggy',
    'py>=1.5.2',
    'pygments>=2.4.1',
    'six>=1.11.0',
    'wcwidth>=0.1.7',
    'astor',
    'prompt_toolkit',
    'jedi',
    'cloudpickle>=0.6.1',
    'PyYAML>=3.13',
    'psutil',
    'pymongo'
] + webui_dependencies

def ninja_installed():
    # check whether ninja is on the path
    from distutils.spawn import find_executable
    return find_executable('ninja') is not None

def find_files(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result

def remove_temp_files(build_dir):
    """
    remove temp cmake files but LEAVE files necessary to run ctest.
    """
    paths = set(os.listdir(build_dir)) - {'dist', 'test', 'CTestTestfile.cmake'}
    paths = map(lambda name: os.path.join(build_dir, name), paths)
    for path in paths:
        if os.path.isfile(path):
            os.remove(path)
        else:
            shutil.rmtree(path)

# Convert distutils Windows platform specifiers to CMake -A arguments
PLAT_TO_CMAKE = {
    "win32": "Win32",
    "win-amd64": "x64",
    "win-arm32": "ARM",
    "win-arm64": "ARM64",
}

# A CMakeExtension needs a sourcedir instead of a file list.
# The name must be the _single_ output extension from the CMake build.
# If you need multiple extensions, see scikit-build.
class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=""):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)

class CMakeBuild(build_ext):

    def build_extension(self, ext):

        ext_filename = str(ext.name)
        ext_filename = ext_filename[ext_filename.rfind('.') + 1:]  # i.e. this is "tuplex"
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))

        # required for auto-detection of auxiliary "native" libs
        if not extdir.endswith(os.path.sep):
            extdir += os.path.sep

        cfg = "Debug" if self.debug else "Release"

        # because still alpha, use RelWithDebInfo
        cfg = "Debug" if self.debug else "RelWithDebInfo"

        # force release version
        cfg = "Release"

        # CMake lets you override the generator - we need to check this.
        # Can be set with Conda-Build, for example.
        cmake_generator = os.environ.get("CMAKE_GENERATOR", "")

        py_maj_min = "{}.{}".format(sys.version_info.major, sys.version_info.minor)

        llvm_root = None
        boost_include_dir = None
        py_include_dir = None
        py_libs_dir = None

        # check whether run with cibuildwheel:
        # Note: manylinux2014 does NOT contain the shared objects, therefore
        #       can't build/test testcore etc. => only build tuplex
        if os.environ.get('CIBUILDWHEEL', '0') == '1':
            # run in cibuildwheel, adjust options to fit docker image...

            # e.g., to adjust use:
            # /opt/_internal/cpython-3.7.10/bin/python3-config  --ldflags
            # -L/opt/_internal/cpython-3.7.10/lib/python3.7/config-3.7m-x86_64-linux-gnu -L/opt/_internal/cpython-3.7.10/lib -lpython3.7m -lcrypt -lpthread -ldl  -lutil -lm

            # command that works:
            # cmake -DPython3_INCLUDE_DIRS=/opt/python/cp37-cp37m/include/python3.7/ \
            #       -DPython3_LIBRARY=/opt/python/cp37-cp37m/lib/python3.7/ \
            #       -DBoost_INCLUDE_DIR=/opt/boost/python3.7/include/ \
            #       -DLLVM_ROOT=/usr/lib64/llvm9.0/ ..
            # llvm_root = '/usr/lib64/llvm9.0/' # yum based
            llvm_root = '/opt/llvm-9.0'  # manual install
            boost_include_dir = '/opt/boost/python{}/include/'.format(py_maj_min)
            py_include_dir = pyconfig.get_paths()['include']
            py_libs_dir = pyconfig.get_paths()['stdlib']

            # Mac OS? Use boost python versions!
            # /usr/local/Cellar/boost/1.75.0_2
            if platform.system().lower() == 'darwin':
                # mac os, use brewed versions!
                out_py = subprocess.check_output(['brew', 'info', 'python3']).decode()
                out_boost_py = subprocess.check_output(['brew', 'info', 'boost-python3']).decode()

                print(out_py)
                print(out_boost_py)

                def find_pkg_path(lines):
                    return list(filter(lambda x: 'usr/local' in x, lines.split('\n')))[0]

                out_py = find_pkg_path(out_py)
                out_boost_py = find_pkg_path(out_boost_py)
                print('Found python3 @ {}'.format(out_py))
                print('Found boost-python3 @ {}'.format(out_boost_py))

                # setups find everything automatically...
                llvm_root = None
                boost_include_dir = None
                py_include_dir = None
                py_libs_dir = None

        # Set Python_EXECUTABLE instead if you use PYBIND11_FINDPYTHON
        cmake_args = [
            "-DBUILD_NATIVE=OFF", # disable march=native to avoid issues.
            # "-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={}".format(extdir),
            "-DPYTHON_EXECUTABLE={}".format(sys.executable),
            "-DCMAKE_BUILD_TYPE={}".format(cfg),  # not used on MSVC, but no harm
            "-DPYTHON3_VERSION={}".format(py_maj_min),
        ]

        # add version info if not dev
        version_cmake = "-DVERSION_INFO={}".format(self.distribution.get_version())
        if re.match(r'\d+.\d+.\d+', version_cmake):
            cmake_args.append(version_cmake)

        if llvm_root is not None:
            cmake_args.append('-DLLVM_ROOT={}'.format(llvm_root))
            if os.environ.get('CIBUILDWHEEL', '0') == '1':
                print('setting prefix path...')
                # ci buildwheel?
                # /opt/llvm-9.0/lib/cmake/llvm/
                prefix_path = "/opt/llvm-9.0/lib/cmake/llvm/" #os.path.join(llvm_root, '/lib/cmake/llvm')
                #cmake_args.append('-DCMAKE_PREFIX_PATH={}'.format(prefix_path))
                cmake_args.append('-DLLVM_DIR={}'.format(prefix_path))
                cmake_args.append('-DLLVM_ROOT_DIR={}'.format(llvm_root))

        if py_include_dir is not None:
            cmake_args.append('-DPython3_INCLUDE_DIRS={}'.format(py_include_dir))
        if py_libs_dir is not None:
            cmake_args.append('-DPython3_LIBRARY={}'.format(py_libs_dir))
        if boost_include_dir is not None:
            cmake_args.append('-DBoost_INCLUDE_DIR={}'.format(boost_include_dir))

        build_args = []
        if self.compiler.compiler_type != "msvc":
            # Using Ninja-build since it a) is available as a wheel and b)
            # multithreads automatically. MSVC would require all variables be
            # exported for Ninja to pick it up, which is a little tricky to do.
            # Users can override the generator with CMAKE_GENERATOR in CMake
            # 3.15+.
            if not cmake_generator:

                # yet, check if Ninja exists...
                if ninja_installed():
                    cmake_args += ["-GNinja"]
        else:

            # Single config generators are handled "normally"
            single_config = any(x in cmake_generator for x in {"NMake", "Ninja"})

            # CMake allows an arch-in-generator style for backward compatibility
            contains_arch = any(x in cmake_generator for x in {"ARM", "Win64"})

            # Specify the arch if using MSVC generator, but only if it doesn't
            # contain a backward-compatibility arch spec already in the
            # generator name.
            if not single_config and not contains_arch:
                cmake_args += ["-A", PLAT_TO_CMAKE[self.plat_name]]

            # Multi-config generators have a different way to specify configs
            if not single_config:
                cmake_args += [
                    # "-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}".format(cfg.upper(), extdir)
                ]
                build_args += ["--config", cfg]

        # Set CMAKE_BUILD_PARALLEL_LEVEL to control the parallel build level
        # across all generators.
        if "CMAKE_BUILD_PARALLEL_LEVEL" not in os.environ:
            # self.parallel is a Python 3 only way to set parallel jobs by hand
            # using -j in the build_ext call, not supported by pip or PyPA-build.
            if hasattr(self, "parallel") and self.parallel:
                # CMake 3.12+ only.
                build_args += ["-j{}".format(self.parallel)]

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        ## on cibuildwheel b.c. manylinux2014 does not have python shared objects, build
        ## only tuplex target (the python shared object)
        #if os.environ.get('CIBUILDWHEEL', '0') == '1':

        # because the goal of setup.py is to only build the package, build only target tuplex.
        # changed from before.

        def parse_bool_option(key):
            val = os.environ.get(key, None)
            if not val:
                return False
            if val.lower() == 'on' or val.lower() == 'yes' or val.lower() == 'true' or val.lower() == '1':
                return True
            if val.lower() == 'off' or val.lower() == 'no' or val.lower() == 'false' or val.lower() == '0':
                return True
            return False


        BUILD_ALL = parse_bool_option('TUPLEX_BUILD_ALL')
        if BUILD_ALL is True:
            # build everything incl. all google tests...
            logging.info('Building all Tuplex targets (incl. tests)...')
        else:
            # restrict to shared object only...
            logging.info('Building only shared objects...')
            build_args += ['--target', 'tuplex']

        # hack: only run for first invocation!
        if ext_filename == 'tuplex_runtime':
            return

        # check environment variable CMAKE_ARGS and overwrite whichever args are passed there
        if len(os.environ.get('CMAKE_ARGS', '')) > 0:
            extra_args = shlex.split(os.environ['CMAKE_ARGS'])

            print(cmake_args)
            for arg in extra_args:
                # cmake option in the style of -D/-G=?
                m = re.search("-[DG][a-zA-z_]+=", arg)
                if m:
                    # search for substring in existing args, if found replace!
                    idxs = list(filter(lambda t: t[0].lower().strip().startswith(m[0].lower()), zip(cmake_args, range(len(cmake_args)))))
                    if len(idxs) > 0:
                        idx = idxs[0][1]
                        cmake_args[idx] = arg
                    else:
                        # append!
                        cmake_args.append(arg)
                else:
                    # append
                    cmake_args.append(arg)

        logging.info('configuring cmake with: {}'.format(' '.join(["cmake", ext.sourcedir] + cmake_args)))
        logging.info('compiling with: {}'.format(' '.join(["cmake", "--build", "."] + build_args)))
        subprocess.check_call(
            ["cmake", ext.sourcedir] + cmake_args, cwd=self.build_temp
        )
        logging.info('configuration done, workdir={}'.format(self.build_temp))
        subprocess.check_call(
            ["cmake", "--build", "."] + build_args, cwd=self.build_temp
        )

        # this helps to search paths in doubt
        # print('searching for .so files in {}'.format(self.build_temp))
        # subprocess.check_call(['find', '.', '-name', '*.so'], cwd = self.build_temp)
        # subprocess.check_call(['find', '.', '-name', '*.so'], cwd = ext.sourcedir)

        # check whether files can be located, if this doesn't work, search for files!
        tuplexso_path = os.path.join('dist', 'python', 'tuplex', 'libexec', 'tuplex.so')
        src_runtime = os.path.join('dist', 'python', 'tuplex', 'libexec', 'tuplex_runtime.so')

        if not os.path.isfile(tuplexso_path):
            print('Could not find file tuplex.so under {}, searching for it...'.format(tuplexso_path))
            paths = find_files("*tuplex.so", self.build_temp)
            assert len(paths) > 0, 'did not find any file under {}'.format(self.build_temp)
            print('Found following paths: {}'.format(''.join(paths)))
            print('Using {}'.format(paths[0]))
            tuplexso_path = paths[0]

        if not os.path.isfile(src_runtime):
            print('Could not find tuplex_runtime under {}, searching for it...'.format(tuplexso_path))
            paths = find_files("*tuplex_runtime*.*", self.build_temp)
            assert len(paths) > 0, 'did not find any file under {}'.format(self.build_temp)
            print('Found following paths: {}'.format(''.join(paths)))
            print('Using {}'.format(paths[0]))
            src_runtime = paths[0]

        # copy over modules so that setup.py picks them up.
        # i.e. according to current setup, the file is expected to be in
        # build/lib.macosx-10.15-x86_64-3.9/tuplex/libexec/tuplex.cpython-39-darwin.so e.g. for Mac OS X
        ext_suffix = sysconfig.get_config_var('EXT_SUFFIX')
        target_path = os.path.join(extdir, ext_filename + ext_suffix)
        print('target path is: {}'.format(target_path))
        os.makedirs(extdir, exist_ok=True)

        # copy file from build temp dir
        shutil.copyfile(tuplexso_path, target_path)
        if not os.path.isfile(src_runtime):
            src_runtime = src_runtime.replace('.so', '.dylib')
            assert os.path.isfile(src_runtime), 'Tuplex runtime does not exist'

        runtime_target = os.path.join(extdir, 'tuplex_runtime' + ext_suffix)
        shutil.copyfile(src_runtime, runtime_target)

        # run clean, to reclaim space
        # also remove third_party folder, because it is big!

        # this will remove test executables as well...
        if not BUILD_ALL:
            logging.info('Running cmake clean target to reclaim space')
            subprocess.check_call(
                ['cmake', '--build', '.', '--target', 'clean'], cwd=self.build_temp
            )
        else:
            # when build all is hit, preserve test files
            # i.e. need folders test, dist and CTestTestfile.cmake
            logging.info('Removing temporary build files, preserving test files...')
            remove_temp_files(self.build_temp)

        subprocess.check_call(
            ['rm', '-rf', 'third_party'], cwd=self.build_temp
        )


def get_subfolders(rootdir='.'):
    subfolders = []
    for rootdir, dirs, files in os.walk(rootdir):
        for subdir in dirs:
            subfolders.append(os.path.join(rootdir, subdir))
    return subfolders


# helper function to retrieve list of packages, i.e. ['tuplex', 'tuplex.repl', ...]
def discover_packages(where='.'):
    # files to copy for install
    files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(where)) for f in fn]
    subfolders = [f.path for f in os.scandir(where) if f.is_dir()]

    subfolders = get_subfolders(where)

    # remove __pycache__ dirs
    subfolders = filter(lambda x: '__pycache__' not in x, subfolders)
    # to extract dirs, check what are dirs and whether there exists some __init__.py in the dir!
    # i.e., only keep folders where there is an __init__.py in it!
    # @TODO: could add some warnings here for developers...
    subfolders = filter(lambda p: os.path.isfile(os.path.join(p, '__init__.py')), subfolders)

    # remove where prefix
    if not where.endswith(os.sep):
        where += os.sep
    packages = map(lambda p: p[len(where):], subfolders)
    packages = map(lambda x: x.replace(os.sep, '.'), packages)
    packages = sorted(packages)
    return list(packages)


def read_readme():
    # read the contents of your README file
    this_directory = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
        return long_description

def reorg_historyserver():
    """
    reorganize historyserver to become part of pip package.
    """
    # get absolute path of this file's location
    import pathlib
    current_path = pathlib.Path(__file__).parent.resolve()
    assert os.path.exists(os.path.join(current_path, 'tuplex', 'historyserver')), 'Could not find historyserver root dir'

    # copy all the files from history server to directory historyserver under tuplex/python
    src_path = os.path.join(current_path, 'tuplex', 'historyserver')
    dst_path = os.path.join(current_path, 'tuplex', 'python', 'tuplex', 'historyserver')
    distutils.dir_util.copy_tree(src_path, dst_path)

    # at-exit, delete
    def remove_history():
        shutil.rmtree(dst_path)
    atexit.register(remove_history)

    return []

# The information here can also be placed in setup.cfg - better separation of
# logic and declaration, and simpler if you include description/version in a file.
setup(name="tuplex",
    python_requires='>=3.7.0',
    version="0.3.1",
    author="Leonhard Spiegelberg",
    author_email="tuplex@cs.brown.edu",
    description="Tuplex is a novel big data analytics framework incorporating a Python UDF compiler based on LLVM "
                "together with a query compiler featuring whole-stage code generation and optimization.",
    long_description=read_readme(),
    long_description_content_type='text/markdown',
    packages=reorg_historyserver() + discover_packages(where="tuplex/python"),
    package_dir={"": "tuplex/python"},
    package_data={
      # include libs in libexec
    'tuplex.libexec' : ['*.so', '*.dylib'],
        'tuplex.historyserver': ['thserver/templates/*.html', 'thserver/static/css/*.css', 'thserver/static/css/styles/*.css',
                                 'thserver/static/img/*.*', 'thserver/static/js/*.js', 'thserver/static/js/modules/*.js',
                                 'thserver/static/js/styles/*.css']
    },
    ext_modules=[CMakeExtension("tuplex.libexec.tuplex", "tuplex"), CMakeExtension("tuplex.libexec.tuplex_runtime", "tuplex")],
    cmdclass={"build_ext": CMakeBuild},
    # deactivate for now, first fix python sources to work properly!
    zip_safe=False,
    install_requires=install_dependencies,
    # metadata for upload to PyPI
    url="https://tuplex.cs.brown.edu",
    license="Apache 2.0",
    keywords="ETL BigData Python LLVM UDF Data Analytics",
    classifiers=[
        # How mature is this project? Common values are
        #   2 - Pre-Alpha
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 2 - Pre-Alpha',

        # supported environments
        'Operating System :: MacOS',
        'Operating System :: POSIX :: Linux',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    scripts=['tuplex/historyserver/bin/tuplex-webui'],
    project_urls={
        "Bug Tracker": "https://github.com/tuplex",
        "Documentation": "https://tuplex.cs.brown.edu/python-api.html",
        "Source Code": "https://github.com/tuplex",
    }
)
