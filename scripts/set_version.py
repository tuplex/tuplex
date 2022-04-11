#!/usr/bin/env python3
import os
import re
import datetime
import argparse
import logging
import sys
import requests
import re
#from distutils.version import LooseVersion

def LooseVersion(v):
    parts = v.split('.')
    return parts


# to create a testpypi version use X.Y.devN
version = '0.3.3rc0'

# https://pypi.org/simple/tuplex/
# or https://test.pypi.org/simple/tuplex/
def get_latest_pypi_version(url='https://pypi.org/simple/tuplex/'):
    r = requests.get(url)

    # parse all strings from page
    links = re.findall(r'href=[\'"]?([^\'" >]+)', r.text)

    links = list(filter(lambda s: 'tuplex' in s, map(lambda s: s[s.find('tuplex'):s.rfind('.whl')], links)))

    # extract version string & sort
    links = {link[len('tuplex-'):link.find('-cp')] for link in links}

    links = sorted(list(links), key=LooseVersion)

    # what's the latest version?
    return links[-1]

if __name__ == '__main__':
    file_handler = logging.FileHandler(filename='version.log')
    stdout_handler = logging.StreamHandler(sys.stdout)
    handlers = [file_handler, stdout_handler]

    logging.basicConfig(level=logging.INFO,
        format='[%(asctime)s] %(levelname)s - %(message)s',
        handlers=handlers)

    parser = argparse.ArgumentParser(description='Zillow cleaning')
    parser.add_argument('--version', type=str, dest='version', default=version,
                        help='version string to use (better modify this file)')
    parser.add_argument('--dev', dest='dev', action="store_true",
                        help="whether to create a latest dev version or not")
    parser.add_argument('-f', '--force', dest='force', action="store_true",
                        help="whether to force version on files")
    args = parser.parse_args()

    if version != args.version:
        logging.info('renaming script uses different version then specified.')
        version = args.version

    major, minor, patch = version.split('.')

    # check if version is not there yet...
    version_pypi = get_latest_pypi_version()
    version_test = get_latest_pypi_version(url='https://test.pypi.org/simple/tuplex/')
    logging.info('latest pypi.org version of tuplex is: {}'.format(version_pypi))
    logging.info('latest test.pypi.org version of tuplex is: {}'.format(version_test))

    if args.dev:
        # get from testpypi (dynamic renaming basically)
        major, minor, patch = version_test.split('.')

        # check if there's dev in latest version, if not create one!
        if 'dev' not in patch:
            patch = 'dev0'
        else:
            # patch should be dev...
            no = int(patch[len('dev'):])
            patch = 'dev' + str(no + 1)

            # to avoid conflicts use datetime as version!
            patch = 'dev' + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")

        dev_version = '{}.{}.{}'.format(major, minor, patch)
        version = dev_version
        logging.info('creating dev version {}'.format(dev_version))

        # write to file
        with open('dev.version', 'w') as fp:
            fp.write(dev_version)
    else:
        # skip if requested version is on test pypi
        if not args.force and LooseVersion(version) <= LooseVersion(version_pypi):
            logging.error("newer version on pypi.org, abort")
            sys.exit(1)

    # paths etc.
    doc_path = '../doc/source/conf.py'
    version_py_path = '../tuplex/python/tuplex/utils/version.py'
    setup_py_path = '../tuplex/python/setup.py'
    toplevel_setup_py_path = '../setup.py'
    version_hist_path = '../tuplex/historyserver/thserver/version.py'

    # modify files...
    with open(version_py_path, 'w') as fp:
        fp.writelines('# (c) L.Spiegelberg 2017 - {}\n__version__="{}"'.format(datetime.datetime.now().year, version))

    with open(version_hist_path, 'w') as fp:
        fp.writelines('# (c) L.Spiegelberg 2017 - {}\n__version__="{}"'.format(datetime.datetime.now().year, version))

    with open(setup_py_path, 'r') as fp:
        contents = fp.read()
    with open(setup_py_path, 'w') as fp:
        fp.write(re.sub('version\s*=\s*[\'"].*[\'"]', 'version="{}"'.format(version), contents))

    with open(toplevel_setup_py_path, 'r') as fp:
        contents = fp.read()
    with open(toplevel_setup_py_path, 'w') as fp:
        fp.write(re.sub('version\s*=\s*[\'"].*[\'"]', 'version="{}"'.format(version), contents))

    with open(doc_path, 'r') as fp:
        contents = fp.read()

    contents = re.sub('version\s*=\s*[\'"].*[\'"]', 'version="{}.{}"'.format(major, minor), contents)
    contents = re.sub('release\s*=\s*[\'"].*[\'"]', 'release="{}"'.format(version), contents)

    with open(doc_path, 'w') as fp:
        fp.write(contents)
