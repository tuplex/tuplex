import os
import re
import datetime

version = '0.3.0'
major, minor, patch = version.split('.')

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
