#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

import collections
import yaml
import sys
from datetime import datetime

import json
import urllib.request
import os
import socket
import shutil
import subprocess

try:
  import pwd
except ImportError:
  import getpass
  pwd = None


def cmd_exists(cmd):
    """
    checks whether command `cmd` exists or not
    Args:
        cmd: executable or script to check for existence

    Returns: True if it exists else False

    """
    return shutil.which(cmd) is not None

def is_shared_lib(path):
    """
    Args:
        path: str path to a file
    detects whether given path is a shared object or not
    Returns: true if shared object, false else
    """

    # use file command
    assert cmd_exists('file')

    res = subprocess.check_output(['file', '--mime-type', path])
    mime_type = res.split()[-1]
    return mime_type == 'application/x-sharedlib' or mime_type == 'application/x-application'

def current_timestamp():
    """
    get current time as isoformatted string
    Returns: isoformatted current time (utc)

    """
    return str(datetime.now().isoformat())

def current_user():
    """
    retrieve current user name
    Returns: username as string

    """
    if pwd:
        return pwd.getpwuid(os.geteuid()).pw_name
    else:
        return getpass.getuser()

def host_name():
    """
    retrieve host name to identify machine
    Returns: some hostname as string

    """
    if socket.gethostname().find('.') >= 0:
        return socket.gethostname()
    else:
        return socket.gethostbyaddr(socket.gethostname())[0]

def post_json(url, data):
    """
    perform a post request to a REST endpoint with JSON
    Args:
        url: where to perform the post request
        data: Dictionary or other data. will be encoded as json

    Returns: Dictionary of the response of the REST API

    """

    params = json.dumps(data).encode('utf8')
    req = urllib.request.Request(url, data=params,
                                 headers={'content-type': 'application/json'})
    response = urllib.request.urlopen(req)
    return json.loads(response.read())

def in_jupyter_notebook():
    """check whether frameworks runs in jupyter notebook.

    Returns: ``True`` if the module is running in IPython kernel,
    ``False`` if in IPython shell or other Python shell.

    """
    # simple method, fails though because helper functions to extract jupyter code use this
    # return 'IPython' in sys.modules
    try:
        # get_ipython won't be defined in standard python interpreter
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True  # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter

def in_google_colab():
    """
        check whether framework runs in Google Colab environment
    Returns:
        True if Tuplex is running in Google Colab
    """
    found_colab_package = False
    try:
        import google.colab
        found_colab_package = True
    except:
        pass

    shell_name_matching = False
    try:
        shell_name_matching =  'google.colab' in str(get_ipython())
    except:
        pass

    if found_colab_package or shell_name_matching:
        return True
    else:
        return False

def is_in_interactive_mode():
    """checks whether the module is loaded in an interactive shell session or not

    Returns: True when in interactive mode. Note that Jupyter notebook also returns True here.

    """

    return bool(getattr(sys, 'ps1', sys.flags.interactive))

def flatten_dict(d, sep='.', parent_key=''):
    """ flattens a nested dictionary into a flat dictionary by concatenating keys with the separator.
    Args:
         d (dict): The dictionary to flatten
         sep (str): string to use to flat keys together. E.g. ``{'a' : {'b' : 10}}`` would be flattened to \
                    ``{'a.b' : 10}`` if ``sep='.'``
    Returns:
        dictionary with string keys.
    """
    items = []
    for key, val in d.items():
        new_key = parent_key + sep + key if parent_key else key
        if isinstance(val, collections.MutableMapping):
            items.extend(flatten_dict(val, sep, new_key).items())
        else:
            items.append((new_key, val))
    return dict(items)

def unflatten_dict(dictionary, sep='.'):
    """
    unflattens a dictionary into a nested dictionary according to sep
    Args:
        dictionary: flattened dictionary, i.e. there are not dictionaries as elements
        sep: separator to use when nesting. I.e. on what the keys are splot

    Returns: nested dictionary

    """
    resultDict = dict()

    # sorting after longest key prevents issues when nesting is screwed up
    # i.e. when there are key key=False, key.another=False
    keyvals = sorted(list(dictionary.items()), key=lambda t: t[0])[::-1]
    for key, value in keyvals:
        parts = key.split(sep)
        d = resultDict
        for part in parts[:-1]:
            if part not in d:
                d[part] = dict()
            d = d[part]
        d[parts[-1]] = value
    return resultDict

def save_conf_yaml(conf, file_path):
    """saves a dictionary holding the configuration options to Tuplex Yaml format. \
    Dict can be either flattened or not.

    Args:
        conf: a dictionary holding the configuration.
        file_path:
    """
    def beautify_nesting(d):
        # i.e. make lists out of dicts
        if isinstance(d, dict):
            items = d.items()
            return [{key : beautify_nesting(val)} for key, val in items]
        else:
            return d
    assert isinstance(file_path, str), 'file_path must be instance of str'

    with open(file_path, 'w') as f:
        f.write('# Tuplex configuration file\n')
        f.write('# created {} UTC\n'.format(datetime.utcnow()))

        out = yaml.dump(beautify_nesting(unflatten_dict(conf)))
        #pyyaml prints { } around single item dicts. Remove by hand
        out = out.replace('{', '').replace('}', '')
        f.write(out)


def load_conf_yaml(file_path):
    """loads yaml file and converts contents to nested dictionary

    Args:
        file_path: where to save the file

    """
    # helper function to get correct nesting from yaml file!
    def to_nested_dict(obj):
        resultDict = dict()
        if isinstance(obj, list):
            for item in obj:
                # check type:
                if isinstance(item, dict):
                    resultDict.update(to_nested_dict(item))
                else:
                    return obj
        elif isinstance(obj, dict):
            for key, val in obj.items():
                # type of val?
                if isinstance(val, list):
                    # flatten out:
                    val = to_nested_dict(val)
                resultDict[key] = val
        return resultDict

    assert isinstance(file_path, str), 'file_path must be instance of str'
    d = dict()
    with open(file_path, 'r') as f:
        confs = list(yaml.safe_load_all(f))
        for conf in confs:
            d.update(to_nested_dict(conf))
    return to_nested_dict(d)


def stringify_dict(d):
    """convert keys and vals into strings
    Args:
        d (dict): dictionary

    Returns:
        dictionary with keys and vals as strs
    """
    assert isinstance(d, dict), 'd must be a dictionary'
    return {str(key) : str(val) for key, val in d.items()}