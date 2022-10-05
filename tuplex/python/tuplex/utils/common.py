#!/usr/bin/env python3
# ----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
# ----------------------------------------------------------------------------------------------------------------------#
import atexit
import sys
import collections

import collections.abc
import pathlib
import signal

import yaml
import sys
from datetime import datetime

import json
import urllib.request
import os
import signal
import atexit
import socket
import shutil
import psutil
import subprocess
import logging
import iso8601
import re
import tempfile
import time
import shlex
import pathlib

try:
    import pwd
except ImportError:
    import getpass

    pwd = None

try:
    from tuplex.utils.version import __version__
except:
    __version__ = 'dev'


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
    mime_type = res.split()[-1].decode()
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


def get_json(url, timeout=10):
    """
    perform a GET request to given URL
    Args:
        url: hostname & port

    Returns:
        python dictionary of decoded json
    """

    req = urllib.request.Request(url, headers={'content-type': 'application/json'})
    response = urllib.request.urlopen(req, timeout=timeout)
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
        shell_name_matching = 'google.colab' in str(get_ipython())
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

        # Python 3.10+ moved MutableMapping to collections.abc.MutableMapping permanently
        if isinstance(val, collections.abc.MutableMapping):
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
            return [{key: beautify_nesting(val)} for key, val in items]
        else:
            return d

    assert isinstance(file_path, str), 'file_path must be instance of str'

    with open(file_path, 'w') as f:
        f.write('# Tuplex configuration file\n')
        f.write('# created {} UTC\n'.format(datetime.utcnow()))

        out = yaml.dump(beautify_nesting(unflatten_dict(conf)))
        # pyyaml prints { } around single item dicts. Remove by hand
        out = out.replace('{', '').replace('}', '')
        f.write(out)


def pythonize_options(options):
    """
    convert string based options into python objects/types
    Args:
        options: flat dict

    Returns:
        dict with python types
    """

    def parse_string(item):
        """
        check what kind of variable string represents and convert accordingly
        Args:
            item: string

        Returns:
            parsed obj in correct type
        """

        # assert flat
        assert not isinstance(item, dict)

        # hack: is that correct?
        if isinstance(item, (list, tuple)):
            return item

        if not isinstance(item, str):
            return item

        # do not use bool(...) to convert!
        if item.lower() == 'true':
            return True
        if item.lower() == 'false':
            return False
        try:
            return int(item)
        except:
            pass
        try:
            return float(item)
        except:
            pass
        return item

    return {k: parse_string(v) for k, v in options.items()}


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
    return {str(key): str(val) for key, val in d.items()}


def registerLoggingCallback(callback):
    """
    register a custom logging callback function with tuplex
    Args:
        callback: callback to register

    Returns:
        None
    """
    from ..libexec.tuplex import registerLoggingCallback as ccRegister

    # create a wrapper to capture exceptions properly and avoid crashing
    def wrapper(level, time_info, logger_name, msg):
        args = (level, time_info, logger_name, msg)

        try:
            callback(*args)
        except Exception as e:
            logging.error("logging callback produced following error: {}".format(e))

    ccRegister(wrapper)


def logging_callback(level, time_info, logger_name, msg):
    """
    this is a callback function which can be used to redirect C++ logging to python logging.
    :param level: logging level as integer, for values cf. PythonCommon.h
    :param time_info: time info as ISO8601 string
    :param logger_name: name of the logger as invoked in C++
    :param msg: message to display
    :return: None
    """

    # convert level to logging levels
    if 0 == level:  # unsupported level in C++
        level = logging.INFO
    if 1 == level:  # trace in C++
        level = logging.DEBUG
    if 2 == level:
        level = logging.DEBUG
    if 3 == level:
        level = logging.INFO
    if 4 == level:
        level = logging.WARNING
    if 5 == level:
        level = logging.ERROR
    if 6 == level:
        level = logging.CRITICAL

    pathname = None
    lineno = None
    ct = iso8601.parse_date(time_info).timestamp()

    # fix pathname/lineno
    if pathname is None:
        pathname = ''
    if lineno is None:
        lineno = 0

    log_record = logging.LogRecord(logger_name, level, pathname, lineno, msg, None, None)
    log_record.created = ct
    log_record.msecs = (ct - int(ct)) * 1000
    log_record.relativeCreated = log_record.created - logging._startTime
    logging.getLogger(logger_name).handle(log_record)


## WebUI helper functions

# shutdown mongod process via KILL
# https://docs.mongodb.com/manual/tutorial/manage-mongodb-processes/


# this is a global var which is a list to hold registered exit handlers
# tuple of (key, func).
__exit_handlers__ = []


# register at exit function to take care of exit handlers
def auto_shutdown_all():
    """
    helper function to automatially shutdown whatever is in the global exit handler array. Resets global variable.
    Returns:
        None
    """
    global __exit_handlers__

    for entry in __exit_handlers__:
        try:
            name, func, args, msg = entry
            logging.debug('Attempting to shutdown {}...'.format(name))
            if msg:
                logging.info(msg)
            func(args)
            logging.info('Shutdown {} successfully'.format(name))
        except Exception as e:
            logging.error('Failed to shutdown {}'.format(name))
    __exit_handlers__ = []


def register_auto_shutdown(name, func, args, msg=None):
    global __exit_handlers__
    __exit_handlers__.append((name, func, args, msg))


atexit.register(auto_shutdown_all)


def is_process_running(name):
    """
    helper function to check if a process is running on the local machine
    Args:
        name: name of the process to search for

    Returns:
        True if a process with name was found
    """
    # Iterate over the all the running process
    for proc in psutil.process_iter():
        try:
            # Check if process name contains the given name string.
            if name.lower() in proc.name().lower():
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


def mongodb_uri(mongodb_url, mongodb_port, db_name='tuplex-history'):
    """
    constructs a fully qualified MongoDB URI
    Args:
        mongodb_url: hostname
        mongodb_port: port
        db_name: database name

    Returns:
        string representing MongoDB URI
    """
    return 'mongodb://{}:{}/{}'.format(mongodb_url, mongodb_port, db_name)


def check_mongodb_connection(mongodb_url, mongodb_port, db_name='tuplex-history', timeout=10.0):
    """
    connects to a MongoDB database instance, raises exception if connection fails
    Args:
        mongodb_url: hostname
        mongodb_port: port
        db_name: database name
        timeout: timeout parameter after which to error

    Returns:
        None, throws exception in case of connection failure
    """
    uri = mongodb_uri(mongodb_url, mongodb_port, db_name)

    # check whether one can connect to MongoDB
    from pymongo import MongoClient
    from pymongo.errors import ServerSelectionTimeoutError

    start_time = time.time()
    connect_successful = False
    logging.debug('Attempting to contact MongoDB under {}'.format(uri))

    connect_try = 1
    while abs(time.time() - start_time) < timeout:
        logging.debug('MongoDB connection try {}...'.format(connect_try))
        try:
            # set client connection to super low timeouts so the wait is not too long.
            client = MongoClient(uri, serverSelectionTimeoutMS=100, connectTimeoutMS=1000)
            info = client.server_info()  # force a call to mongodb, alternative is client.admin.command('ismaster')
            connect_successful = True
        except Exception as e:
            logging.debug('Connection try {} produced {} exception {}'.format(connect_try, type(e), str(e)))

        if connect_successful:
            timeout = 0
            break

        time.sleep(0.05)  # sleep for 50ms
        logging.debug('Contacting MongoDB under {}... -- {:.2f}s of poll time left'.format(uri, timeout - (
                    time.time() - start_time)))
        connect_try += 1

    if connect_successful is False:
        raise Exception('Could not connect to MongoDB, check network connection. (ping must be < 100ms)')

    logging.debug('Connection test to MongoDB succeeded')


def shutdown_process_via_kill(pid):
    """
    issues a KILL signals to a process with pid
    Args:
        pid: process id to kill

    Returns:
        None
    """
    logging.debug('Shutting down process PID={}'.format(pid))
    os.kill(pid, signal.SIGKILL)


def find_or_start_mongodb(mongodb_url, mongodb_port, mongodb_datapath, mongodb_logpath, db_name='tuplex-history'):
    """
    attempts to connect to a MongoDB database. If no running local MongoDB is found, will auto-start a mongodb database. R
    aises exception when fails.
    Args:
        mongodb_url: hostname
        mongodb_port: port
        mongodb_datapath: for local auto-start path where to store data
        mongodb_logpath: for local auto-start path where to store the MongoDB log
        db_name: database name

    Returns:
        None, raises exceptions on failure
    """

    # is it localhost?
    if 'localhost' in mongodb_url:
        logging.debug('Using local MongoDB instance')

        # first check whether mongod is on path
        if not cmd_exists('mongod'):
            raise Exception('MongoDB (mongod) not found on PATH. In order to use Tuplex\'s WebUI, you need MongoDB'
                            ' installed or point the framework to a running MongoDB instance')

        # is mongod running on local machine?
        if is_process_running('mongod'):
            logging.debug('Found locally running MongoDB daemon process')

            # process is running, try to connect
            check_mongodb_connection(mongodb_url, mongodb_port, db_name)
        else:
            # startup process and add to list of processes. Check for any errors!

            # important: data directory needs to exist first!
            os.makedirs(mongodb_datapath, exist_ok=True)
            os.makedirs(pathlib.Path(mongodb_logpath).parent, exist_ok=True)

            # startup via mongod --fork --logpath /var/log/mongodb/mongod.log --port 1234 --dbpath <path>
            try:
                cmd = ['mongod', '--fork', '--logpath', str(mongodb_logpath), '--port', str(mongodb_port), '--dbpath',
                       str(mongodb_datapath)]

                logging.debug('starting MongoDB daemon process via {}'.format(' '.join(cmd)))
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                short_timeout = 2.5
                max_mongodb_timeout = 10  # maximum timeout is 10s
                try:
                    # set a timeout of 2 seconds to keep everything interactive
                    p_stdout, p_stderr = process.communicate(timeout=short_timeout)
                except subprocess.TimeoutExpired:
                    # try now with more time (up to max)
                    logging.info(
                        "Could not start MongoDB daemon process in {}s, trying with timeout={}s".format(short_timeout,
                                                                                                        max_mongodb_timeout))
                    p_stdout, p_stderr = process.communicate(timeout=max_mongodb_timeout)

                # decode
                p_stdout = p_stdout.decode()
                p_stderr = p_stderr.decode()

                if len(p_stderr.strip()) > 0:
                    raise Exception('mongod produced following errors: {}'.format(p_stderr))

                # find mongod pid
                m = re.search(r'forked process: (\d+)', p_stdout)
                assert m is not None, 'Could not find Child process ID when starting MongoDB'
                mongo_pid = int(m[1])
                logging.debug('MongoDB Daemon PID={}'.format(mongo_pid))

                # add a new shutdown func for mongod
                register_auto_shutdown('mongod', shutdown_process_via_kill, mongo_pid)

            except Exception as e:
                logging.error('Failed to start MongoDB daemon. Details: {}'.format(str(e)))

                # print out first 10 and last 10 lines of mongodb log if exists
                n_to_print = 15
                mongodb_logpath = str(mongodb_logpath)
                if os.path.isfile(mongodb_logpath):
                    with open(mongodb_logpath, 'r') as fp_mongo:
                        lines = list(map(lambda line: line.strip(), fp_mongo.readlines()))
                        shortened_log = ''
                        if len(lines) > 2 * n_to_print:
                            shortened_log = '\n'.join(lines[:n_to_print]) + '...\n' + '\n'.join(lines[-n_to_print:])
                        else:
                            shortened_log = '\n'.join(lines)
                        logging.error('MongoDB daemon log:\n{}'.format(shortened_log))
                else:
                    logging.error('Could not find MongoDB log under {}. Permission error?'.format(mongodb_logpath))

                raise e
            logging.debug("Attempting to connect to freshly started MongoDB daemon...")
            check_mongodb_connection(mongodb_url, mongodb_port, db_name)
    else:
        # remote MongoDB
        logging.debug('Connecting to remote MongoDB instance')
        check_mongodb_connection(mongodb_url, mongodb_port, db_name)


def log_gunicorn_errors(logpath):
    """
    uses logging module to print out gunicorn errors if something went wrong
    Args:
        logpath: where gunicorn log is stored

    Returns:
        None
    """

    # parse log, check whether there's any line where [ERROR] is contined
    with open(logpath, 'r') as fp:
        lines = fp.readlines()
        indices = map(lambda t: t[1], filter(lambda t: '[ERROR]' in t[0], zip(lines, range(len(lines)))))
        if indices:
            first_idx = min(indices)
            logging.error('Gunicorn error log:\n {}'.format(''.join(lines[first_idx:])))


def find_or_start_webui(mongo_uri, hostname, port, web_logfile):
    """
    tries to connect to Tuplex WebUI. If local uri is specified, autostarts WebUI.
    Args:
        mongo_uri: MongoDB database uri on which WebUI should be running
        hostname: hostname of WebUI
        port: port of WebUI
        web_logfile: for local auto-start path where to store the WebUI log

    Returns:
        None, raises exceptions on failure
    """
    version_endpoint = '/api/version'  # use this to connect and trigger WebUI connection

    if not hostname.startswith('http://') and not hostname.startswith('https://'):
        hostname = 'http://' + str(hostname)

    base_uri = '{}:{}'.format(hostname, port)

    version_info = None
    try:
        version_info = get_json(base_uri + version_endpoint)
    except Exception as err:
        logging.debug("Couldn't connect to {}, starting WebUI...".format(base_uri + version_endpoint))

    if version_info is not None:
        # check version compatibility
        return version_info
    else:
        # start WebUI up!
        if not cmd_exists('gunicorn'):
            raise Exception(
                'Tuplex uses per default gunicorn with eventlet to run the WebUI. Please install via `pip3 install "gunicorn[eventlet]"` or add to PATH')

        # command for this is:
        # env MONGO_URI=$MONGO_URI gunicorn --daemon --worker-class eventlet --log-file $GUNICORN_LOGFILE -b $HOST:$PORT thserver:app

        # directory needs to be the one where the history server is located in!
        # ==> from structure of file we can infer that
        dir_path = os.path.dirname(os.path.realpath(__file__))
        assert dir_path.endswith(os.path.join('tuplex', 'utils')), 'folder structure changed. Need to fix.'
        # get tuplex base dir
        tuplex_basedir = pathlib.Path(dir_path).parent

        # two options: Could be dev install or site-packages install, therefore check two folders
        if not os.path.isdir(os.path.join(tuplex_basedir, 'historyserver', 'thserver')):
            # dev install or somehow different folder structure?

            # --> try to find root tuplex folder containing historyserver folder!
            path = pathlib.Path(tuplex_basedir)
            while path.parent != path:
                # check in path
                if 'tuplex' in os.listdir(path) and 'historyserver' in os.listdir(os.path.join(path, 'tuplex')):
                    tuplex_basedir = os.path.join(str(path), 'tuplex')
                    logging.debug('Detected Tuplex rootfolder (dev) to be {}'.format(tuplex_basedir))
                    break
                path = path.parent

        # check dir historyserver/thserver exists!
        assert os.path.isdir(os.path.join(tuplex_basedir, 'historyserver',
                                          'thserver')), 'could not find Tuplex WebUI WebApp in {}'.format(
            tuplex_basedir)
        assert os.path.isfile(os.path.join(tuplex_basedir, 'historyserver', 'thserver',
                                           '__init__.py')), 'could not find Tuplex WebUI __init__.py file in thserver folder'

        # history server dir to use to start gunicorn
        ui_basedir = os.path.join(tuplex_basedir, 'historyserver')
        logging.debug('Launching gunicorn from {}'.format(ui_basedir))

        # create temp PID file to get process ID to shutdown auto-started WebUI
        PID_FILE = tempfile.NamedTemporaryFile(delete=False).name

        ui_env = os.environ
        ui_env['MONGO_URI'] = mongo_uri
        gunicorn_host = '{}:{}'.format(hostname.replace('http://', '').replace('https://', ''), port)

        # need to convert everything to absolute paths (b.c. gunicorn fails else)
        web_logfile = os.path.abspath(web_logfile)
        ui_basedir = os.path.abspath(ui_basedir)
        # also make sure parent dir of web_logfile exists
        try:
            wl_path = pathlib.Path(web_logfile).parent
            os.makedirs(str(wl_path), exist_ok=True)
        except Exception as e:
            logging.error("ensuring parent dir of {} exists, failed with {}".format(web_logfile, e))

        cmd = ['gunicorn', '--daemon', '--worker-class', 'eventlet', '--chdir', ui_basedir, '--pid', PID_FILE,
               '--log-file', web_logfile, '-b', gunicorn_host, 'thserver:app']

        logging.debug('Starting gunicorn with command: {}'.format(' '.join(cmd)))

        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ui_env)
        # set a timeout of 2 seconds to keep everything interactive
        p_stdout, p_stderr = process.communicate(timeout=2)

        # decode
        p_stdout = p_stdout.decode()
        p_stderr = p_stderr.decode()

        if len(p_stderr.strip()) > 0:
            raise Exception('gunicorn produced following errors: {}'.format(p_stderr))

        logging.info('Gunicorn locally started...')

        # find out process id of gunicorn
        ui_pid = None

        # Writing the PID might require some time for gunicorn, therefore poll the temp file for up to 2s
        TIME_LIMIT = 3
        start_time = time.time()
        while time.time() - start_time < TIME_LIMIT:
            if not os.path.isfile(PID_FILE) or os.stat(PID_FILE).st_size == 0:
                time.sleep(0.05)  # sleep for 50ms
            else:
                break
            logging.debug('Polling for Gunicorn PID... -- {:.2f}s of poll time left'.format(
                TIME_LIMIT - (time.time() - start_time)))

        ui_pid = None
        try:
            # Read PID file
            with open(PID_FILE, 'r') as fp:
                ui_pid = int(fp.read())
        except Exception as e:
            logging.debug("failed to retrieve PID for WebUI, details: {}".format(e))

            non_daemon_log = 'timeout - no log'
            # something went wrong with starting gunicorn. Try to capture some meaningful output and abort
            try:
                cmd = ['gunicorn', '--worker-class', 'eventlet', '--chdir', ui_basedir, '--pid', PID_FILE,
                       '--log-file', '-', '-b', gunicorn_host, 'thserver:app']
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ui_env)
                # set a timeout of 5 seconds to keep everything interactive
                p_stdout, p_stderr = process.communicate(timeout=5)

                # decode
                p_stdout = p_stdout.decode()
                p_stderr = p_stderr.decode()

                non_daemon_log = p_stdout + '\n' + p_stderr
            except subprocess.TimeoutExpired:
                pass
            logging.error('Gunicorn process log:\n' + non_daemon_log)
            raise Exception("Failed to start gunicorn daemon, non-daemon run yielded:\n{}".format(non_daemon_log))

        assert ui_pid is not None, 'Invalid PID for WebUI'
        logging.info('Gunicorn PID={}'.format(ui_pid))

        # register daemon shutdown
        logging.debug('Adding auto-shutdown of process with PID={} (WebUI)'.format(ui_pid))

        def shutdown_gunicorn(pid):

            pids_to_kill = []

            # iterate over all gunicorn processes and kill them all
            for proc in psutil.process_iter():
                try:
                    # Get process name & pid from process object.
                    process_name = proc.name()
                    process_id = proc.pid

                    sep_line = '|'.join(proc.cmdline()).lower()
                    if 'gunicorn' in sep_line:

                        # check whether that gunicorn instance matches what has been started
                        if 'thserver:app' in proc.cmdline() and gunicorn_host in proc.cmdline() and PID_FILE in proc.cmdline():
                            pids_to_kill.append(proc.pid)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass

            # kill all gunicorn processes
            for pid in pids_to_kill:
                os.kill(pid, signal.SIGQUIT)
                os.kill(pid, signal.SIGKILL)
                os.kill(pid, signal.SIGTERM)
                logging.debug('Shutdown gunicorn worker with PID={}'.format(pid))
            logging.debug('Shutdown gunicorn with PID={}'.format(pid))

        register_auto_shutdown('gunicorn', shutdown_gunicorn, ui_pid)

        version_info = get_json(base_uri + version_endpoint)
        if version_info is None:
            raise Exception('Could not retrieve version info from WebUI')

        # perform checks (same MongoDB URI? Same Version?)
        return version_info


def ensure_webui(options):
    """
    Helper function to ensure WebUI/MongoDB is auto-started when webui is specified
    Args:
        options: Context options object used to connect to WebUI/MongoDB

    Returns:
        None
    """

    # Relevant options are:
    #    {"tuplex.webui.enable", "true"},
    #    {"tuplex.webui.port", "5000"},
    #    {"tuplex.webui.url", "localhost"},
    #    {"tuplex.webui.mongodb.url", "localhost"},
    #    {"tuplex.webui.mongodb.port", "27017"},
    #    {"tuplex.webui.mongodb.path", temp_mongodb_path}

    assert options['tuplex.webui.enable'] is True, 'only call ensure webui when webui option is true'

    mongodb_url = options['tuplex.webui.mongodb.url']
    mongodb_port = options['tuplex.webui.mongodb.port']
    mongodb_datapath = os.path.join(options['tuplex.scratchDir'], 'webui', 'data')
    mongodb_logpath = os.path.join(options['tuplex.scratchDir'], 'webui', 'logs', 'mongod.log')
    gunicorn_logpath = os.path.join(options['tuplex.scratchDir'], 'webui', 'logs', 'gunicorn.log')
    webui_url = options['tuplex.webui.url']
    webui_port = options['tuplex.webui.port']

    try:
        logging.debug('finding MongoDB...')
        find_or_start_mongodb(mongodb_url, mongodb_port, mongodb_datapath, mongodb_logpath)

        mongo_uri = mongodb_uri(mongodb_url, mongodb_port)

        logging.debug('finding WebUI..')
        # now it's time to do the same thing for the WebUI (and also check it's version v.s. the current one!)
        version_info = find_or_start_webui(mongo_uri, webui_url, webui_port, gunicorn_logpath)

        logging.debug('WebUI services found or started!')

        # check that version of WebUI and Tuplex version match
        # exclude dev versions, i.e. silence warning there.
        if 'dev' not in __version__ and version_info['version'] != __version__:
            logging.warning('Version of Tuplex WebUI ({}) and Tuplex ({}) do not match.'.format(version_info['version'],
                                                                                                __version__))

        # all good, print out link so user can access WebUI easily
        webui_uri = webui_url + ':' + str(webui_port)
        if not webui_uri.startswith('http'):
            webui_uri = 'http://' + webui_uri
        print('Tuplex WebUI can be accessed under {}'.format(webui_uri))
    except Exception as e:
        logging.error('Failed to start or connect to Tuplex WebUI. Details: {}'.format(e))

        # log gunicorn errors for local startup
        if os.path.isfile(gunicorn_logpath) and 'localhost' == webui_url:
            log_gunicorn_errors(gunicorn_logpath)
