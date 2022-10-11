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

import logging

try:
    from .libexec.tuplex import _Context, _DataSet, getDefaultOptionsAsJSON
except ModuleNotFoundError as e:
    logging.error("need to compiled Tuplex first, details: {}".format(e))

from .dataset import DataSet
import os
import glob
import sys
import cloudpickle
from tuplex.utils.common import flatten_dict, load_conf_yaml, stringify_dict, unflatten_dict, save_conf_yaml, in_jupyter_notebook, in_google_colab, is_in_interactive_mode, current_user, is_shared_lib, host_name, ensure_webui, pythonize_options, logging_callback, registerLoggingCallback
import uuid
import json
from .metrics import Metrics

class Context:

    def __init__(self, conf=None, name="", **kwargs):
        r"""creates new Context object, the main entry point for all operations with the Tuplex big data framework

        Args:

            conf (str) or (dict): Can be either the path to a YAML configuration file that is used to configure this \
                                  particular Tuplex context or a dictionary with Tuplex configuration options. \
                                  For keys and their meaning see below the list of Keyword Arguments.
            name (str): An optional name can be given to the context object. WHen given an empty string, \
                        Tuplex will choose a random name.
            **kwargs: Arbitrary keyword arguments, confer Keyword Arguments section for more information.

        Keyword Arguments:

            executorMemory (str) or (int): Specify how much memory each executor should use. If given as int, will be \
                                           interpreted as number of bytes. Else, one can also specify a memory amount \
                                           in string syntax, e.g. '1G' for 1GB of memory.
            executorCount (int): Number of executors (threads) to use. Defaults to ``std::thread::hardware_concurrency()``
            driverMemory (str) or (int): ``executorMemory`` for the driver
            partitionSize (str) or (int): ``executorMemory`` will be divided in blocks of size ``partitionSize``. This also \
                              corresponds more or less 1:1 to the task size and is thus a parameter to tune \
                              parallelism.
            runTimeMemory (str) or (int): Each executor allocates besides the ``executorMemory`` a memory region that is used \
                                          to store temporary objects when processing a single tuple. E.g. for string copy operations \
                                          arrays etc. This key allows to set memory via a memory string or as integer in bytes.
            runTimeMemoryBlockSize (str) or (int): Size of blocks used to allocate ``runTimeMemory``
            useLLVMOptimizer (str) or (bool): Specify whether LLVM Optimizers should be applied to generated LLVM IR or not.
            autoUpcast (str) or (bool): When transferring data to python, e.g. ``[1, 3.0, 4.0]`` the inferred type will be ``float``. \
                                        When this parameter is set to ``True``, ``1`` will be automatically cast to ``float`` and no exception be raised. \
                                        In case of the parameter being ``False``, tuple with data ``1`` will raise a ``ValueError``.
            allowUndefinedBehavior: (str) or (bool): When set to true, certain errors won't be raised, e.g. division by zero will be ignored. This allows for better speed.
            scratchDir (str): Tuplex allows to process larger than memory datasets. If the main memory budget is exceeded, executors will cache files at `scratchDir`.
            logDir (str): Tuplex produces a log file `log.txt` per default. Specify with `logDir` where to store it.
            historyDir (str): Tuplex stores the database and logs within this dir when the webui is enabled.
            normalcaseThreshold (float): used to detect the normal case
            webui (bool): Alias for webui.enable, whether to use the WebUI interface. By default true.
            webui.enable (bool): whether to use the WebUI interface. By default true.
            webui.url (str): URL where to connect to for history server. Default: localhost
            webui.port (str): port to use when connecting to history server. Default: 6543
            webui.mongodb.url (str): URL where to connect to MongoDB storage. If empty string, Tuplex will start and exit a local mongodb instance.
            webui.mongodb.port (int): port for MongoDB instance
            webui.mongodb.path (str): local path where to store files for MongoDB instance to be started.
            webui.exceptionDisplayLimit (int): How many exceptions to display in UI max, must be at least 1.
            csv.maxDetectionRows (int): maximum number of rows to determine types for CSV files.
            csv.maxDetectionMemory (str) or (int): maximum number of bytes to use when performing type detection, separator inference, etc. over CSV files.
            csv.separators (list): list of single character strings that are viable separators when autodetecting. E.g. ``[','. ';', '\t']``.
            csv.quotechar (str): single character denoting the character that is used as quote char according to RFC-4180 standard. E.g.  ``'"'``
            csv.comments (str): list of single character string which indicate start of a comment line, e.g. ``['#', '~']``
            csv.generateParser (str) or (bool): Whether to use C++ parser or a LLVM code generated parser
            csv.selectionPushdown (str) or (bool): When enabled, then the physical planner will generate a parser that \
                                                   only serializes data that is required within the pipeline.

        """
        runtime_path = os.path.join(os.path.dirname(__file__), 'libexec', 'tuplex_runtime')
        paths = glob.glob(runtime_path + '*')

        if len(paths) != 1:
            # filter based on type (runtime must be shared object!)
            paths = list(filter(is_shared_lib, paths))

        if len(paths) != 1:
            if len(paths) == 0:
                logging.error("found no tuplex runtime (tuplex_runtime.so). Faulty installation?")
            else:
                logging.error('found following candidates for tuplex runtime:\n{}, please specify which to use.'.format(paths))
            sys.exit(1)

        # pass configuration options
        # (1) check if conf is a dictionary or a string
        options = dict()

        # put meaningful defaults for special environments...

        # per default disable webui
        options['tuplex.webui.enable'] = False
        if in_google_colab():
            logging.debug('Detected Google Colab environment, adjusting options...')

            # do not use a lot of memory, restrict...
            options['tuplex.driverMemory'] = '64MB'
            options['tuplex.executorMemory'] = '64MB'
            options['tuplex.inputSplitSize'] = '16MB'
            options['tuplex.partitionSize'] = '4MB'
            options['tuplex.runTimeMemory'] = '16MB'
            options['tuplex.webui.enable'] = False

        if conf:
            if isinstance(conf, str):
                # need to load yaml file
                loaded_options = flatten_dict(load_conf_yaml(conf))
                options.update(loaded_options)
            elif isinstance(conf, dict):
                # update dict with conf
                options.update(flatten_dict(conf))

        # (2) update options with kwargs
        options.update(kwargs)
        # (3) stringify to get to backend via boost python
        options = stringify_dict(options)

        user = current_user()
        name = name if len(name) > 0 else 'context' + str(uuid.uuid4())[:8]
        mode = 'file'
        if is_in_interactive_mode():
            mode = 'shell'
        if in_jupyter_notebook():
            mode = 'jupyter'
        if in_google_colab():
            mode = 'colab'
        host = host_name()

        # pass above options as env.user, ...
        # also pass runtime path like that
        options['tuplex.env.user'] = str(user)
        options['tuplex.env.hostname'] = str(host)
        options['tuplex.env.mode'] = str(mode)

        # update runtime path according to user
        if 'tuplex.runTimeLibrary' in options:
            runtime_path = options['tuplex.runTimeLibrary']

        # normalize keys to be of format tuplex.<key>
        supported_keys = json.loads(getDefaultOptionsAsJSON()).keys()
        key_set = set(options.keys())
        for k in key_set:
            if k not in supported_keys and 'tuplex.' + k in supported_keys:
                options['tuplex.' + k] = options[k]

        # check if redirect to python logging module should happen or not
        if 'tuplex.redirectToPythonLogging' in options.keys():
            py_opts = pythonize_options(options)
            if py_opts['tuplex.redirectToPythonLogging']:
                logging.info('Redirecting C++ logging to Python')
                registerLoggingCallback(logging_callback)
        else:
            # check what default options say
            defaults = pythonize_options(json.loads(getDefaultOptionsAsJSON()))
            if defaults['tuplex.redirectToPythonLogging']:
                logging.info('Redirecting C++ logging to Python')
                registerLoggingCallback(logging_callback)

        # autostart mongodb & history server if they are not running yet...
        # deactivate webui for google colab per default
        if 'tuplex.webui.enable' not in options:
            # for google colab env, disable webui per default.
            if in_google_colab():
                options['tuplex.webui.enable'] = False

        # fetch default options for webui ...
        webui_options = {k: v for k, v in json.loads(getDefaultOptionsAsJSON()).items() if 'webui' in k or 'scratch' in k}

        # update only non-existing options!
        for k, v in webui_options.items():
            if k not in options.keys():
                options[k] = v

        # pythonize
        options = pythonize_options(options)

        if options['tuplex.webui.enable']:
            ensure_webui(options)

        # last arg are the options as json string serialized b.c. of boost python problems
        logging.debug('Creating C++ context object')

        # because webui=False/True is convenient, pass it as well to tuplex options
        if 'tuplex.webui' in options.keys():
            options['tuplex.webui.enable'] = options['tuplex.webui']
            del options['tuplex.webui']
        if 'webui' in options.keys():
            options['tuplex.webui.enable'] = options['webui']
            del options['webui']

        self._context = _Context(name, runtime_path, json.dumps(options))
        logging.debug('C++ object created.')
        python_metrics = self._context.getMetrics()
        assert python_metrics, 'internal error: metrics object should be valid'
        self.metrics = Metrics(python_metrics)
        assert self.metrics

    def parallelize(self, value_list, columns=None, schema=None, auto_unpack=True):
        """ passes data to the Tuplex framework. Must be a list of primitive objects (e.g. of type bool, int, float, str) or
        a list of (nested) tuples of these types.

        Args:
            value_list (list): a list of objects to pass to the Tuplex backend.
            columns (list): a list of strings or None to pass to the Tuplex backend in order to name the columns.
                            Allows for dict access in functions then.
            schema: a schema defined as tuple of typing types. If None, then most likely schema will be inferred.
            auto_unpack: whether or not to automatically unpack dictionaries with string keys.
        Returns:
            Tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations
        """

        assert isinstance(value_list, list), "data must be given as a list of objects"
        assert isinstance(auto_unpack, bool), "auto_unpack must be given as a boolean"

        cols = []
        if not columns:
            if len(value_list) > 0:
                num_cols = 1
                if isinstance(value_list[0], (list, tuple)):
                    num_cols = len(value_list[0])
                cols = ['column{}'.format(i) for i in range(num_cols)]
        else:
            cols = columns

        for col in cols:
            assert isinstance(col, str), 'element {} must be a string'.format(col)


        ds = DataSet()
        ds._dataSet = self._context.parallelize(value_list, columns, schema, auto_unpack)
        return ds

    def csv(self, pattern, columns=None, header=None, delimiter=None, quotechar='"', null_values=[''], type_hints={}):
        """ reads csv (comma separated values) files. This function may either be provided with
        parameters that help to determine the delimiter, whether a header present or what kind
        of quote char is used. Overall, CSV parsing is done according to the RFC-4180 standard
        (cf. https://tools.ietf.org/html/rfc4180)

        Args:
            pattern     (str): a file glob pattern, e.g. /data/file.csv or /data/\*.csv or /\*/\*csv

            columns     (list): optional list of columns, will be used as header for the CSV file.
                                If header is True, the first line will be automatically checked against the column names.
                                If header is None, then it will be inferred whether a header is present and a check against
                                the columns performed.

            header      (bool): optional argument, if set to None Tuplex will automatically
                               infer whether a header is present or not.

            delimiter   (str): optional argument, if set Tuplex will use this as delimiter.
                               If set to None, Tuplex will automatically infer the delimiter.

            quotechar   (str): defines quoting according to RFC-4180.

            null_values (list): list of strings to be identified as null value, i.e. they will be parsed as None

            type_hints  (dict): dictionary of hints for column types. Columns can be index either using integers or strings.

        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations
        """

        if not null_values:
            null_values = []

        assert isinstance(pattern, str), 'file pattern must be given as str'
        assert isinstance(columns, list) or columns is None, 'columns must be a list or None'
        assert isinstance(delimiter, str) or delimiter is None, 'delimiter must be given as , or None for auto detection'
        assert isinstance(header, bool) or header is None, 'header must be given as bool or None for auto detection'
        assert isinstance(quotechar, str), 'quote char must be given as str'
        assert isinstance(null_values, list), 'null_values must be a list of strings representing null values'
        assert isinstance(type_hints, dict), 'type_hints must be a dictionary mapping index to type hint' # TODO: update with other options

        if delimiter:
            assert len(delimiter) == 1, 'delimiter can only exist out of a single character'
        assert len(quotechar) == 1, 'quotechar can only be a single character'

        ds = DataSet()
        ds._dataSet = self._context.csv(pattern,
                                        columns,
                                        header is None,
                                        header if header is not None else False,
                                        '' if delimiter is None else delimiter,
                                        quotechar,
                                        null_values,
                                        type_hints)
        return ds

    def text(self, pattern, null_values=None):
        """reads text files.
        Args:
            pattern     (str): a file glob pattern, e.g. /data/file.csv or /data/\*.csv or /\*/\*csv
            null_values (List[str]): a list of string to interpret as None. When empty list or None, empty lines will be the empty string ''
        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations
        """

        # adjust None to be []
        if not null_values:
            null_values = []

        assert isinstance(pattern, str), 'file pattern must be given as str'
        assert isinstance(null_values, list), 'null_values must be a list of strings representing null values'

        ds = DataSet()
        ds._dataSet = self._context.text(pattern, null_values)
        return ds

    def orc(self, pattern, columns=None):
        """ reads orc files.
        Args:
            pattern (str): a file glob pattern, e.g. /data/file.csv or /data/\*.csv or /\*/\*csv
            columns (list): optional list of columns, will be used as header for the CSV file.
        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations
        """

        assert isinstance(pattern, str), 'file pattern must be given as str'
        assert isinstance(columns, list) or columns is None, 'columns must be a list or None'

        ds = DataSet()
        ds._dataSet = self._context.orc(pattern, columns)
        return ds

    def options(self, nested=False):
        """ retrieves all framework parameters as dictionary

        Args:
            nested (bool): When set to true, this will return a nested dictionary.
            May be helpful to provide better overview.
        Returns:
            dictionary with configuration keys and values for this context
        """
        assert self._context
        opt = self._context.options()

        # small hack because boost python has problems with nested dicts
        opt['tuplex.csv.separators'] = eval(opt['tuplex.csv.separators'])
        opt['tuplex.csv.comments'] = eval(opt['tuplex.csv.comments'])

        if nested:
            return unflatten_dict(opt)
        else:
            return opt

    def optionsToYAML(self, file_path='config.yaml'):
        """saves options as yaml file to (local) filepath

        Args:
            file_path (str): local filepath where to store file
        """

        save_conf_yaml(self.options(), file_path)

    def ls(self, pattern):
        """
        return a list of strings of all files found matching the pattern. The same pattern can be supplied to read inputs.
        Args:
            pattern: a UNIX wildcard pattern with a prefix like s3:// or file://. If no prefix is specified,
                     defaults to the local filesystem i.e. file://.

        Returns: list of strings

        """
        assert self._context
        return self._context.ls(pattern)

    def cp(self, pattern, target_uri):
        """
        copies all files matching the pattern to a target uri. If more than one file is found, a folder is created
         containing all the files relative to the longest shared path prefix.
        Args:
            pattern: a UNIX wildcard pattern with a prefix like s3:// or file://. If no prefix is specified,
                     defaults to the local filesystem i.e. file://.
            target_uri: a uri, i.e. path prefixed with s3:// or file://. If no prefix is used, defaults to file://

        Returns: None

        """
        assert self._context
        return self._context.cp(pattern, target_uri)

    def rm(self, pattern):
        """
        removes all files matching the pattern
        Args:
            pattern: a UNIX wildcard pattern with a prefix like s3:// or file://. If no prefix is specified,
                     defaults to the local filesystem i.e. file://.

        Returns: None

        """
        # TODO: change to list of files actually having been removed.
        assert self._context
        return self._context.rm(pattern)

    @property
    def uiWebURL(self):
        """
        retrieve URL of webUI if running
        Returns:
            None if webUI was disabled, else URL as string
        """
        options = self.options()
        if not options['tuplex.webui.enable']:
            return None

        hostname = options['tuplex.webui.url']
        port = options['tuplex.webui.port']
        url = '{}:{}'.format(hostname, port)
        if not url.startswith('http://') or url.startswith('https://'):
            url = 'http://' + url
        return url
