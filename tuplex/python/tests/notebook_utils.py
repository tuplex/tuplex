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

import nbformat
import os
import tempfile
import subprocess
import json

def get_jupyter_version():
    """helper to get version of jupyter as tuple"""
    try:
        proc = os.popen('jupyter notebook --version')
        version = proc.read()
        proc.close()
        return tuple(map(lambda x: int(x), version.split('.')))
    except Exception as e:
        print(e)
        return (0, 0, 0)


def notebook_run(path):
    """Execute a notebook via nbconvert and collect output.
       :returns (parsed nb object, execution errors)
    """

    # check jupyter version
    jv = get_jupyter_version()
    jv_numeric_version = jv[0] * 100 + jv[1] * 10 + jv[2]
    if not jv_numeric_version >= 440:
        print('WARNING: Jupyter notebook >= 4.4.0 is at least required, found {}.{}'.format(jv[0], jv[1]))

    dirname, __ = os.path.split(path)
    if len(dirname) > 0:
        os.chdir(dirname)
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".ipynb") as fout:
        args = ['jupyter', "nbconvert", "--to", "notebook", "--execute",
                "--ExecutePreprocessor.timeout=60",
                "--output", fout.name, path]
        subprocess.check_call(args, stderr=subprocess.DEVNULL)

        fout.seek(0)
        nb = nbformat.read(fout, nbformat.current_nbformat)

    errors = [output for cell in nb.cells if "outputs" in cell
              for output in cell["outputs"] \
              if output.output_type == "error"]

    outputs = [output for cell in nb.cells if "outputs" in cell
               for output in cell["outputs"] \
               if output.output_type == "execute_result"]

    return nb, outputs, errors



def create_function_notebook(func_name, func_code, output_filename="testnb.ipynb"):
    nb = nbformat.v4.new_notebook()

    text = "## Notebook test file for code reflection (auto-created)"
    codeI = "from tuplex.utils.reflection import get_source"
    codeIII = "get_source({})".format(func_name)

    nb['cells'] = [nbformat.v4.new_markdown_cell(text),
                   nbformat.v4.new_code_cell(codeI),
                   nbformat.v4.new_code_cell(func_code),
                   nbformat.v4.new_code_cell(codeIII)]

    nbformat.write(nb, output_filename)

def get_jupyter_function_code(func_name, code):
    """
    execute notebook & get output of last cell for testing. I.e. puts code into one cell,
    then executes get_source({func_name}) and returns the result of this
    Args:
        func_name: function name for which to call get_source
        code: code which to insert into notebook

    Returns:
        result of get_source run in jupyter notebook
    """
    fname = 'testnb.ipynb'

    # create notebook
    if os.path.exists(fname):
        raise Exception('File {} already exists. Aborting testing.'.format(fname))

    try:
        create_function_notebook(func_name, code, fname)

        # run the notebook and assert code to be the same
        nb, out, errs = notebook_run(fname)

        # return last output for processing
        # ipynb is json, so load value
        # old version uses json, new seems to use literal?
        # this should fix it
        try:
            return json.loads(out[-1]['data']['text/plain'])
        except:
            return eval(out[-1]['data']['text/plain'])

    finally:
        if os.path.exists(fname):
            os.remove(fname)
    return ''