Tuplex Python API
=================

.. warning::
    The Tuplex project is still under development and the API is thus subject to frequent  change.

Modules
-------

Typical usage of the Python client will involve to import the ``tuplex`` module first, e.g.


.. code:: python

    from tuplex import *


Depending on whether this code is executed from the python interpreter in
interactive mode or by calling a file via the interpreter, an interactive FastETL shell providing a REPL (read-evaluate-print-loop) may be started.

.. raw:: html

  <hr>

Context
-------

.. autoclass:: tuplex.context.Context
    :members:

    .. automethod:: __init__

.. raw:: html

  <hr>

DataSet
-------

.. autoclass:: tuplex.dataset.DataSet
    :members:
