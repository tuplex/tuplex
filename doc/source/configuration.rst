Configuration
=============

Tuplex provides several mechanism to easily configure the framework. To quickly try out different settings,
you can directly manipulate the ``Context`` object. For an overview over available configuration keys, consult the API of the :func:`Context <tuplex.context.Context.__init__>` object directly.

This can be either done by passing a dictionary

.. code-block:: python

    from tuplex import *
    c = Context(conf={"executorMemory" : "2G"})


or using keyword arguments.

.. code-block:: python

    from tuplex import *
    c = Context(executorMemory="2G")

Furthermore, Tuplex allows to use a yaml file for configuration. If no file is specified, Tuplex looks for a file :file:`conf.yaml` in the working directory.
If found, this file will be attempted to be loaded as configuration file.

.. code-block:: python

    from tuplex import *
    c = Context(conf="/conf/tuplex.yaml")

For any keys the user did not supply values, Tuplex will use its internal defaults.
An example config file is

.. literalinclude:: config.yaml

.. note::
    The same yaml file passed to configure Tuplex can be also used to store application specific configuration details. Tuplex only requires keys to start with ``tuplex`` in the yaml file.
