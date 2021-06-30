.. _examples:

Examples
========

Flexible Syntax
---------------

Tuplex allows a more flexible syntax than Spark. The following statements are all possible in order to access elements within a UDF:

*Tuple Syntax:*

.. code-block:: python

    from tuplex import *
    c = Context()
    # access elements via tuple syntax
    # will print [11, 22, 33]
    c.parallelize([(1, 10), (2, 20), (3, 30)]) \
     .map(lambda x: x[0] + x[1]) \
     .collect()

*Multiparameter Syntax:*

.. code-block:: python

     from tuplex import *
     c = Context()
     # access elements via auto unpacking
     # will print [11, 22, 33]
     c.parallelize([(1, 10), (2, 20), (3, 30)]) \
      .map(lambda x, y: x + y) \
      .collect()

*Dictionary Syntax:*

.. code-block:: python

    from tuplex import *
    c = Context()
    # access elements via dictionary access
    # will print [11, 22, 33]
    c.parallelize([(1, 10), (2, 20), (3, 30)], columns=['colX', 'colY']) \
     .map(lambda row: row['colX'] + row['colY']) \
     .collect()


In order to return multiple columns, simply return a tuple.

.. raw:: html

  <hr>
  

Exception Handling
------------------

One of the key features of the tuplex framework is its ability to efficiently and robustly deal with exceptions.

This is best demonstrated using a simple example. Imagine, you have a simple pipeline like this:

.. code-block:: python

    from tuplex import *
    c = Context()

    c.parallelize([(1, 0), (2, 1), (3, 0), (4, -1)]) \
     .map(lambda x, y: x / y) \
     .collect()

In a framework like Spark, this code would fail because of the two division-by-zero exceptions raised by the first and
third tuple respectively. However, in Tuplex the returned result would be ``[2, -4]`` with a warning that 2 exceptions
occurred while processing the pipeline. The WebUI further provides insights to where and what kind of data caused these
exceptions.

A user may now want to resolve these exceptions by adding resolvers to the pipeline. This is as easy as calling
the ``resolve(...)`` once or multiple times after the operator that caused the exception.

.. code-block:: python

    from tuplex import *
    c = Context()

    c.parallelize([(1, 0), (2, 1), (3, 0), (4, -1)]) \
     .map(lambda x, y: x / y) \
     .resolve(ZeroDivisionError, lambda a, b: 0) \
     .collect()

Thus, the result is here ``[0, 2, 0, -4]``.

When dealing with ``filter`` operators the operation is quite similar. The function passed to the resolver needs to yield a boolean.

.. code-block::python

    from tuplex import *
    c = Context()

    c.parallelize([(1, 0), (2, 1), (3, 0), (4, -1)]) \
     .filter(lambda x, y: x / y > 0) \
     .resolve(ZeroDivisionError, lambda a, b: True) \
     .collect()

This code basically keeps all tuples ``[(1, 0), (2, 1), (3, 0), (4, -1)]``.

.. note::

    All resolvers applied to an operator need to have the same type semantics. E.g. when a map operator
    maps tuples ``T -> S`` then the UDF supplied to the ``resolve(...)`` function also needs to have type
    ``T -> S``
