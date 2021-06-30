Getting involved
================
Tuplex is an ideal project if you'd like to learn more about data analytics, compilers and how they work, and enjoy writing C/C++ and Python code.

If you're still reading and this sounds like a project you'd love to get involved in, please email us directly (`Leonhard <mailto:lspiegel@cs.brown.edu>`_ or `Malte <mailto:malte@cs.brown.edu>`_) and fill out the following `form <https://forms.gle/rbbZtrpBaojSwNk5A>`_ so we can get you access to the source code and computing resources.

Starter projects
----------------

Before attempting any project on your own, please familiarize yourself with the core ideas of the Tuplex project (e.g., by reading `the preprint <http://cs.brown.edu/~lspiegel/files/Tuplex_Preprint2020.pdf>`_) and install Tuplex on your machine (`build instructions <gettingstarted.html#installation>`_). Then, try out one of the examples in the ``examples/`` folder.
Tuplex only works on Mac OS X or a Linux distribution currently. If your machine runs Windows, you might want to consider using either `Docker <https://www.docker.com/get-started>`_ or a `Virtual Machine <https://www.virtualbox.org/>`_.
When attempting one of these starter projects, use the master to setup your own development branch.

.. code-block:: bash

  git clone https://github.com/LeonhardFS/Tuplex.git
  git checkout -b tplx-<your branchname>


Then, please create a pull request on Github when you're done or need help!

Starter project I: Job Metrics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Tuplex currently prints a good amount of information out while running a job. However, these numbers are not really exposed to the user yet. The only metric that users can retrieve is the exception count:

.. code-block:: python

  import tuplex
  c = Context()
  ds = c.parallelize([1, 2, 0, 4, 5]).map(lambda x: 2 / x)
  res = ds.collect()

  print('Exceptions in map operator: {}'.format(ds.exception_counts))


The purpose of this project is to expose additional metrics. The C++ class where metrics are stored is *JobMetrics* (``JobMetrics.h``). Extend the class to record following times (you may use the *Timer* class in ``Timer.cc``):
 1. logical optimization time (see ``LogicalOptimizer.cc, LogicalOptimize::optimize``);
 2. compilation times via LLVM (see ``TransformStage.cc, TransformStage::compile``);
 3. total compilation time for everything; and
 4. time for using LLVM optimization passes.

Then, expose the numbers via the ``Context`` class in ``context.py``. To do so, create a new class called ``Metrics`` in Python (save to ``metrics.py``). The following should expose the metrics class in Python:

.. code-block:: python

  c.metrics # get a new Metrics object back

To get the numbers from C++ back to Python, create a new class in the Boost Python module folder (``python/`` folder) and expose it. Consult ``PythonBindings.cc/PythonContext.cc`` and ``context.py`` to see how this is done.

- Difficulty level: Medium
- Skills needed: C++, Python
- Framework documentation that might be helpful: Boost Python (https://www.boost.org/doc/libs/1_70_0/libs/python/doc/html/index.html)



Starter project II: More convenient aggregates
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


Tuplex allows for simple aggregations: e.g., in order to count the number of rows you can use the following code snippet:

.. code-block:: python

  import tuplex
  c = Context()
  c.parallelize([1, 2, 3, 4, 5]).aggregate(lambda a, b: a + b, lambda a, x: a + 1, 0).collect()

The way this works is basically as follows. A new aggregate is initialized with a value ``0``. Then, for each row (represented by ``x``) the aggregate a is increased by ``1`` in the ``lambda a, x: a + 1`` function. Two aggregates ``a, b`` are then combined via ``lambda a, b: a + b``.
However, having to always write all this boilerplate code just to count rows is cumbersome. The goal of this project is to allow a more convenient API, such as ``count()``, to have a shorter API that simply returns the count of rows as a single integer.

In ``dataset.py``, implement a new function ``count()`` which returns the count of rows. In other words, the following code snippet should work:

.. code-block:: python

  import tuplex
  c = Context()
  cnt = c.parallelize([1, 2, 3, 4, 5]).count()
  assert cnt == 5, 'wrong count delivered…'

To make sure your code works, add a new file ``python/tests/test_aggregates.py`` and write some tests to check your count function works. Make sure to think of possible edge cases! To copy your test file during the build in Tuplex, edit the ``FILE(COPY …)`` command in ``python/CMakeLists.txt``.

In a second step, it would be also interesting to get the mean and variance via functions ``.mean()``, or ``.var()``. Implement mean/var, but note that these functions are only meaningful for numeric data! Make sure to throw an exception if the user calls ``mean()`` or ``var()`` over non-numeric data. You can use the ``.types`` property of the dataset class in ``dataset.py`` for this. To compute mean/var, use the online version for each formula. Note that an aggregate can be a tuple of any size! You can use that to store multiple variables to compute the mean/variance.

When you have variance implemented, add a function ``std`` by using the fact that ``std = sqrt(var)``.

- Difficulty: Easy
- Skills needed: Python
- Useful documentation: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance (Welford's online algorithm), http://www.grad.hr/nastava/gs/prg/NumericalRecipesinC.pdf Chapter 14.1


Starter project III: Add a new string built-in function
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Tuplex already supports many built-in string functions. However, not all functions are supported. In this project, the goal is to add support for ``str.swapcase()`` (https://docs.python.org/3.7/library/stdtypes.html#str.swapcase). First, read the documentation for ``swapcase``. To add support, first create a C++ test case in ``RuntimeTest.cc``. Then, implement a C function ``strSwapcase`` in ``runtime/src/StringFunctions.cc``. In your C++ test case, test and make sure your ``strSwapcase`` function works. Next, we need to hook up the C function with Tuplex's Python compiler. For this, we need to register the function: go to ``SymbolTableVisitor.cc`` and, in ``SymbolTableBuilder::addBuiltIns()``, add swapcase similar to lower/upper/... via ``table->addBuiltinTypeAttribute``. This requires the correct typing for the function. In this case, it's a function which maps ``str -> str``.
Next, we need to add an LLVM call to the function: Go to ``FunctionRegistry.cc`` and study how ``FunctionRegistry::createLowerCall`` works. Similarly to it, add a new function ``createSwapcaseCall`` and implement it. Once you're done with that, it's time to test that swapcase works: go to ``test/core/StringFunctions.cc`` and add a test case similar to the others in the file to test your swapcase function.

Last, add a python test case: go to ``python/tests/test_strings.py`` and add a new test case for the swapcase function. Congrats, Tuplex now supports swapcase!

- Difficulty: Medium
- Skills required: C++, Python, a bit of LLVM understanding
- Useful documentation: https://llvm.org/docs/tutorial/MyFirstLanguageFrontend/index.html

Starter project IV: Adding support for the ``is`` keyword in Python
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many static analysis tools (e.g., the ones used in Jetbrains' IDEs like PyCharm, CLion) recommend for checks to write ``x is not None`` instead of ``x != None``.
The goal of this project is to support the ``is`` keyword but only allow it to be used with  ``None``, ``True``, ``False``. Else, a warning should be displayed and the user prevented from submitting code containing ``is`` in other places.
As a start, read up on the ``is`` keyword in the Python language specification: https://docs.python.org/3.7/reference/expressions.html#comparisons. For an explanation why the is usage should be restricted,
cf. https://stackoverflow.com/questions/2987958/how-is-the-is-keyword-implemented-in-python. Therefore, Tuplex should allow expressions like ``x is None`` but prevent problematic ones like ``3 * 'a' is 'aaa'``.
A good first test case should be:

.. code-block:: python

    import tuplex
    c = tuplex.Context()
    c.parallelize([1, 2, 3, None, 4]).map(lambda x: x is None).collect()

First, make sure you understand the different stages of compiler (lexing, parsing, code generation) and how the visitor pattern works (https://www.cse.wustl.edu/~cytron/cacweb/Tutorial/Visitor/). Luckily, for ``is`` lexing and parsing is already done via ANTLR4. In ``ASTNodes.h`` you can find a class ``NCompare`` which represents a comparison expression.
``grammar/Python3.g4`` is the ANTLR4 grammar we use to generate a lexer and parser. It's always helpful to take a first look there to see how a rule is implemented. The class ``ASTBuilderVisitor`` converts the parse tree provided by ANTLR4 into an abstract syntax tree (AST). As the ``is`` keyword is part of a compare expression,
in the first step support needs to be added in the ``antlrcpp::Any ASTBuilderVisitor::visitComparison(Python3Parser::ComparisonContext *ctx)`` function to process ``is`` and ``is not``. For this, emit two new ``TokenType`` entries: ``TokenType::IS`` and ``TokenType::ISNOT``. You need to edit ``TokenType.h`` for this and update the ``stringToToken`` conversion function.
As a comparison yields a boolean as type, you don't need to work with the ``TypeAnnotatorVisitor`` class, as this is already handled. However, support for ``is`` should be added to the ``TracingVisitor`` class which performs the tracing of the sample if necessary. I.e., in ``void TraceVisitor::visit(NCompare *node)`` add support for your new ``TokenType::IS, TokenType::ISNOT`` tokens.
Make sure to write a test for this e.g. in ``test/core/PythonTracer.cc``.

Next, after having done the prerequisites actual code generation support needs to be added.

Go to ``BlockGeneratorVisitor.cc`` and edit the

.. code-block:: c++

    llvm::Value *
    BlockGeneratorVisitor::compareInst(llvm::IRBuilder<>& builder, llvm::Value *L, const python::Type &leftType, const TokenType &tt,
                                           llvm::Value *R, const python::Type &rightType)

function to add support for the ``is`` tokens you added. You can use ``error(...)`` to fail on bad comparison expressions involving ``is`` as discussed above.

In a final step, add 1. C++ tests 2. Python tests for the is functionality.
For 1. C++ tests, create a new file under ``test/core`` and make sure to cover several edge cases. You should use the ``TEST_F(...)`` pattern as used in the other tests.
For 2. Python tests, add in ``python/tests`` a new test file ``test_is.py``. Again, write here a test to make sure the C++/Python integration works. You can confer the other tests. Important: You need to edit ``python/CMakeLists.txt`` to copy over the new test file.

- Difficulty: Medium-Hard
- Skills required: C++, Python, LLVM, rough knowledge of how a compiler works

Starter project V: Ctrl-C support for parallelize(...) functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Users sometimes execute code, especially in a jupyter notebook or the Tuplex interactive shell which might take longer than they expected. Thus, they want to stop execution by sending SIGINT (e.g. via Ctrl-C). In Tuplex, currently some functions can be interrupted via SIGINT. However, this does not work for ``tuplex.Context().parallelize(...)`` yet. This project should make ``parallelize(...)`` interruptible.
In ``python/src/PythonContext.cc`` you can find the Boost-Python bindings for ``parallelize``, i.e. the C++ backend interface which is called from ``context.py``. The function ``check_and_forward_signals`` in ``Signal.h`` provides an easy way to detect whether SIGINT was caught.

In this project please add support for allowing SIGINT to be handled within the various ``parallelize...`` functions in ``PythonContext.cc``.

The questions to keep in mind thereby are: What could be a meaningful return value? How can we assure that the state of the CPython interpreter is not being corrupted?
Can you think of a way to test this? (cf. e.g. ``test/core/SignalTest.cc``)
If so, please add a C++ or Python test.

- Difficulty: Medium
- Skills required: C++, Python, CPython interpreter, signals

Starter project VI: Jemalloc instead of libc malloc?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In this project, we want to explore experimentally whether an alternative memory allocator like jemalloc (http://jemalloc.net/) could provide benefits to Tuplex.
For this, in a first step edit the top-level ``CMakeLists.txt`` file to add support for using jemalloc. You may want to read up how other projects usually include jemalloc, e.g. https://github.com/apache/arrow/blob/master/cpp/CMakeLists.txt
and add an option ``USE_JEMALLOC`` to the cmake setup for Tuplex. E.g., to compile with jemalloc support the following command should work

.. code-block:: bash

  cmake -DCMAKE_BUILD_TYPE=Release -DUSE_JEMALLOC=ON .. && make -j$(nproc) && ctest

Then, in a next step compile Tuplex with and without jemalloc. Use the zillow and flights benchmark in ``benchmarks/`` to produce a first result.

Does jemalloc provide a benefit?

Next, perform a follow-up experiment. Use different data input sizes for zillow and flights to see whether the impact of jemalloc is the same for each datasize. Is there any correlation to data size or is the impact (in %) independent of the data size?

After you've verified your experimental setup works locally, please reach out to Leonhard (lspiegel@cs.brown.edu) to get access to a performance benchmarking machine. Then, rerun your experiments on the benchmarking machine.

- Difficulty: Medium
- Skills required: Bash, CMake, python data science libs for analysis
