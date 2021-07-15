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

import cloudpickle
import sys
from enum import Enum

from .libexec.tuplex import _Context, _DataSet
from tuplex.utils.reflection import get_source as get_udf_source
from tuplex.utils.reflection import get_globals
from tuplex.utils.source_vault import SourceVault
from .exceptions import classToExceptionCode

# signed 64bit limit
max_rows = 9223372036854775807

class SortBy(Enum):
    ASCENDING = 1
    DESCENDING = 2
    ASCENDING_LENGTH = 3
    DESCENDING_LENGTH = 4
    ASCENDING_LEXICOGRAPHICALLY = 5
    DESCENDING_LEXICOGRAPHICALLY = 6

    # makes a dictionary out of ordered column names
    # where key (column name) -> value (column index)
def makeDictColumns(columns):
    d = {}
    for i, c in enumerate(columns):
        d[c] = i
    return d

class DataSet:

    def __init__(self):
        self._dataSet = None

    def unique(self):
        """ removes duplicates from Dataset (out-of-order). Equivalent to a DISTINCT clause in a SQL-statement.
        Returns:
            tuplex.dataset.Dataset: A Tuplex Dataset object that allows further ETL operations.
        """
        assert self._dataSet is not None, 'internal API error, datasets must be created via context object'

        ds = DataSet()
        ds._dataSet = self._dataSet.unique()
        return ds

def sort(self, by):
    """

    Args:
        by (dict): key (column name or index) -> value (SortBy Enum). Columns
        that are not specified in keys will be ignored.
    Returns:
        tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations
    """
    assert self._dataSet is not None, 'internal API error, datasets must be created via context objects'
    if by is None or len(by) == 0: return self._dataSet
    assert isinstance(by, dict), 'by must be a dict'
    order = list()
    enumInOrder = list()
    columns = []
    sortbytypes = [SortBy.ASCENDING, SortBy.DESCENDING, SortBy.ASCENDING_LENGTH, SortBy.DESCENDING_LENGTH,
                   SortBy.ASCENDING_LEXICOGRAPHICALLY, SortBy.DESCENDING_LEXICOGRAPHICALLY]
    if len(self._dataSet.columns()) > 0:
        columns = makeDictColumns(self._dataSet.columns())
    else:
        columns = makeDictColumns(self._dataSet.types())
    for key in by:
        if isinstance(key, str) and key in columns and (isinstance(by[key], int) or isinstance(by[key], SortBy)) and \
                1 <= by[key] <= 6:
            order.append(columns[key])
            enumInOrder.append((by[key]))
        # key = column index. column index must be a valid index for a column of the dataset.
        # value corresponding key must be an integer 1-4 since there are four enums
        # the value could be
        elif isinstance(key, int) and 0 <= key < len(columns) and ((isinstance(by[key], int) and 1 <= by[key] <= 6) or (isinstance(by[key], SortBy) and by[key] in sortbytypes)):
            order.append(key)
            if isinstance(by[key], int):
                enumInOrder.append((by[key]))
            else:
                enumInOrder.append((by[key].value))
        else:
            raise Exception("Error: Unsupported key type")
        assert(len(order) == len(enumInOrder), "Internal error when constructing arguments to be passed"
                                               "into backend from the by parameter")
    return self._dataSet.sort(order, enumInOrder)

    def map(self, ftor):
        """ performs a map operation using the provided udf function over the dataset and
        returns a dataset for further processing.

        Args:
            ftor (lambda) or (function): a lambda function, e.g. ``lambda x: x`` or an identifier to a function. \
            Currently there are two supported syntactical options for functions. A function may either take a \
            single parameter which is then interpreted as tuple of the underlying data or a list of parameters, \
            e.g. ``lambda a, b: a + b`` would sum the two columns. If there is not match, whenever an action is \
            called Tuplex will point out the mismatch.

        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations

        """
        assert self._dataSet is not None, 'internal API error, datasets must be created via context object'
        assert ftor is not None, 'need to provide valid functor'

        code = ''
        # try to get code from vault (only lambdas supported yet!)
        try:
            # convert code object to str representation
            code = get_udf_source(ftor)
        except Exception as e:
            raise Exception('Could not extract code for {}. Details:\n{}'.format(ftor, e)) from None

        g = get_globals(ftor)

        ds = DataSet()
        ds._dataSet = self._dataSet.map(code, cloudpickle.dumps(ftor), g)
        return ds

    def filter(self, ftor):
        """ performs a map operation using the provided udf function over the dataset and
        returns a dataset for further processing.

        Args:
            ftor (lambda) or (function): a lambda function, e.g. ``lambda x: x`` or an identifier to a function. \
            that returns a boolean. Tuples for which the functor returns ``True`` will be kept, the others discarded.

        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations

        """
        assert self._dataSet is not None, 'internal API error, datasets must be created via context object'
        assert ftor is not None, 'need to provide valid functor'

        code = ''
        # try to get code from vault (only lambdas supported yet!)
        try:
            # convert code object to str representation
            code = get_udf_source(ftor)
        except Exception as e:
            raise Exception('Could not extract code for {}.Details:\n{}'.format(ftor, e))

        g = get_globals(ftor)
        ds = DataSet()
        ds._dataSet = self._dataSet.filter(code, cloudpickle.dumps(ftor), g)
        return ds

    def collect(self):
        """ action that generates a physical plan, processes data and collects result then as list of tuples.

        Returns:
            (list): A list of tuples, or values if the dataset has only one column.

        """
        assert self._dataSet is not None, 'internal API error, datasets must be created via context objects'
        return self._dataSet.collect()

    def take(self, nrows=5):
        """ action that generates a physical plan, processes data and collects the top results then as list of tuples.

        Args:
            nrows (int): number of rows to collect. Per default ``5``.
        Returns:
            (list): A list of tuples

        """

        assert isinstance(nrows, int), 'num rows must be an integer'
        assert nrows > 0, 'please specify a number greater than zero'

        assert self._dataSet is not None, 'internal API error, datasets must be created via context objects'

        return self._dataSet.take(nrows)

    def show(self, nrows=None):
        """ action that generates a physical plan, processes data and prints results as nicely formatted
        ASCII table to stdout.

        Args:
            nrows (int): number of rows to collect. If ``None`` all rows will be collected

        """
        assert self._dataSet is not None, 'internal API error, datasets must be created via context objects'

        # if optional value is None or below zero, simply return all rows. Else only up to nrows!
        if nrows is None or nrows < 0:
            nrows = -1

        self._dataSet.show(nrows)

    def resolve(self, eclass, ftor):
        """ Adds a resolver operator to the pipeline. The signature of ftor needs to be identical to the one of the preceding operator.

        Args:
            eclass: Which exception to apply resolution for, e.g. ZeroDivisionError
            ftor: A function used to resolve this exception. May also produce exceptions.

        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations

        """

        # check that predicate is a class for an exception class
        assert issubclass(eclass, Exception), 'predicate must be a subclass of Exception'

        # translate to C++ exception code enum
        ec = classToExceptionCode(eclass)

        assert self._dataSet is not None, 'internal API error, datasets must be created via context objects'

        assert ftor is not None, 'need to provide valid functor'

        code = ''
        # try to get code from vault (only lambdas supported yet!)
        try:
            # convert code object to str representation
            code = get_udf_source(ftor)
        except Exception as e:
            raise Exception('Could not extract code for {}.Details:\n{}'.format(ftor, e))

        g = get_globals(ftor)
        ds = DataSet()
        ds._dataSet = self._dataSet.resolve(ec, code, cloudpickle.dumps(ftor), g)
        return ds

    def withColumn(self, column, ftor):
        """ appends a new column to the dataset by calling ftor over existing tuples

        Args:
            column: name for the new column/variable. If column exists, its values will be replaced
            ftor: function to call

        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations

        """

        assert self._dataSet is not None, 'internal API error, datasets must be created via context object'
        assert ftor is not None, 'need to provide valid functor'
        assert isinstance(column, str), 'column needs to be a string'

        code = ''
        # try to get code from vault (only lambdas supported yet!)
        try:
            # convert code object to str representation
            code = get_udf_source(ftor)
        except Exception as e:
            raise Exception('Could not extract code for {}.Details:\n{}'.format(ftor, e))
        g = get_globals(ftor)
        ds = DataSet()
        ds._dataSet = self._dataSet.withColumn(column, code, cloudpickle.dumps(ftor), g)
        return ds

    def mapColumn(self, column, ftor):
        """ maps directly one column. UDF takes as argument directly the value of the specified column and will overwrite
        that column with the result. If you need access to multiple columns, use withColumn instead.
        If the column name already exists, it will be overwritten.

        Args:
            column (str): name for the column to map
            ftor: function to call

        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations
        """

        assert self._dataSet is not None, 'internal API error, datasets must be created via context object'
        assert ftor is not None, 'need to provide valid functor'
        assert isinstance(column, str), 'column needs to be a string'

        code = ''
        # try to get code from vault (only lambdas supported yet!)
        try:
            # convert code object to str representation
            code = get_udf_source(ftor)
        except Exception as e:
            raise Exception('Could not extract code for {}.Details:\n{}'.format(ftor, e)) from None
        g = get_globals(ftor)
        ds = DataSet()
        ds._dataSet = self._dataSet.mapColumn(column, code, cloudpickle.dumps(ftor), g)
        return ds

    def selectColumns(self, columns):
        """ selects a subset of columns as defined through columns which is a list or a single column

        Args:
            columns: list of strings or integers. A string should reference a column name, whereas as an integer refers to an index. Indices may be negative according to python rules. Order in list determines output order

        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations

        """

        assert self._dataSet is not None, 'internal API error, datasets must be created via context object'

        # syntatic sugar, allow single column, list, tuple, ...
        if isinstance(columns, (str, int)):
            columns = [columns]
        if isinstance(columns, tuple):
            columns = list(columns)
        assert(isinstance(columns, list))

        for el in columns:
            assert isinstance(el, (str, int)), 'element {} must be a string or int'.format(el)

        ds = DataSet()
        ds._dataSet = self._dataSet.selectColumns(columns)
        return ds

    def renameColumn(self, oldColumnName, newColumnName):
        """ rename a column in dataset
        Args:
            oldColumnName: str, old column name. Must exist.
            newColumnName: str, new column name

        Returns:
            Dataset
        """

        assert self._dataSet is not None, 'internal API error, datasets must be created via context object'

        assert isinstance(oldColumnName, str), 'oldColumnName must be a string'
        assert isinstance(newColumnName, str), 'newColumnName must be a string'

        ds = DataSet()
        ds._dataSet = self._dataSet.renameColumn(oldColumnName, newColumnName)
        return ds

    def ignore(self, eclass):
        """ ignores exceptions of type eclass caused by previous operator

        Args:
            eclass: exception type/class to ignore

        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations

        """

        # check that predicate is a class for an exception class
        assert issubclass(eclass, Exception), 'predicate must be a subclass of Exception'

        # translate to C++ exception code enum
        ec = classToExceptionCode(eclass)

        assert self._dataSet is not None, 'internal API error, datasets must be created via context objects'

        ds = DataSet()
        ds._dataSet = self._dataSet.ignore(ec)
        return ds

    def cache(self, store_specialized=True):
        """ materializes rows in main-memory for reuse with several pipelines. Can be also used to benchmark certain pipeline costs

        Args:
            store_specialized: bool whether to store normal case and general case separated or merge everything into one normal case. This affects optimizations for operators called on a cached dataset.

        Returns:
            tuplex.dataset.DataSet: A Tuplex Dataset object that allows further ETL operations

        """
        assert self._dataSet is not None, 'internal API error, datasets must be created via context object'

        ds = DataSet()
        ds._dataSet = self._dataSet.cache(store_specialized)
        return ds

    @property
    def columns(self):
        """ retrieve names of columns if assigned

        Returns:
            None or List[str]: Returns None if columns haven't been named yet or a list of strings representing the column names.
        """
        cols = self._dataSet.columns()
        return cols if len(cols) > 0 else None

    @property
    def types(self):
        """ output schema as list of type objects of the dataset. If the dataset has an error, None is returned.

        Returns:
            detected types (general case) of dataset. Typed according to typing module.
        """
        types = self._dataSet.types()
        return types

    def join(self, dsRight, leftKeyColumn, rightKeyColumn, prefixes=None, suffixes=None):
        """
        (inner) join with other dataset
        Args:
            dsRight: other dataset
            leftKeyColumn: column name of the column to use as key in the caller
            rightKeyColumn: column name of the column to use as key in the dsRight dataset
            prefixes: tuple or list of 2 strings. One element may be None.
            suffixes tuple or list of 2 strings. One element may be None

        Returns: Dataset

        """

        assert self._dataSet is not None, 'internal API error, datasets must be created via context objects'
        assert dsRight._dataSet is not None, 'internal API error, datasets must be created via context objects'

        # process prefixes/suffixes
        leftPrefix = ''
        leftSuffix = ''
        rightPrefix = ''
        rightSuffix = ''

        if prefixes:
            prefixes = tuple(prefixes)
            assert len(prefixes) == 2, 'prefixes must be a sequence of 2 elements!'
            leftPrefix = prefixes[0] if prefixes[0] else ''
            rightPrefix = prefixes[1] if prefixes[1] else ''

        if suffixes:
            suffixes = tuple(suffixes)
            assert len(suffixes) == 2, 'prefixes must be a sequence of 2 elements!'
            leftSuffix = suffixes[0] if suffixes[0] else ''
            rightSuffix = suffixes[1] if suffixes[1] else ''

        ds = DataSet()
        ds._dataSet = self._dataSet.join(dsRight._dataSet, leftKeyColumn, rightKeyColumn,
                                         leftPrefix, leftSuffix, rightPrefix, rightSuffix)
        return ds

    def leftJoin(self, dsRight, leftKeyColumn, rightKeyColumn, prefixes=None, suffixes=None):
        """
        left (outer) join with other dataset
        Args:
            dsRight: other dataset
            leftKeyColumn: column name of the column to use as key in the caller
            rightKeyColumn: column name of the column to use as key in the dsRight dataset
            prefixes: tuple or list of 2 strings. One element may be None.
            suffixes tuple or list of 2 strings. One element may be None

        Returns: Dataset

        """

        assert self._dataSet is not None, 'internal API error, datasets must be created via context objects'
        assert dsRight._dataSet is not None, 'internal API error, datasets must be created via context objects'

        # process prefixes/suffixes
        leftPrefix = ''
        leftSuffix = ''
        rightPrefix = ''
        rightSuffix = ''

        if prefixes:
            prefixes = tuple(prefixes)
            assert len(prefixes) == 2, 'prefixes must be a sequence of 2 elements!'
            leftPrefix = prefixes[0] if prefixes[0] else ''
            rightPrefix = prefixes[1] if prefixes[1] else ''

        if suffixes:
            suffixes = tuple(suffixes)
            assert len(suffixes) == 2, 'prefixes must be a sequence of 2 elements!'
            leftSuffix = suffixes[0] if suffixes[0] else ''
            rightSuffix = suffixes[1] if suffixes[1] else ''

        ds = DataSet()
        ds._dataSet = self._dataSet.leftJoin(dsRight._dataSet, leftKeyColumn, rightKeyColumn,
                                         leftPrefix, leftSuffix, rightPrefix, rightSuffix)
        return ds


    def tocsv(self, path, part_size=0, num_rows=max_rows, num_parts=0, part_name_generator=None, null_value=None, header=True):
        """ save dataset to one or more csv files. Triggers execution of pipeline.
        Args:
            path: path where to save files to
            split_size: optional size in bytes for each part to not exceed.
            num_rows: limit number of output rows
            num_parts: number of parts to split output into. The last part will be the smallest
            part_name_generator: optional name generator function to the output parts, receives an integer \
                                 as parameter for the output number. This is intended as a convenience helper function. \
                                 Should not raise any exceptions.
            null_value: string to represent null values. None equals empty string. Must provide explicit quoting for this argument.
            header: bool to indicate whether to write a header or not or a list of strings to specify explicitly a header to write. number of names provided must match the column count.
        """
        assert self._dataSet is not None, 'internal API error, datasets must be created via context objects'
        assert isinstance(header, list) or isinstance(header, bool), 'header must be a list of strings, or a boolean'

        code, code_pickled = '', ''
        if part_name_generator is not None:
            code_pickled = cloudpickle.dumps(part_name_generator)
            # try to get code from vault (only lambdas supported yet!)
            try:
                # convert code object to str representation
                code = get_udf_source(part_name_generator)
            except Exception as e:
                raise Exception('Could not extract code for {}.Details:\n{}'.format(part_name_generator, e))

        # clamp max rows
        if num_rows > max_rows:
            raise Exception('Tuplex supports at most {} rows'.format(max_rows))

        if null_value is None:
            null_value = ''

        self._dataSet.tocsv(path, code, code_pickled, num_parts, part_size, num_rows, null_value, header)

    def aggregate(self, combine, aggregate, initial_value):
        """
        Args:
            combine: a UDF to combine two aggregates (results of the aggregate function or the initial_value)
            aggregate: a UDF which produces a result
            initial_value: a neutral initial value.
        Returns:
            Dataset
        """

        comb_code, agg_code = '', ''

        comb_code_pickled = cloudpickle.dumps(combine)
        agg_code_pickled = cloudpickle.dumps(aggregate)
        try:
            # convert code object to str representation
            comb_code = get_udf_source(combine)
        except Exception as e:
            raise Exception('Could not extract code for {}.Details:\n{}'.format(combine, e))

        try:
            # convert code object to str representation
            agg_code = get_udf_source(aggregate)
        except Exception as e:
            raise Exception('Could not extract code for {}.Details:\n{}'.format(aggregate, e))

        g_comb = get_globals(combine)
        g_agg = get_globals(aggregate)
        ds = DataSet()
        ds._dataSet = self._dataSet.aggregate(comb_code, comb_code_pickled,
                                              agg_code, agg_code_pickled,
                                              cloudpickle.dumps(initial_value), g_comb, g_agg)
        return ds

    def aggregateByKey(self, combine, aggregate, initial_value, key_columns):
        """
        Args:
            combine: a UDF to combine two aggregates (results of the aggregate function or the initial_value)
            aggregate: a UDF which produces a result
            initial_value: a neutral initial value.
            key_columns: the columns to group the aggregate by
        Returns:
            Dataset
        """

        comb_code, comb_code_pickled = '', ''
        agg_code, agg_code_pickled = '', ''
        try:
            # convert code object to str representation
            comb_code = get_lambda_source(combine)
            comb_code_pickled = cloudpickle.dumps(combine)
        except:
            print('{} is not a lambda function or its code could not be extracted'.format(combine))

        try:
            # convert code object to str representation
            agg_code = get_lambda_source(aggregate)
            agg_code_pickled = cloudpickle.dumps(aggregate)
        except:
            print('{} is not a lambda function or its code could not be extracted'.format(aggregate))

        print(comb_code)
        print(agg_code)

        ds = DataSet()
        ds._dataSet = self._dataSet.aggregateByKey(comb_code, comb_code_pickled,
                                              agg_code, agg_code_pickled,
                                              cloudpickle.dumps(initial_value), key_columns)
        return ds

    @property
    def exception_counts(self):
        """

        Returns: dictionary of exception class names with integer keys, i.e. the counts. Returns None
        if error occurred in dataset. Note that Python has an exception hierarchy, e.g. an IndexError is a LookupError.
        The counts returned here correspond to whatever type is being raised.

        """
        return self._dataSet.exception_counts()