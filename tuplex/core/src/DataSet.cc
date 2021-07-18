//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <DataSet.h>
#include <logical/MapOperator.h>
#include <logical/FilterOperator.h>
#include <logical/TakeOperator.h>
#include <logical/ResolveOperator.h>
#include <logical/IgnoreOperator.h>
#include <logical/MapColumnOperator.h>
#include <logical/WithColumnOperator.h>
#include <logical/FileOutputOperator.h>
#include <logical/JoinOperator.h>
#include <logical/AggregateOperator.h>
#include <logical/CacheOperator.h>
#include <physical/ResultSet.h>
#include <logical/SortOperator.h>
#include <Utils.h>
#include <ErrorDataSet.h>
#include <Signals.h>

namespace tuplex {
    DataSet::~DataSet() {
        _id = -1;

        // memory is freed by MemoryManager!!!
        _partitions.clear();
        _context = nullptr;
        _operator = nullptr;
    }

    std::shared_ptr<ResultSet> DataSet::collect(std::ostream &os) {
        return take(-1, os);
    }

    std::shared_ptr<ResultSet> DataSet::take(int64_t numElements, std::ostream &os) {
        // error dataset?
        if (isError())
            throw std::runtime_error("is error dataset!");

        // negative numbers mean get all elements!
        if (numElements < 0)
            numElements = std::numeric_limits<int64_t>::max();

        // create a take node
        assert(_context);
        LogicalOperator *op = _context->addOperator(new TakeOperator(this->_operator, numElements));
        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // perform action.
        assert(this->_context);
        auto rs = op->compute(*this->_context);

        return rs;
    }

    // collect functions
    std::vector<Row> DataSet::collectAsVector(std::ostream &os) {
        return takeAsVector(-1, os);
    }

    // -1 means to retrieve all elements
    std::vector<Row> DataSet::takeAsVector(int64_t numElements, std::ostream &os) {
        auto rs = take(numElements, os);
        Timer timer;

#warning "limiting should make this hack irrelevant..."
        if (numElements < 0)
            numElements = std::numeric_limits<int64_t>::max();
        std::vector<Row> v;
        while (rs->hasNextRow() && v.size() < numElements) {
            v.push_back(rs->getNextRow());
        }

        Logger::instance().defaultLogger().debug("Result set converted to " + pluralize(v.size(), "row"));
        Logger::instance().defaultLogger().info(
                "Collecting result of " + pluralize(v.size(), "row") + " took " + std::to_string(timer.time()) +
                " seconds");

        // be sure that result set does not contain more data than desired!

        // TODO: limit pushdown should solve this!
        // assert(v.size() <= numElements);

        if (v.size() > numElements) {
            Logger::instance().defaultLogger().warn("limit pushdown should make this code piece here unnecessary");
            v.resize(numElements);
        }

        return v;
    }

    void DataSet::tofile(tuplex::FileFormat fmt, const tuplex::URI &uri, const tuplex::UDF &udf,
                         size_t fileCount, size_t shardSize,
                         const std::unordered_map<std::string, std::string> &outputOptions, size_t limit,
                         std::ostream &os) {
        if (isError())
            throw std::runtime_error("is error dataset!");

        if (fmt != FileFormat::OUTFMT_CSV)
            throw std::runtime_error("only csv output format yet supported!");

        assert(_context);
        assert(_operator);
        LogicalOperator *op = _context->addOperator(
                new FileOutputOperator(_operator, uri, udf, "csv", FileFormat::OUTFMT_CSV, outputOptions,
                                       fileCount, shardSize, limit));
        ((FileOutputOperator*)op)->udf().getAnnotatedAST().allowNumericTypeUnification(_context->getOptions().AUTO_UPCAST_NUMBERS());

        if (!op->good()) {
            Logger::instance().defaultLogger().error("failed to create file output operator");
        } else {
            // action, perform it!
            DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
            dsptr->_operator = op;
            dsptr->setColumns(_columnNames); // file op doesn't change column names!
            op->setDataSet(dsptr);

            // perform action.
            assert(this->_context);
            auto rs = op->compute(*this->_context);
        }
    }

    DataSet &DataSet::map(const UDF &udf) {
        // if error dataset, return itself
        if (isError())
            return *this;

        assert(_context);
        assert(this->_operator);
        LogicalOperator *op = _context->addOperator(new MapOperator(this->_operator, udf, _columnNames, allowTypeUnification()));

        if (!op->good()) {
            Logger::instance().defaultLogger().error("failed to create map operator");
            return _context->makeError("failed to add map operator to logical plan");
        }

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        auto outputCols = ((MapOperator *) op)->columns();
        if (!outputCols.empty())
            dsptr->setColumns(outputCols);
        op->setDataSet(dsptr);

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    DataSet &DataSet::sort(std::vector<std::size_t> order, std::vector<std::size_t> orderEnums) {
        // if error dataset, return itself
        if (isError())
            return *this;

        assert(_context);
        assert(this->_operator);

        // parameter checking
        assert(order.size() == orderEnums.size());

        // sanity check/print for order and orderEnums
        //        for (int i = 0; i < order.size(); i++) {
        //            printf("o: %d, e: %d\n", order[i], orderEnums[i]);
        //        }

        LogicalOperator *op = _context->addOperator(new SortOperator(this->_operator, order, orderEnums));

        if (!op->good()) {
            Logger::instance().defaultLogger().error("failed to create sort operator");
            return _context->makeError("failed to add sort operator to logical plan");
        }
        // action, perform it!
        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        dsptr->setColumns(_columnNames); // sort doesn't change column names
        op->setDataSet(dsptr);

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }


    DataSet & DataSet::cache(const Schema::MemoryLayout &memoryLayout, bool storeSpecialized) {
        // if error dataset, return itself
        if (isError())
            return *this;

        assert(_context);
        assert(this->_operator);

        LogicalOperator *op = _context->addOperator(new CacheOperator(this->_operator, storeSpecialized, memoryLayout));

        if (!op->good()) {
            Logger::instance().defaultLogger().error("failed to create cache operator");
            return _context->makeError("failed to add ache operator to logical plan");
        }

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // set columns (they do not change)
        dsptr->setColumns(columns());

        // this operator is both action & source, i.e. trigger materialization here!
        // => it will hold then both an array of partitions for the common case
        // and an array with exceptions
        // perform action.
        assert(this->_context);
        auto rs = op->compute(*this->_context); // note: this should also hold the exceptions...

        // result set is computed, now make both partitions&exceptions ephemeral (@TODO: uncache mechanism)
        auto cop = (CacheOperator*)op;
        cop->setResult(rs);

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    DataSet &DataSet::mapColumn(const std::string &columnName, const tuplex::UDF &udf) {

        // if error dataset, return itself
        if (isError())
            return *this;

        assert(_context);
        assert(this->_operator);

        // make sure column name is contained in parents columns
        // there is no check in MapColumnOperator for this!!!
        int idx = indexInVector(columnName, columns());
        if (idx < 0)
            return _context->makeError("there is no column " + columnName + " to map");


        LogicalOperator *op = _context->addOperator(new MapColumnOperator(this->_operator,
                                                                          columnName,
                                                                          columns(),
                                                                          udf,
                                                                          _context->getOptions().AUTO_UPCAST_NUMBERS()));
        if (!op->good()) {
            Logger::instance().defaultLogger().error("failed to create mapColumn operator");
            return _context->makeError("failed to add mapColumn operator to logical plan");
        }

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // set column names (they didn't change)
        dsptr->setColumns(_columnNames);

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }


    DataSet &DataSet::withColumn(const std::string &columnName, const tuplex::UDF &udf) {

        // if error dataset, return itself
        if (isError())
            return *this;

        assert(_context);
        assert(this->_operator);

        LogicalOperator *op = _context->addOperator(
                new WithColumnOperator(this->_operator,
                                       _columnNames,
                                       columnName,
                                       udf,
                                       _context->getOptions().AUTO_UPCAST_NUMBERS()));

        if (!op->good()) {
            Logger::instance().defaultLogger().error("failed to create withColumn operator");
            return _context->makeError("failed to add withColumn operator to logical plan");
        }

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // set column names
        auto wop = dynamic_cast<WithColumnOperator *>(op);
        dsptr->setColumns(wop->columns());

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    DataSet &DataSet::selectColumns(const std::vector<size_t> &columnIndices) {
        if (isError())
            return *this;

        assert(_context);
        assert(this->_operator);

        if (columnIndices.empty())
            return _context->makeError("select columns must contain at least one index");

        // check that all indices are valid
        auto num_cols = _operator->getOutputSchema().getRowType().parameters().size();
        for (auto idx : columnIndices)
            if (idx >= num_cols)
                return _context->makeError(
                        "index in selectColumns can be at most " + std::to_string(num_cols) + ", is " +
                        std::to_string(idx));

        // no missing cols, hence one can do selection.
        // for this, create a simple UDF
        std::string code;
        if (columnIndices.size() == 1) {
            code = "lambda t: t[" + std::to_string(columnIndices.front()) + "]";
        } else {
            code = "lambda t: (";
            for (auto idx : columnIndices) {
                code += "t[" + std::to_string(idx) + "], ";
            }
            code += ")";
        }

        // now it is a simple map operator
        DataSet &ds = map(UDF(code));

        // check if cols exist & update them
        auto columns = _operator->columns();
        if (!columns.empty()) {
            assert(columns.size() == num_cols);
            std::vector<std::string> sel_columns;
            for (auto idx : columnIndices) {
                assert(0 <= idx && idx < num_cols);
                sel_columns.push_back(columns[idx]);
            }

            ds.setColumns(sel_columns);
            ((MapOperator*)ds._operator)->setOutputColumns(sel_columns);
        }

        // rename operator
        ((MapOperator*)ds._operator)->setName("select");

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        return ds;
    }

    DataSet & DataSet::renameColumn(const std::string &oldColumnName, const std::string &newColumnName) {
        using namespace std;

        if(isError())
            return *this;

        assert(_context);
        assert(_operator);

        // find old column in current columns
        auto it = std::find(_columnNames.begin(), _columnNames.end(), oldColumnName);
        if(it == _columnNames.end())
            return _context->makeError("renameColumn: could not find column '" + oldColumnName + "' in dataset's columns");

        // position?
        auto idx = it - _columnNames.begin();

        // make copy
        vector<string> columnNames(_columnNames.begin(), _columnNames.end());
        columnNames[idx] = newColumnName;

        // create dummy map operator
        // now it is a simple map operator
        DataSet &ds = map(UDF(""));

        // set columns to restricted cols
        ds.setColumns(columnNames);

        // rename operator
        ((MapOperator*)ds._operator)->setName("rename");
        ((MapOperator*)ds._operator)->setOutputColumns(columnNames);

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        return ds;
    }

    DataSet &DataSet::selectColumns(const std::vector<std::string> &columnNames) {
        // if error dataset, return itself
        if (isError())
            return *this;

        if (columnNames.empty())
            return _context->makeError("select columns must contain at least one column");

        assert(_context);
        assert(this->_operator);

        // check first that each column name is returned, else return error message
        std::vector<std::string> missingColumns;
        for (auto cn : columnNames) {
            if (std::find(_columnNames.begin(), _columnNames.end(), cn) == _columnNames.end()) {
                missingColumns.emplace_back(cn);
            }
        }

        // missing cols?
        if (!missingColumns.empty()) {
            std::stringstream ss;
            if (missingColumns.size() == 1)
                ss << "there is no column " << missingColumns.front() << " to select.";
            else {
                ss << "there are no columns ";
                ss << missingColumns.front();
                for (unsigned i = 1; i < missingColumns.size(); ++i)
                    ss << ", " << missingColumns[i];
                ss << " to select.";
            }

            return _context->makeError(ss.str());
        }

#warning"use here dict syntax to overcome selection problem, i.e. when doing selection pushdown - need to also change code. => that's difficult, hence simply use dict syntax here."
        // no missing cols, hence one can do selection.
        // for this, create a simple UDF
        std::string code;
        if (columnNames.size() == 1) {
            auto idx = indexInVector(columnNames.front(), _columnNames);
            assert(idx >= 0);
            assert(idx < _operator->getOutputSchema().getRowType().parameters().size());
            //code += "t[" + std::to_string(idx) + "]";
            code = "lambda t: t['" + _columnNames[idx] + "']";
        } else {
            code = "lambda t: (";
            for (std::string cn : columnNames) {
                auto idx = indexInVector(cn, _columnNames);
                assert(idx >= 0);
                assert(idx < _operator->getOutputSchema().getRowType().parameters().size());
                // code += "t[" + std::to_string(idx) + "], ";
                code += "t['" + _columnNames[idx] + "'], ";

            }
            code += ")";
        }

        // now it is a simple map operator
        DataSet &ds = map(UDF(code));

        // set columns to restricted cols
        ds.setColumns(columnNames);

        ((MapOperator*)ds._operator)->setOutputColumns(columnNames);
        // rename operator
        ((MapOperator*)ds._operator)->setName("select");

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }
        return ds;
    }

    DataSet &DataSet::filter(const UDF &udf) {

        // if error dataset, return itself
        if (isError())
            return *this;

        assert(_context);
        assert(this->_operator);
        LogicalOperator *op = _context->addOperator(new FilterOperator(this->_operator,
                                                                       udf,
                                                                       _columnNames,
                                                                       _context->getOptions().AUTO_UPCAST_NUMBERS()));

        if (!op->good()) {

            // filter only throws error, if output scheme is wrong:
            std::stringstream err;
            err << "failed to create filter operator, expected return type boolean but got "
                << udf.getOutputSchema().getRowType().desc();
            Logger::instance().defaultLogger().error(err.str());
            return _context->makeError("failed to add filter operator to logical plan");
        }

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // set columns (they do not change)
        dsptr->setColumns(columns());

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    DataSet &DataSet::resolve(const tuplex::ExceptionCode &ec, const tuplex::UDF &udf) {

        // if error dataset, return itself
        if (isError())
            return *this;

        assert(_context);
        assert(this->_operator);
        LogicalOperator *op = _context->addOperator(new ResolveOperator(this->_operator, ec,
                                                                        udf, _columnNames,
                                                                        _context->getOptions().AUTO_UPCAST_NUMBERS()));
        if (!op->good()) {
            Logger::instance().defaultLogger().error("failed to create resolve operator");
            return _context->makeError("failed to add resolve operator to logical plan");
        }

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // set columns (they do not change)
        dsptr->setColumns(columns());

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    DataSet &DataSet::ignore(const tuplex::ExceptionCode &ec) {
        if (isError())
            return *this;

        assert(_context && this->_operator);
        LogicalOperator *op = _context->addOperator(new IgnoreOperator(this->_operator, ec));

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);
        // set columns (they do not change)
        dsptr->setColumns(columns());

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    DataSet &DataSet::unique() {
        if(isError())
            return *this;

        assert(_context && this->_operator);

        LogicalOperator *op = _context->addOperator(new AggregateOperator(this->_operator, AggregateType::AGG_UNIQUE, false));

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);
        // set columns (they do not change)
        dsptr->setColumns(columns());

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    DataSet & DataSet::aggregate(const UDF &aggCombine, const UDF &aggUDF, const Row &aggInitial) {

        if(isError())
            return *this;

        assert(_context && this->_operator);

        LogicalOperator* op = _context->addOperator(new AggregateOperator(this->_operator, AggregateType::AGG_GENERAL,
                                                                          _context->getOptions().AUTO_UPCAST_NUMBERS(),
                                                                          aggCombine, aggUDF, aggInitial));

        ((AggregateOperator*)op)->aggregatorUDF().getAnnotatedAST().allowNumericTypeUnification(_context->getOptions().AUTO_UPCAST_NUMBERS());
        ((AggregateOperator*)op)->combinerUDF().getAnnotatedAST().allowNumericTypeUnification(_context->getOptions().AUTO_UPCAST_NUMBERS());

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // b.c. this is a general aggregate, column names are lost (for hash-aggregate, they can be partially preserved)
        // set columns (they do not change)
        dsptr->setColumns(op->columns());

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    // TODO: this is almost the exact same code as above, maybe refactor?
    DataSet & DataSet::aggregateByKey(const UDF &aggCombine, const UDF &aggUDF, const Row &aggInitial, const std::vector<std::string> &keyColumns) {
        if(isError())
            return *this;

        assert(_context && this->_operator);

        LogicalOperator* op = _context->addOperator(new AggregateOperator(this->_operator, AggregateType::AGG_BYKEY,
                                                                          _context->getOptions().AUTO_UPCAST_NUMBERS(),
                                                                          aggCombine, aggUDF, aggInitial, keyColumns));

        ((AggregateOperator*)op)->aggregatorUDF().getAnnotatedAST().allowNumericTypeUnification(_context->getOptions().AUTO_UPCAST_NUMBERS());
        ((AggregateOperator*)op)->combinerUDF().getAnnotatedAST().allowNumericTypeUnification(_context->getOptions().AUTO_UPCAST_NUMBERS());

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // b.c. this is a hash-aggregate, column names can be partially preserved
        // set columns (they do not change)
        dsptr->setColumns(op->columns());

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }


    void DataSet::show(const int64_t numRows, std::ostream &os) {
        assert(_context);

        // get rows
        auto rows = takeAsVector(numRows, os);
        if (rows.empty()) {
            return;
        }

        int numColumns = rows[0].getNumColumns();

        std::vector<std::string> headers(numColumns);
        // check if column names are available or not
        if (!_columnNames.empty()) {
            assert(numColumns == _columnNames.size());
            headers = _columnNames;
        } else {
            // default to dummy vals
            for (int i = 0; i < numColumns; ++i) {
                headers[i] = "Column_" + std::to_string(i);
            }
        }

        printTable(os, headers, rows);
    }

    Schema DataSet::schema() const {
        if(!_operator)
            return Schema::UNKNOWN;

        assert(_operator);
        return _operator->getOutputSchema();
    }

    DataSet &DataSet::join(const tuplex::DataSet &other, tuplex::option<std::string> leftColumn,
                           tuplex::option<std::string> rightColumn,
                           option<std::string> leftPrefix, option<std::string> leftSuffix,
                           option<std::string> rightPrefix, option<std::string> rightSuffix) {
        // if error dataset, return itself
        if (isError())
            return *this;
        if (other.isError())
            return _context->makeError(dynamic_cast<const ErrorDataSet&>(other).getError());

        // TODO: handle empty dataset properly here!

        assert(_context);
        assert(this->_operator);
        assert(other._operator); // if this fails, probably dataset not declared via auto& ds = ...
        LogicalOperator *op = _context->addOperator(
                new JoinOperator(this->_operator, other._operator, leftColumn, rightColumn, JoinType::INNER,
                                 leftPrefix.value_or(""), leftSuffix.value_or(""), rightPrefix.value_or(""),
                                 rightSuffix.value_or("")));

        if (!op->good()) {

            // filter only throws error, if output scheme is wrong:
            std::stringstream err;
            err << "failed to create join operator.";
            Logger::instance().defaultLogger().error(err.str());
            return _context->makeError("failed to add join operator to logical plan");
        }

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // set columns as defined by the operator
        dsptr->setColumns(op->columns());

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    DataSet &DataSet::leftJoin(const DataSet &other, option<std::string> leftColumn, option<std::string> rightColumn,
                               option<std::string> leftPrefix, option<std::string> leftSuffix,
                               option<std::string> rightPrefix, option<std::string> rightSuffix) {
        // if error dataset, return itself
        if (isError())
            return *this;
        if(other.isError())
            return _context->makeError(dynamic_cast<const ErrorDataSet&>(other).getError());

        assert(_context);
        assert(this->_operator);
        assert(other._operator); // if this fails, probably dataset not declared via auto& ds = ...
        LogicalOperator *op = _context->addOperator(
                new JoinOperator(this->_operator, other._operator, leftColumn, rightColumn, JoinType::LEFT,
                                 leftPrefix.value_or(""), leftSuffix.value_or(""), rightPrefix.value_or(""),
                                 rightSuffix.value_or("")));

        if (!op->good()) {

            // filter only throws error, if output scheme is wrong:
            std::stringstream err;
            err << "failed to create left join operator.";
            Logger::instance().defaultLogger().error(err.str());
            return _context->makeError("failed to add left join operator to logical plan");
        }

        DataSet *dsptr = _context->createDataSet(op->getOutputSchema());
        dsptr->_operator = op;
        op->setDataSet(dsptr);

        // set columns as defined by the operator
        dsptr->setColumns(op->columns());

        // signal check
        if(check_and_forward_signals()) {
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("received signal handler sig, returning error dataset");
#endif
            return _context->makeError("job aborted (signal received)");
        }

        // !!! never return the pointer above
        return *op->getDataSet();
    }

    bool DataSet::isEmpty() const {

        // do we have an operator?
        if(_operator) {
            if(_operator->isDataSource())
                // check if partitions are defined
                return _partitions.empty();
            else return false;
        } else {
            return false;
        }
    }

    bool DataSet::allowTypeUnification() const {
        return _context->getOptions().AUTO_UPCAST_NUMBERS();
    }
}