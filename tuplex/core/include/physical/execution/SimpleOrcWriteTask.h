//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                          //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_SIMPLEORCWRITETASK_H
#define TUPLEX_SIMPLEORCWRITETASK_H

#ifdef BUILD_WITH_ORC

#include <orc/OrcFile.hh>
#include "IExecutorTask.h"
#include <orc/OrcTypes.h>
#include <orc/OrcBatch.h>
#include <orc/I64Batch.h>
#include <orc/F64Batch.h>
#include <orc/BoolBatch.h>
#include <orc/StringBatch.h>
#include <orc/ListBatch.h>
#include <orc/DictBatch.h>
#include <orc/TupleBatch.h>
#include <orc/VirtualOutputStream.h>

#include <codegen/LLVMEnvironment.h>

namespace tuplex {

/*!
 * Simple class to write several Tuplex in-memory blocks to Orc file format.
 */
class SimpleOrcWriteTask : public IExecutorTask {
public:
    SimpleOrcWriteTask() = delete;
    SimpleOrcWriteTask(const URI& uri,
                       const std::vector<Partition *> &partitions,
                       const Schema &schema,
                       const std::string &columns) : _uri(uri), _partitions(partitions.begin(), partitions.end()), _schema(schema), _columns(columnStringToVector(columns)) {}

    void execute() override {
        auto& logger = Logger::instance().defaultLogger();

        Timer timer;

        if (_uri == URI::INVALID) {
            abort("invalid URI to ORC writeToFile Task given");
        }

        if (_partitions.empty()) {
            return;
        }

        using namespace ::orc;
        ORC_UNIQUE_PTR<Type> schema(orc::tuplexRowTypeToOrcType(_schema.getRowType(), _columns));
        if (!schema) {
            abort("error creating Orc schema.");
        }

        orc::VirtualOutputStream outStream(_uri);

        WriterOptions options;
        ORC_UNIQUE_PTR<Writer> writer = createWriter(*schema, &outStream, options);
        if (!writer) {
            abort("error creating Orc writer.");
        }

        auto tree = TupleTree<int>(_schema.getRowType());
        auto flattenedSchema = Schema(_schema.getMemoryLayout(), python::Type::makeTupleType(tree.fieldTypes()));
        auto ds = tuplex::Deserializer(flattenedSchema);

        size_t totalBytes = 0;
        size_t totalRows = 0;
        for (auto p : _partitions) {
            auto numBytes = p->bytesWritten();
            auto numRows = p->getNumRows();

            totalBytes += numBytes;
            totalRows += numRows;

            auto ptr = p->lock();
            auto endptr = ptr + p->capacity();

            ORC_UNIQUE_PTR<ColumnVectorBatch> batch = writer->createRowBatch(numRows);

            std::vector<orc::OrcBatch *> orcColumns;
            std::unordered_map<int, int> orcColumnToRowIndexMap;
            int nextIndex = 0;

            initColumns(_schema.getRowType(), batch.get(), numRows, nextIndex, _schema.getRowType().isOptionType(), orcColumns, orcColumnToRowIndexMap);

            for (uint64_t r = 0; r < numRows; ++r) {
                ds.deserialize(ptr, endptr-ptr);
                for (uint64_t i = 0; i < orcColumns.size(); ++i) {
                    auto rowInd = orcColumnToRowIndexMap[i];
                    orcColumns.at(i)->setData(ds, rowInd, r);
                }
                ptr += ds.rowSize();
            }

            writer->add(*batch);

            for (auto el : orcColumns) {
                delete el;
            }

            p->unlock();
            p->invalidate();
        }

        writer->close();

        std::stringstream ss;
        ss<<"[Task Finished] write to Orc file in "
          <<std::to_string(timer.time())<<"s (";
        ss<<pluralize(totalRows, "row")<<", "<<sizeToMemString(totalBytes)<<")";
        owner()->info(ss.str());
    }

    TaskType type() const override { return TaskType::SIMPLEFILEWRITE; }
    std::vector<Partition*> getOutputPartitions() const override { return std::vector<Partition*>{}; }

private:
    URI _uri;
    std::vector<Partition *> _partitions;
    Schema _schema;
    std::vector<std::string> _columns;

    void abort(const std::string& message) {
        throw std::runtime_error(message);
    }

    void initColumns(const python::Type& rowType, ::orc::ColumnVectorBatch *orcType, const size_t numRows, int &nextIndex, bool isOption, std::vector<orc::OrcBatch *>& orcColumns, std::unordered_map<int, int>& orcColumnToRowIndexMap) {
        using namespace ::orc;
        if (rowType.isTupleType()) {
            auto batch = static_cast<StructVectorBatch *>(orcType);
            batch->numElements = numRows;
            batch->hasNulls = isOption;
            for (int i = 0; i < rowType.parameters().size(); ++i) {
                initColumns(rowType.parameters().at(i), batch->fields[i], numRows,nextIndex, isOption, orcColumns, orcColumnToRowIndexMap);
            }
        } else if (rowType.isOptionType()) {
            initColumns(rowType.elementType(), orcType, numRows,  nextIndex, true, orcColumns, orcColumnToRowIndexMap);
        }  else {
            auto col = rowTypeToOrcBatch(rowType, orcType, numRows, isOption);
            orcColumnToRowIndexMap[orcColumns.size()] = nextIndex;
            nextIndex += 1;
            orcColumns.push_back(col);
        }
    }

    static std::vector<std::string> columnStringToVector(const std::string& columns) {
        std::vector<std::string> result;
        if (!columns.empty()) {
            std::stringstream css(columns);
            while (css.good()) {
                std::string substr;
                getline(css, substr, ',');
                result.push_back(substr);
            }
        }
        return result;
    }

    orc::OrcBatch *rowTypeToOrcBatch(const python::Type& rowType, ::orc::ColumnVectorBatch *orcType, const size_t numRows, bool isOption) {
        if (rowType.isPrimitiveType()) {
            if (rowType == python::Type::I64) {
                return new orc::I64Batch(orcType, numRows, isOption);
            } else if (rowType == python::Type::F64) {
                return new orc::F64Batch(orcType, numRows, isOption);
            } else if (rowType == python::Type::STRING) {
                return new orc::StringBatch(orcType, numRows, isOption);
            } else if (rowType == python::Type::BOOLEAN) {
                return new orc::BoolBatch(orcType, numRows, isOption);
            } else {
                throw std::runtime_error("could not convert row type to orc batch.");
            }
        } else if (rowType.isListType()) {
            auto list = static_cast<::orc::ListVectorBatch *>(orcType);
            auto child = rowTypeToOrcBatch(rowType.elementType(), list->elements.get(), numRows, isOption);
            return new orc::ListBatch(orcType, child, numRows, isOption);
        } else if (rowType.isDictionaryType()) {
            auto map = static_cast<::orc::MapVectorBatch *>(orcType);
            auto keyType = rowType.keyType();
            auto key = rowTypeToOrcBatch(rowType.keyType(), map->keys.get(), numRows, isOption);
            auto valueType = rowType.valueType();
            auto value = rowTypeToOrcBatch(rowType.valueType(), map->elements.get(), numRows, isOption);
            return new orc::DictBatch(orcType, key, value, keyType, valueType, numRows, isOption);
        } else if (rowType.isTupleType()) {
            auto structType = static_cast<::orc::StructVectorBatch *>(orcType);
            std::vector<orc::OrcBatch *> children;
            for (int i = 0; i < rowType.parameters().size(); ++i) {
                children.push_back(rowTypeToOrcBatch(rowType.parameters().at(i), structType->fields[i], numRows, isOption));
            }
            return new orc::TupleBatch(orcType, children, numRows, isOption);
        } else {
            throw std::runtime_error("could not convert row type to orc batch.");
        }
    }

    void releaseAllLocks() override {
        for(auto p : _partitions)
            p->unlock();
    }
};

}

#endif

#endif //TUPLEX_SIMPLEORCWRITETASK_H