//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ORCREADER_H
#define TUPLEX_ORCREADER_H

#include <orc/OrcFile.hh>
#include <PartitionWriter.h>
#include <orc/OrcBatch.h>
#include <orc/I64Batch.h>
#include <orc/F64Batch.h>
#include <orc/ListBatch.h>
#include <orc/DictBatch.h>
#include <orc/StringBatch.h>
#include <orc/BoolBatch.h>
#include <orc/TupleBatch.h>
#include <orc/VirtualInputStream.h>

namespace tuplex {
    class OrcReader : public FileInputReader {
    public:
        OrcReader() = delete;
        OrcReader(IExecutorTask *task,
                  codegen::read_block_f functor,
                  uint64_t id,
                  size_t partitionSize,
                  Schema schema) : _task(task), _functor(functor), _id(id), _partitionSize(partitionSize), _schema(schema), _numRowsRead(0) {};
        size_t inputRowCount() const override { return _numRowsRead; }
        virtual ~OrcReader() {}

        /*!
         * read the contents of an Orc file into Tuplex memory
         * @param inputFilePath
         */
        void read(const URI& inputFilePath) override {
            using namespace ::orc;
            auto inStream = std::make_unique<orc::VirtualInputStream>(inputFilePath);
            ReaderOptions options;
            ORC_UNIQUE_PTR<Reader> reader = createReader(std::move(inStream), options);

            RowReaderOptions rowReaderOptions;
            ORC_UNIQUE_PTR<RowReader> rowReader = reader->createRowReader(rowReaderOptions);
            ORC_UNIQUE_PTR<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
            PartitionWriter pw(_task->owner(), _schema, _id, _partitionSize);

            std::vector<tuplex::orc::OrcBatch *> columns;
            if (rowReader->next(*batch)) {
                auto structBatch = static_cast<::orc::StructVectorBatch *>(batch.get());
                auto cols = _schema.getRowType().parameters();
                for (int i = 0; i < cols.size(); ++i) {
                    auto rowType = rowTypeToOrcBatch(cols.at(i), structBatch->fields[i], batch->numElements, cols.at(i).isOptionType());
                    columns.push_back(rowType);
                }

                writeBatchToPartition(pw, batch.get(), columns);
            }

            while (rowReader->next(*batch)) {
                auto structBatch = static_cast<::orc::StructVectorBatch *>(batch.get());
                for (int i = 0; i < columns.size(); ++i) {
                    columns.at(i)->setBatch(structBatch->fields[i]);
                }
                writeBatchToPartition(pw, batch.get(), columns);
            }

            int64_t numNormalRows = 0;
            int64_t numBadRows = 0;

            try {
                for (auto partition : pw.getOutputPartitions()) {
                    int64_t size = partition->size();
                    const uint8_t *ptr = partition->lockRaw();
                    _functor(_task, ptr, size, &numNormalRows, &numBadRows, false);
                    partition->unlock();
                    partition->invalidate();
                }
            } catch (std::exception& e) {
                for (auto el : columns) {
                    delete el;
                }
                throw e;
            }

            for (auto el : columns) {
                delete el;
            }
            batch.reset();
            rowReader.reset();
            reader.reset();
            inStream.reset();
        }

    private:
        IExecutorTask *_task;
        codegen::read_block_f _functor;
        uint64_t _id;
        size_t _partitionSize;
        Schema _schema;

        size_t _numRowsRead;

        void writeBatchToPartition(PartitionWriter &pw, ::orc::ColumnVectorBatch *batch, std::vector<tuplex::orc::OrcBatch *> &columns) {
            for (uint64_t r = 0; r < batch->numElements; ++r) {
                std::vector<Field> fields;
                for (auto col : columns) {
                    fields.push_back(col->getField(r));
                }
                pw.writeRow(Row::from_vector(fields));
            }
            _numRowsRead += batch->numElements;
        }

        static tuplex::orc::OrcBatch *rowTypeToOrcBatch(const python::Type& rowType, ::orc::ColumnVectorBatch *orcType, const size_t numRows, bool isOption) {
            using namespace tuplex::orc;
            if (rowType.isOptionType()) {
                return rowTypeToOrcBatch(rowType.elementType(), orcType, numRows, true);
            } else if (rowType.isPrimitiveType()) {
                if (rowType == python::Type::I64) {
                    return new I64Batch(orcType, numRows, isOption);
                } else if (rowType == python::Type::F64) {
                    return new F64Batch(orcType, numRows, isOption);
                } else if (rowType == python::Type::STRING) {
                    return new StringBatch(orcType, numRows, isOption);
                } else if (rowType == python::Type::BOOLEAN) {
                    return new BoolBatch(orcType, numRows, isOption);
                } else {
                    throw std::runtime_error("python row type is unsupported");
                }
            } else if (rowType.isListType()) {
                auto list = static_cast<::orc::ListVectorBatch *>(orcType);
                auto child = rowTypeToOrcBatch(rowType.elementType(), list->elements.get(), numRows, isOption);
                return new ListBatch(orcType, child, numRows, isOption);
            } else if (rowType.isDictionaryType()) {
                auto map = static_cast<::orc::MapVectorBatch *>(orcType);
                auto keyType = rowType.keyType();
                auto key = rowTypeToOrcBatch(keyType, map->keys.get(), numRows, isOption);
                auto valueType = rowType.valueType();
                auto value = rowTypeToOrcBatch(valueType, map->elements.get(), numRows, isOption);
                return new DictBatch(orcType, key, value, keyType, valueType, numRows, isOption);
            } else if (rowType.isTupleType()) {
                auto structType = static_cast<::orc::StructVectorBatch *>(orcType);
                std::vector<OrcBatch *> children;
                for (int i = 0; i < rowType.parameters().size(); ++i) {
                    children.push_back(rowTypeToOrcBatch(rowType.parameters().at(i), structType->fields[i], numRows, isOption));
                }
                return new TupleBatch(orcType, children, numRows, isOption);
            } else {
                throw std::runtime_error("python row type is unsupported");
            }
        }
    };
}

#endif //TUPLEX_ORCREADER_H
