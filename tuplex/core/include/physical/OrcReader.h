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
#include <orc/TimestampBatch.h>
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
            auto &orcType = reader->getType();

            RowReaderOptions rowReaderOptions;
            ORC_UNIQUE_PTR<RowReader> rowReader = reader->createRowReader(rowReaderOptions);
            ORC_UNIQUE_PTR<ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
            PartitionWriter pw(_task->owner(), _schema, _id, _partitionSize);

            std::vector<tuplex::orc::OrcBatch *> columns;
            if (rowReader->next(*batch)) {
                auto structBatch = static_cast<::orc::StructVectorBatch *>(batch.get());
                auto cols = _schema.getRowType().parameters();
                for (int i = 0; i < cols.size(); ++i) {
                    auto rowType = rowTypeToOrcBatch(cols.at(i), orcType.getSubtype(i), structBatch->fields[i], batch->numElements, cols.at(i).isOptionType());
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
                Serializer serializer(false);
                serializer.setSchema(_schema);
                for (auto col : columns) {
                    col->getField(serializer, r);
                }
                auto len = serializer.length();
                const uint8_t *ptr = new uint8_t[len];
                serializer.serialize((void *) ptr, len);
                pw.writeData(ptr, len);
            }
            _numRowsRead += batch->numElements;
        }

        static tuplex::orc::OrcBatch *rowTypeToOrcBatch(const python::Type& rowType, const ::orc::Type *orcType, ::orc::ColumnVectorBatch *orcBatch, const size_t numRows, bool isOption) {
            using namespace tuplex::orc;
            if (rowType.isOptionType()) {
                return rowTypeToOrcBatch(rowType.elementType(), orcType, orcBatch, numRows, true);
            }

            switch (orcType->getKind()) {
                case ::orc::BOOLEAN: {
                    return new BoolBatch(orcBatch, numRows, isOption);
                }
                case ::orc::BYTE:
                case ::orc::SHORT:
                case ::orc::INT:
                case ::orc::LONG:
                case ::orc::DATE: {
                    return new I64Batch(orcBatch, numRows, isOption);
                }
                case ::orc::FLOAT:
                case ::orc::DOUBLE: {
                    return new F64Batch(orcBatch, numRows, isOption);
                }
                case ::orc::BINARY:
                case ::orc::VARCHAR:
                case ::orc::CHAR:
                case ::orc::STRING: {
                    return new StringBatch(orcBatch, numRows, isOption);
                }
                case ::orc::TIMESTAMP: {
                    return new TimestampBatch(orcBatch, numRows, isOption);
                }
                case ::orc::LIST: {
                    auto list = static_cast<::orc::ListVectorBatch *>(orcBatch);
                    auto child = rowTypeToOrcBatch(rowType.elementType(), orcType->getSubtype(0), list->elements.get(), numRows, isOption);
                    return new ListBatch(orcBatch, child, numRows, isOption);
                }
                case ::orc::MAP: {
                    auto map = static_cast<::orc::MapVectorBatch *>(orcBatch);
                    auto keyType = rowType.keyType();
                    auto key = rowTypeToOrcBatch(keyType, orcType->getSubtype(0), map->keys.get(), numRows, isOption);
                    auto valueType = rowType.valueType();
                    auto value = rowTypeToOrcBatch(valueType, orcType->getSubtype(1), map->elements.get(), numRows, isOption);
                    return new DictBatch(orcBatch, key, value, keyType, valueType, numRows, isOption);
                }
                case ::orc::STRUCT: {
                    auto structType = static_cast<::orc::StructVectorBatch *>(orcBatch);
                    std::vector<OrcBatch *> children;
                    for (int i = 0; i < rowType.parameters().size(); ++i) {
                        children.push_back(rowTypeToOrcBatch(rowType.parameters().at(i), orcType->getSubtype(i), structType->fields[i], numRows, isOption));
                    }
                    return new TupleBatch(orcBatch, children, numRows, isOption);
                }
                default:
                    throw std::runtime_error("Orc row type: " + orcType->toString() + " unable to be converted to Tuplex type");
            }
        }
    };
}

#endif //TUPLEX_ORCREADER_H
