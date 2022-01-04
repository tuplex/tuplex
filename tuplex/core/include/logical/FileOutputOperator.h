//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_OUTPUTFILEOPERATOR_H
#define TUPLEX_OUTPUTFILEOPERATOR_H

#include "../Defs.h"
#include "LogicalOperator.h"
#include "LogicalOperatorType.h"
#include <limits>

namespace tuplex {

    class FileOutputOperator : public LogicalOperator {
    private:
        size_t _splitSize; //! after how many bytes to split files. If 0, leave split decision to Tuplex
        size_t _numParts; //! how many parts to create at most. If 0, unlimited parts allowed
        size_t _limit; //! how many rows to ouput (max)
        URI _uri; //! where to output files
        FileFormat _fmt;
        std::string _name;

        // UDF to compile for parts (optional)
        UDF _outputPathUDF;

        std::unordered_map<std::string, std::string> _options; // output format specific options
    public:
        FileOutputOperator(LogicalOperator* parent,
                const URI& uri,
                const UDF& udf,
                const std::string& name,
                const FileFormat& fmt,
                const std::unordered_map<std::string, std::string>& options,
                size_t numParts=0,
                size_t splitSize=0,
                size_t limit=std::numeric_limits<size_t>::max());

        virtual ~FileOutputOperator() {}

        virtual std::string name() override { return _name; }
        virtual LogicalOperatorType type() const override { return LogicalOperatorType::FILEOUTPUT; }

        virtual bool good() const override { return true; }

        virtual std::vector<Row> getSample(const size_t num=1) const override { return std::vector<Row>(); }
        virtual bool isActionable() override { return true; }

        virtual bool isDataSource() override { return false; }

        virtual Schema getInputSchema() const override { assert(parent()); return parent()->getOutputSchema(); }

        virtual Schema getOutputSchema() const override {
            // depending on format:
            switch (_fmt) {
                case FileFormat::OUTFMT_CSV:
                    // single string (per row)
                    return Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::STRING));
                case FileFormat::OUTFMT_ORC:
                    // No translation performed on rows
                    return getInputSchema();
                default:
                    throw std::runtime_error("file output operator get output schema not supported yet!");
            }
        }

        FileFormat fileFormat()  const { return _fmt; }

        URI uri() const { return _uri; }

        LogicalOperator *clone() override;

        std::vector<std::string> columns() const override {
            // check if parent has columns, if not fail
            auto ds = parent()->getDataSet();
            if(ds)
                return ds->columns();
            else if(parent()) {
                return parent()->columns();
            } else
                return {};
        }

        size_t limit() const { return _limit; }
        size_t splitSize() const { return _splitSize; }
        size_t numParts() const { return _numParts; }
        std::unordered_map<std::string, std::string> options() const { return _options; }

        UDF& udf() { return _outputPathUDF; }
        const UDF& udf() const { return _outputPathUDF; }
    };
}

#endif //TUPLEX_OUTPUTFILEOPERATOR_H