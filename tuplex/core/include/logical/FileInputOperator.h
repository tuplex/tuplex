//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_FILEINPUTOPERATOR_H
#define TUPLEX_FILEINPUTOPERATOR_H

#include "LogicalOperator.h"
#include <physical/memory/Partition.h>
#include <boost/align/aligned_allocator.hpp>

namespace tuplex {

    // because processing happens with 16byte alignment, need to use aligned 16byte strings!
    using aligned_string=std::basic_string<char, std::char_traits<char>, boost::alignment::aligned_allocator<char, 16>>;

    enum class SamplingMode {
        SAMPLE_FIRST_ROWS,
        SAMPLE_LAST_ROWS,
        SAMPLE_RANDOM
    };

    /*!
     * CSV operator will immediately load data into memory when added
     */
    class FileInputOperator : public LogicalOperator {
    private:
        std::vector<URI>      _fileURIs;
        std::vector<size_t>   _sizes;
        size_t _estimatedRowCount; // number of rows estimated for these files...

        FileFormat _fmt;

        // CSV Fields
        char _quotechar;
        char _delimiter;
        bool _header;
        std::vector<std::string> _null_values;

        Schema _optimizedSchema; // schema after selection pushdown is performed.
        std::vector<std::string> _columnNames;
        std::vector<std::string> _optimizedColumnNames;
        std::vector<bool> _columnsToSerialize; // which columns to serialize

        std::unordered_map<size_t, python::Type> _indexBasedHints;

        python::Type _normalCaseRowType;
        python::Type _optimizedNormalCaseRowType;

        // internal sample, used for tracing & Co.
        std::vector<Row> _firstRowsSample;
        std::vector<Row> _lastRowsSample;

        void detectFiles(const std::string& pattern);

        // TODO: Refactor constructors

        // CSV Constructor
        FileInputOperator(const std::string& pattern,
                          const ContextOptions& co,
                          option<bool> hasHeader,
                          option<char> delimiter,
                          option<char> quotechar,
                          const std::vector<std::string>& null_values,
                          const std::vector<std::string>& column_name_hints,
                          const std::unordered_map<size_t, python::Type>& index_based_type_hints,
                          const std::unordered_map<std::string, python::Type>& column_based_type_hints);

        // Text Constructor
        FileInputOperator(const std::string& pattern,
                          const ContextOptions& co,
                          const std::vector<std::string>& null_values);

        // Orc Constructor
        FileInputOperator(const std::string& pattern,
                          const ContextOptions& co);

        FileInputOperator(FileInputOperator& other); // specialized copy constructor!

        aligned_string loadSample(size_t sampleSize, const URI& uri, size_t file_size, const SamplingMode& mode);

        double _sampling_time_s;

        std::vector<size_t> translateOutputToInputIndices(const std::vector<size_t>& output_indices);
    public:

        // required by cereal
        // make private
        FileInputOperator() = default;

        /*!
         * create a new CSV File Input operator
         * @param pattern files to search for
         * @param co ContextOptions, pipeline will take configuration for planning from there
         * @param hasHeader whether file has a header or not
         * @param delimiter delimiter
         * @param quotechar quote char, i.e. only understood dialect is RFC compliant one
         * @param null_values which strings to interpret as null values
         * @param type_hints an optional mapping of column index to type if a certain type should be enforced for a column
         */
        static FileInputOperator *fromCsv(const std::string& pattern,
                                         const ContextOptions& co,
                                         option<bool> hasHeader,
                                         option<char> delimiter,
                                         option<char> quotechar,
                                         const std::vector<std::string>& null_values,
                                         const std::vector<std::string>& column_name_hints,
                                         const std::unordered_map<size_t, python::Type>& index_based_type_hints,
                                         const std::unordered_map<std::string, python::Type>& column_based_type_hints);

        /*!
         * create a new Text File Input operator
         * @param pattern files to search for
         * @param co ContextOptions, pipeline will take configuration for planning from there
         * @param null_values which strings to interpret as null values
         */
        static FileInputOperator *fromText(const std::string& pattern,
                                          const ContextOptions& co,
                                          const std::vector<std::string>& null_values);

        /*!
        * create a new orc File Input operator.
        * @param pattern files to search for
        * @param co ContextOptions, pipeline will take configuration for planning from there
        */
        static FileInputOperator *fromOrc(const std::string& pattern,
                                         const ContextOptions& co);

        std::string name() override {
            switch (_fmt) {
                case FileFormat::OUTFMT_CSV:
                    return "csv";
                case FileFormat::OUTFMT_TEXT:
                    return "txt";
                case FileFormat::OUTFMT_ORC:
                    return "orc";
                default:
                    auto &logger = Logger::instance().logger("fileinputoperator");
                    std::stringstream ss;
                    ss << "unknown file input operator with integer value of " << std::to_string((int) _fmt);
                    logger.error(ss.str());
                    return ss.str();
            }
        }

        LogicalOperatorType type() const override { return LogicalOperatorType::FILEINPUT; }

        bool isActionable() override { return false; }

        bool isDataSource() override { return true; }

        /*!
         * get the partitions where the parallelized data is stored.
         * @return vector of partitions.
         */
        std::vector<tuplex::Partition*> getPartitions();

        bool good() const override { return true; }

        std::vector<URI> getURIs() const { return _fileURIs; }
        std::vector<size_t> getURISizes() const { return _sizes; }

        double samplingTime() const { return _sampling_time_s; }
        // CSV Only Operations
        // @Todo: add here check that estimate has been called.
        bool hasHeader() const { return _header; }
        std::vector<std::string> header() const { return columns(); }
        char delimiter() const { return _delimiter;}
        char quotechar() const { return _quotechar;}
        std::vector<std::string> null_values() const { return _null_values; }

        std::unordered_map<size_t, python::Type> typeHints() const { return _indexBasedHints; }

        /*!
         * force usage of normal case type for schema & Co.
         */
        void useNormalCase() {
            auto ml = _optimizedSchema.getMemoryLayout();
            setSchema(Schema(ml, _normalCaseRowType));
            _optimizedSchema = Schema(ml, _optimizedNormalCaseRowType);
        }

        Schema getOptimizedOutputSchema() const {
            auto ml = _optimizedSchema.getMemoryLayout();
            return  Schema(ml, _optimizedNormalCaseRowType);
        }

        Schema getOptimizedInputSchema() const {
            auto ml = _optimizedSchema.getMemoryLayout();
            return  Schema(ml, _normalCaseRowType);
        }

        std::vector<Row> getSample(const size_t num) const override;

        Schema getInputSchema() const override {
            // get here the original, saved output schema => required when matching input against normal case
            return LogicalOperator::getOutputSchema();
        }

        Schema getOutputSchema() const override { return _optimizedSchema; }

        /*!
         * calls this function to output only a partial number of columns. I.e. used in optimizer for projectionPushdown
         * @param columnsToSerialize indices of the columns to serialize/keep
         * @param original_indices flag indicating whether indices are wrt to original input rows, i.e. in the range of
         *                         [0, ..., inputColumnCount() - 1] or indices relative to the output columns
         */
        void selectColumns(const std::vector<size_t>& columnsToSerialize, bool original_indices=true);

        /*!
         * explicitly define some column names for this operator.
         * @param columnNames
         */
        void setColumns(const std::vector<std::string>& columnNames);

        /*!
         * specialize input type, e.g. based on sample. checks that type is compatible
         * @param rowTypes
         * @return true if successful, false else.
         */
        bool retype(const std::vector<python::Type>& rowTypes=std::vector<python::Type>()) override;

        std::vector<std::string> columns() const override { return _optimizedColumnNames; }
        std::vector<std::string> inputColumns() const override { return _columnNames; }
        std::vector<bool> columnsToSerialize() const { return _columnsToSerialize; }

        /*!
         * number of output columns this file input operator yields AFTER projection pushdown.
         * @return number of output columns.
         */
        inline size_t outputColumnCount() const {
            // fetch number of columns, either from names or type
            size_t num_cols = 0;
            if(columns().empty())
                num_cols = _optimizedSchema.getRowType().parameters().size();
            else {
                if(columns().size() != _optimizedSchema.getRowType().parameters().size()) // important to hold
                    throw std::runtime_error("size of columns stored (" + std::to_string(columns().size()) + ") does not match actual, projected number of columns("
                                             + std::to_string(_optimizedSchema.getRowType().parameters().size()) + ").");
                num_cols = columns().size();
            }
            return num_cols;
        }

        /*!
         * number of input columns in actual file (i.e. BEFORE/WITHOUT projection pushdown)
         * @return number of input columns in file
         */
        inline size_t inputColumnCount() const {
            return getInputSchema().getRowType().parameters().size();
        }

        /*!
         * get a mapping which index is mapped to which index after projection pushdown. Empty, if there's no projection pushdown.
         * @return
         */
        std::unordered_map<int, int> projectionMap() const;

        FileFormat fileFormat() const { return _fmt; }
        std::shared_ptr<LogicalOperator> clone() override;

        void setProjectionDefaults();

        int64_t cost() const override;

        /*!
         * check whether files detected are all empty or not. I.e., there are no files OR the sum of all file sizes is 0.
         * @return true or false
         */
        bool isEmpty() const;

        // HACK quick n dirty serializatio n (targeting only the CSV case...)
        inline nlohmann::json to_json() const {
            nlohmann::json obj;
            obj["name"] = "csv"; // hack
            auto uris = nlohmann::json::array();
            for(auto uri : _fileURIs)
                uris.push_back(uri.toString());
            auto sizes = nlohmann::json::array();
            for(auto s : _sizes)
                sizes.push_back(s);
            obj["uris"] = uris;
            obj["sizes"] = sizes;
            obj["quotechar"] = _quotechar;
            obj["delimiter"] = _delimiter;
            obj["hasHeader"] = _header;
            obj["null_values"] = _null_values;

            obj["columnNames"] = _columnNames;
            obj["optimizedColumnNames"] = _optimizedColumnNames;
            obj["columnsToSerialize"] = _columnsToSerialize;

            obj["schema"] = schema().getRowType().desc();
            obj["optimizedSchema"] = _optimizedSchema.getRowType().desc();
            obj["normalCaseRowType"] = _normalCaseRowType.desc();
            obj["optimizedNormalCaseRowType"] = _optimizedNormalCaseRowType.desc();

            // skip index based hints...

            obj["id"] = getID();

            return obj;
        }

        // HACK !!!
        void setInputFiles(const std::vector<URI>& uris, const std::vector<size_t>& uri_sizes, bool resample=false);

        // HACK !!!
        static FileInputOperator* from_json(nlohmann::json obj) {

            auto fop = new FileInputOperator();
            fop->_fmt = FileFormat::OUTFMT_CSV;
            fop->_quotechar = obj["quotechar"].get<char>();
            fop->_delimiter = obj["delimiter"].get<char>();
            fop->_header = obj["hasHeader"].get<bool>();
            for(auto uri : obj["uris"])
                fop->_fileURIs.push_back(uri.get<std::string>());
            fop->_sizes = obj["sizes"].get<std::vector<size_t>>();
            fop->_null_values = obj["null_values"].get<std::vector<std::string>>();
            fop->_columnNames = obj["columnNames"].get<std::vector<std::string>>();
            fop->_optimizedColumnNames = obj["optimizedColumnNames"].get<std::vector<std::string>>();
            fop->_columnsToSerialize = obj["columnsToSerialize"].get<std::vector<bool>>();

            fop->_optimizedSchema = Schema(Schema::MemoryLayout::ROW, python::decodeType(obj["optimizedSchema"].get<std::string>()));
            fop->_normalCaseRowType = python::decodeType(obj["normalCaseRowType"].get<std::string>());
            fop->_optimizedNormalCaseRowType = python::decodeType(obj["optimizedNormalCaseRowType"].get<std::string>());

            fop->setID(obj["id"]);
            auto schema = Schema(Schema::MemoryLayout::ROW, python::decodeType(obj["schema"].get<std::string>()));
            fop->setSchema(schema);
            return fop;
        }

#ifdef BUILD_WITH_CEREAL
        // DO NOT MIX load/save with serialize.
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(cereal::base_class<LogicalOperator>(this),
                    _fileURIs, _sizes, _estimatedRowCount, _fmt, _quotechar, _delimiter, _header, _null_values, _optimizedSchema,
                    _columnNames, _optimizedColumnNames,
                    _columnsToSerialize, _indexBasedHints,
                    _normalCaseRowType, _optimizedNormalCaseRowType,
                    _sampling_time_s); // do NOT serialize samples!
        }

        template<class Archive> void load(Archive &ar) {
            ar(cereal::base_class<LogicalOperator>(this),
               _fileURIs, _sizes, _estimatedRowCount, _fmt, _quotechar, _delimiter, _header, _null_values, _optimizedSchema,
               _columnNames, _optimizedColumnNames,
               _columnsToSerialize, _indexBasedHints,
               _normalCaseRowType, _optimizedNormalCaseRowType,
               _sampling_time_s); // do NOT serialize samples!
        }
#endif
    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::FileInputOperator)
#endif

#endif
//TUPLEX_FILEINPUTOPERATOR_H