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

    /*!
     * CSV operator will immediately load data into memory when added
     */
    class FileInputOperator : public LogicalOperator {
    private:
        // *** members to serialize ***
        std::vector<URI>      _fileURIs;
        std::vector<size_t>   _sizes;
        size_t _estimatedRowCount; // number of rows estimated for these files...

        FileFormat _fmt;

        // CSV Fields
        char _quotechar;
        char _delimiter;
        bool _header;

        // general fields for managing both cases & projections
        std::vector<std::string> _null_values;
        std::vector<bool> _columnsToSerialize; // which columns to serialize
        std::vector<std::string> _columnNames;

        // specialized normal-case type
        python::Type _normalCaseRowType;
        python::Type _generalCaseRowType;
        std::unordered_map<size_t, python::Type> _indexBasedHints;

        // *** members NOT to serialize ***
        // Variables that wont' get serialized.
        double _sampling_time_s;
        SamplingMode _samplingMode;
        size_t _samplingSize;

        // a sample cache (avoids sampling the same file over and over again).
        // -> load_sample uses this
        std::unordered_map<std::tuple<URI, SamplingMode>, aligned_string> _sampleCache;
        bool _cachePopulated;
        std::vector<Row> _rowsSample;
        void fillCache(SamplingMode mode);

        // for CSV, have here a global csv stat (that can get reset)
        // ??

//        // internal sample, used for tracing & Co.
//        std::vector<Row> _firstRowsSample;
//        std::vector<Row> _lastRowsSample;

        // *** helper functions ***
        inline Schema normalCaseSchema() const { return Schema(Schema::MemoryLayout::ROW, _normalCaseRowType); }
        inline Schema generalCaseSchema() const { return Schema(Schema::MemoryLayout::ROW, _generalCaseRowType); }

    public:
        // helper function to project a schema
        inline python::Type projectRowType(const python::Type& rowType) const {
            if(_columnsToSerialize.empty()) {
                // not set, use original
                return rowType;
            }
            // use columns to serialize to get projected type
            assert(rowType.isTupleType());
            assert(rowType.parameters().size() == _columnsToSerialize.size());

            auto params = rowType.parameters();
            std::vector<python::Type> col_types;
            for(unsigned i = 0; i < _columnsToSerialize.size(); ++i) {
                if(_columnsToSerialize[i])
                    col_types.push_back(params[i]);
            }
            return python::Type::makeTupleType(col_types);
        }
    private:
        inline std::vector<std::string> projectColumns(const std::vector<std::string>& columns) const {
            if(_columnsToSerialize.empty() || _columnNames.empty())
                return {};

            assert(_columnsToSerialize.size() == _columnNames.size());
            std::vector<std::string> names;
            for(unsigned i = 0; i < _columnsToSerialize.size(); ++i) {
                if(_columnsToSerialize[i])
                    names.push_back(_columnNames[i]);
            }
            return names;
        }

        inline Schema projectSchema(const Schema& schema) const {
            return Schema(schema.getMemoryLayout(), projectRowType(schema.getRowType()));
        }

        inline size_t num_projected_columns() const {
            // how many columns are there?
            if(_columnsToSerialize.empty()) {
                // take general case row type!
                assert(generalCaseSchema().getRowType().isTupleType());
                assert(generalCaseSchema().getRowType().parameters().size() == _columnsToSerialize.size());

#ifndef NDEBUG
                std::cerr<<"WARNING: weird internal behavior in file input operator...?"<<std::endl;
#endif

                return generalCaseSchema().getRowType().parameters().size();
            } else {
                // count!
                assert(generalCaseSchema().getRowType().isTupleType());
                assert(generalCaseSchema().getRowType().parameters().size() == _columnsToSerialize.size());

                size_t num = 0;
                for(auto flag : _columnsToSerialize)
                    num += flag;
                return num;
            }
        }

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
                          const std::unordered_map<std::string, python::Type>& column_based_type_hints,
                          const SamplingMode& sampling_mode);

        // Text Constructor
        FileInputOperator(const std::string& pattern,
                          const ContextOptions& co,
                          const std::vector<std::string>& null_values,
                          const SamplingMode& sampling_mode);

        // Orc Constructor
        FileInputOperator(const std::string& pattern,
                          const ContextOptions& co,
                          const SamplingMode& sampling_mode);

        FileInputOperator(FileInputOperator& other); // specialized copy constructor!

        aligned_string loadSample(size_t sampleSize, const URI& uri, size_t file_size, const SamplingMode& mode, bool use_cache=true);
        std::vector<size_t> translateOutputToInputIndices(const std::vector<size_t>& output_indices);

        // sampling functions
        std::vector<Row> sample(const SamplingMode& mode);
        std::vector<Row> multithreadedSample(const SamplingMode& mode);
        std::vector<Row> sampleCSVFile(const URI& uri, size_t uri_size, const SamplingMode& mode);
        std::vector<Row> sampleTextFile(const URI& uri, size_t uri_size, const SamplingMode& mode);
        std::vector<Row> sampleORCFile(const URI& uri, size_t uri_size, const SamplingMode& mode);
        inline std::vector<Row> sampleFile(const URI& uri, size_t uri_size, const SamplingMode& mode) {
            auto& logger = Logger::instance().logger("logical");
            if(!(mode & SamplingMode::FIRST_ROWS) && !(mode & SamplingMode::LAST_ROWS) && !(mode & SamplingMode::RANDOM_ROWS)) {
                logger.debug("no file sampling mode (first rows/last rows/random rows) specified. skip.");
                return {};
            }

            switch(_fmt) {
                case FileFormat::OUTFMT_CSV: {
                    return sampleCSVFile(uri, uri_size, mode);
                }
                case FileFormat::OUTFMT_TEXT: {
                    return sampleTextFile(uri, uri_size, mode);
                }
                case FileFormat::OUTFMT_ORC: {
                    return sampleORCFile(uri, uri_size, mode);
                }
                default:
                    throw std::runtime_error("unsupported sampling of file format "+ std::to_string(static_cast<int>(this->_fmt)));
            }
            return {};
        }
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
                                         const std::unordered_map<std::string, python::Type>& column_based_type_hints,
                                          const SamplingMode& sampling_mode);

        /*!
         * create a new Text File Input operator
         * @param pattern files to search for
         * @param co ContextOptions, pipeline will take configuration for planning from there
         * @param null_values which strings to interpret as null values
         */
        static FileInputOperator *fromText(const std::string& pattern,
                                          const ContextOptions& co,
                                          const std::vector<std::string>& null_values,
                                           const SamplingMode& sampling_mode);

        /*!
        * create a new orc File Input operator.
        * @param pattern files to search for
        * @param co ContextOptions, pipeline will take configuration for planning from there
        */
        static FileInputOperator *fromOrc(const std::string& pattern,
                                         const ContextOptions& co,
                                          const SamplingMode& sampling_mode);

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
            //auto ml = _optimizedSchema.getMemoryLayout();
            // setSchema(Schema(ml, _normalCaseRowType));
            // _optimizedSchema = Schema(ml, _optimizedNormalCaseRowType);
            //std::cerr<<"DO NOT CALL, deprecated..."<<std::endl;
            _generalCaseRowType = _normalCaseRowType;
            setSchema(Schema(Schema::MemoryLayout::ROW, _generalCaseRowType));
        }

        /*!
         * gets the projected output schema for the normal case
         * @return normal case + projected schema
         */
        Schema getOptimizedOutputSchema() const {
//            auto ml = _optimizedSchema.getMemoryLayout();
//            return  Schema(ml, _optimizedNormalCaseRowType);
            return projectSchema(normalCaseSchema());
        }

        /*!
         * gets the normal case schema (no projection!)
         * @return normal case schema
         */
        Schema getOptimizedInputSchema() const {
            return normalCaseSchema();
//            auto ml = _optimizedSchema.getMemoryLayout();
//            return  Schema(ml, _normalCaseRowType);
        }

        std::vector<Row> getSample(const size_t num) const override;

        Schema getInputSchema() const override {
            // get here the original, saved output schema => required when matching input against normal case
            return LogicalOperator::getOutputSchema();
        }

        /*!
         * returns general case schema. I.e., the schema the operator yield AFTER pushdown. If the operator is forced
         * to use normal-case schema, this will yield the normal-case schema as well.
         */
        Schema getOutputSchema() const override {
            return projectSchema(generalCaseSchema());
        }

        Schema getProjectedOutputSchema() const {
            return projectSchema(generalCaseSchema());
        }

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


        bool retype(const python::Type& rowType, bool ignore_check_for_str_option=true);

        std::vector<std::string> columns() const override { return projectColumns(_columnNames); } //{ return _optimizedColumnNames; }
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
                num_cols = num_projected_columns();
            else {
                if(columns().size() != num_projected_columns()) // important to hold
                    throw std::runtime_error("size of columns stored (" + std::to_string(columns().size()) + ") does not match actual, projected number of columns("
                                             + std::to_string(num_projected_columns()) + ").");
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
            obj["estimatedRowCount"] = _estimatedRowCount;
            obj["quotechar"] = _quotechar;
            obj["delimiter"] = _delimiter;
            obj["hasHeader"] = _header;
            obj["null_values"] = _null_values;

            obj["samplingMode"] = (int)_samplingMode;
            obj["samplingSize"] = _samplingSize;

            obj["columnNames"] = _columnNames;
            obj["columnsToSerialize"] = _columnsToSerialize;

            obj["normalCaseRowType"] = _normalCaseRowType.desc();
            obj["generalCaseRowType"] = _generalCaseRowType.desc();
            // not really needed...
            // obj["indexBasedHints"] = _indexBasedHints;

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
            for(const auto& uri : obj["uris"])
                fop->_fileURIs.push_back(uri.get<std::string>());
            fop->_sizes = obj["sizes"].get<std::vector<size_t>>();
            fop->_estimatedRowCount = obj["estimatedRowCount"].get<size_t>();
            fop->_null_values = obj["null_values"].get<std::vector<std::string>>();
            fop->_columnNames = obj["columnNames"].get<std::vector<std::string>>();
            fop->_columnsToSerialize = obj["columnsToSerialize"].get<std::vector<bool>>();

            fop->_samplingMode = static_cast<SamplingMode>(obj["samplingMode"].get<int>());
            fop->_samplingSize = static_cast<SamplingMode>(obj["samplingSize"].get<int>());

            fop->_normalCaseRowType = python::decodeType(obj["normalCaseRowType"].get<std::string>());
            fop->_generalCaseRowType = python::decodeType(obj["generalCaseRowType"].get<std::string>());

            fop->setID(obj["id"]);
            auto schema = Schema(Schema::MemoryLayout::ROW, fop->_generalCaseRowType);
            fop->setSchema(schema);
            return fop;
        }

#ifdef BUILD_WITH_CEREAL
        // DO NOT MIX load/save with serialize.
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(cereal::base_class<LogicalOperator>(this),
                    _fileURIs,
                    _sizes,
                    _estimatedRowCount,
                    _fmt,
                    _quotechar,
                    _delimiter,
                    _header,
                    _null_values,
                    _columnNames,
                    _columnsToSerialize,
                    _indexBasedHints,
                    _normalCaseRowType,
                    _generalCaseRowType,
                    _samplingMode,
                    _samplingSize); // do NOT serialize samples!
        }

        template<class Archive> void load(Archive &ar) {
            ar(cereal::base_class<LogicalOperator>(this),
               _fileURIs,
                _sizes,
                _estimatedRowCount,
                _fmt,
                _quotechar,
                _delimiter,
                _header,
                _null_values,
                _columnNames,
                _columnsToSerialize,
                _indexBasedHints,
                _normalCaseRowType,
                _generalCaseRowType,
                _samplingMode,
                _samplingSize); // do NOT serialize samples!
        }
#endif
    };
}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::FileInputOperator)
#endif

#endif
//TUPLEX_FILEINPUTOPERATOR_H