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
#include "parameters.h"
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

        // JSON fields
        bool _json_unwrap_first_level;
        bool _json_treat_heterogenous_lists_as_tuples;

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
        std::mutex _sampleCacheMutex;
        bool _cachePopulated;
        std::vector<Row> _rowsSample;
        bool _isRowSampleProjected;

        /*!
         * project sample according to map set for this operator
         * @param rows
         * @return projected rows
         */
        std::vector<Row> projectSample(const std::vector<Row>& rows) const;

        // file cache (i.e. just the samples from the files)
        void fillFileCache(SamplingMode mode);
        void fillFileCacheMultithreaded(SamplingMode mode);
        void fillFileCacheSinglethreaded(SamplingMode mode);

        // row cache (i.e. parse samples utilizing file cache)
        void fillRowCache(SamplingMode mode, std::vector<std::vector<std::string>>* outNames=nullptr,
                          size_t sample_limit=std::numeric_limits<size_t>::max());

        void fillRowCacheWithStratifiedSamples(SamplingMode mode, std::vector<std::vector<std::string>>* outNames=nullptr,
                                               size_t sample_limit_after_strata=std::numeric_limits<size_t>::max(),
                                               size_t strata_size=128, size_t samples_per_strata=1, int random_seed=-1);

        size_t estimateTextFileRowCount(size_t sample_size, const SamplingMode& mode);
        size_t estimateSampleBasedRowCount();

        std::tuple<python::Type, python::Type> detectJsonTypesAndColumns(const ContextOptions& co,
                                                                         const std::vector<std::vector<std::string>>& columnNamesSampleCollection);

        // for CSV, have here a global csv stat (that can get reset)
        // ??

//        // internal sample, used for tracing & Co.
//        std::vector<Row> _firstRowsSample;
//        std::vector<Row> _lastRowsSample;

        // *** helper functions ***
        inline Schema normalCaseSchema() const { return Schema(Schema::MemoryLayout::ROW, _normalCaseRowType); }
        inline Schema generalCaseSchema() const { return Schema(Schema::MemoryLayout::ROW, _generalCaseRowType); }

        inline bool usesProjectionMap() const {
            if(_columnsToSerialize.empty())
                return false;
            for(auto c : _columnsToSerialize) {
                if(!c)
                    return true;
            }
            return false;
        }

    public:
        // helper function to project a schema
        inline python::Type projectRowType(const python::Type& rowType) const {
            if(_columnsToSerialize.empty() || python::Type::EMPTYTUPLE == rowType) {
                // not set, use original
                return rowType;
            }

            if(PARAM_USE_ROW_TYPE) {
                assert(rowType.isRowType());
                if(python::Type::EMPTYROW == rowType)
                    return rowType;

                assert(_columnsToSerialize.size() == rowType.get_column_names().size());
                auto column_names = rowType.get_column_names();
                std::vector<python::Type> projected_column_types;
                std::vector<std::string> projected_column_names;
                for(unsigned i = 0; i < std::min(column_names.size(), _columnsToSerialize.size()); ++i) {
                    if(_columnsToSerialize[i]) {
                        projected_column_types.push_back(rowType.get_column_type(i));
                        projected_column_names.push_back(column_names[i]);
                    }
                }
                return python::Type::makeRowType(projected_column_types, projected_column_names);
            }

            // use columns to serialize to get projected type
            if(!rowType.isTupleType())
                throw std::runtime_error("can't project row type, expected a tuple type but got " + rowType.desc());

            assert(rowType.isTupleType());
            // assert(rowType.parameters().size() == _columnsToSerialize.size());

            auto params = rowType.parameters();
            std::vector<python::Type> col_types;
            for(unsigned i = 0; i < std::min(params.size(), _columnsToSerialize.size()); ++i) {
                if(_columnsToSerialize[i])
                    col_types.push_back(params[i]);
            }
            return python::Type::makeTupleType(col_types);
        }

        /*!
         * set sampling size (i.e., for hyperspecializastion)
         * @param sampling_size
         */
        void setSamplingSize(size_t sampling_size) { _samplingSize = sampling_size; }

        inline size_t reverseProjectToReadIndex(size_t projected_index) const {
#ifndef NDEBUG
            if(projected_index >= outputColumnCount()) {
                auto& logger = Logger::instance().logger("codegen");
                logger.error("projected index " + std::to_string(projected_index) + " is greater equal than current number of output columns " + std::to_string(outputColumnCount()));
            }
#endif
            assert(projected_index < outputColumnCount());

            auto map = projectionMap();

            // empty map? no mapping here. return same index
            if(map.empty())
                return projected_index;

            // map is read index -> projected index, i.e. this here is a reverse lookup
            for(auto kv : map) {
                if(kv.second == projected_index)
                    return kv.first;
            }
            throw std::runtime_error("could not reverse project index");
        }

        inline size_t projectReadIndex(size_t read_index) const {
            if(read_index >= inputColumnCount())
                throw std::runtime_error("read_index is larger than number of input columns");
            int idx = -1;
            for(unsigned i = 0; i <= read_index; ++i) {
                if(_columnsToSerialize[i])
                    idx++;
            }
            return idx;
        }

        /*!
         * force set to sample (used by filter promo). Make friend maybe?
         */
        void setRowsSample(const std::vector<Row>& sample, bool is_projected=true);
    private:
        inline std::vector<std::string> projectColumns(const std::vector<std::string>& columns) const {
            if(_columnsToSerialize.empty() || _columnNames.empty())
                return {};

            //assert(_columnsToSerialize.size() == _columnNames.size());
            std::vector<std::string> names;
            for(unsigned i = 0; i < std::min(_columnNames.size(), _columnsToSerialize.size()); ++i) {
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
                if(PARAM_USE_ROW_TYPE) {
                    assert(generalCaseSchema().getRowType().isRowType());
                    assert(generalCaseSchema().getRowType().get_column_count() == _columnsToSerialize.size());
                } else {
                    assert(generalCaseSchema().getRowType().isTupleType());
                    assert(generalCaseSchema().getRowType().parameters().size() == _columnsToSerialize.size());
                }

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

        aligned_string loadSample(size_t sampleSize, const URI& uri, size_t file_size, const SamplingMode& mode, bool use_cache=true, size_t* file_offset=nullptr);
        std::vector<size_t> translateOutputToInputIndices(const std::vector<size_t>& output_indices);

        struct SamplingParameters {
            size_t limit;
            size_t strata_size;
            size_t samples_per_strata;
            int random_seed;

            inline bool use_stratified_sampling() const {
                assert(strata_size >= 1 && samples_per_strata >= 1);
                auto adj_samples_per_strata = std::min(samples_per_strata, strata_size);
                return (strata_size != 1 || adj_samples_per_strata != strata_size);
            }

            SamplingParameters() : limit(std::numeric_limits<size_t>::max()), strata_size(1), samples_per_strata(1), random_seed(-1) {}
        };

        // sampling functions
        std::vector<Row> sample(const SamplingMode& mode, std::vector<std::vector<std::string>>* outNames=nullptr, const SamplingParameters& sampling_params=SamplingParameters());
        std::vector<Row> multithreadedSample(const SamplingMode& mode, std::vector<std::vector<std::string>>* outNames, const SamplingParameters& sampling_params);
        std::vector<Row> sampleCSVFile(const URI& uri, size_t uri_size, const SamplingMode& mode, const SamplingParameters& sampling_params);
        std::vector<Row> sampleTextFile(const URI& uri, size_t uri_size, const SamplingMode& mode);
        std::vector<Row> sampleORCFile(const URI& uri, size_t uri_size, const SamplingMode& mode);
        std::vector<Row> sampleJsonFile(const URI& uri, size_t uri_size, const SamplingMode& mode,
                                        std::vector<std::vector<std::string>>* outNames, const SamplingParameters& sampling_params);

        /*!
         * samples a file according to internally stored file mode and a per-file sampling mode.
         * @param uri URI of file to sample
         * @param uri_size the actual file size
         * @param mode the per-file sampling mode
         * @param outNames an optional output field for column names (as for now only relevant for JSON)
         * @return row sample.
         */
        inline std::vector<Row> sampleFile(const URI& uri, size_t uri_size,
                                           const SamplingMode& mode, std::vector<std::vector<std::string>>* outNames,
                                           const SamplingParameters& sampling_params) {
            auto& logger = Logger::instance().logger("logical");
            if(!(mode & SamplingMode::FIRST_ROWS) && !(mode & SamplingMode::LAST_ROWS) && !(mode & SamplingMode::RANDOM_ROWS)) {
                logger.debug("no file sampling mode (first rows/last rows/random rows) specified. skip.");
                return {};
            }

            switch(_fmt) {
                case FileFormat::OUTFMT_CSV: {
                    return sampleCSVFile(uri, uri_size, mode, sampling_params);
                }
                case FileFormat::OUTFMT_TEXT: {
                    return sampleTextFile(uri, uri_size, mode);
                }
                case FileFormat::OUTFMT_ORC: {
                    return sampleORCFile(uri, uri_size, mode);
                }
                case FileFormat::OUTFMT_JSON: {
                    return sampleJsonFile(uri, uri_size, mode, outNames, sampling_params);
                }
                default:
                    throw std::runtime_error("unsupported sampling of file format "+ std::to_string(static_cast<int>(this->_fmt)));
            }
            return {};
        }

    public:

        // required by cereal
        // make private
        FileInputOperator();

        /*!
         * create a new CSV File Input operator
         * @param pattern files to search for
         * @param co ContextOptions, pipeline will take configuration for planning from there
         * @param hasHeader whether file has a header or not
         * @param delimiter delimiter
         * @param quotechar quote char, i.e. only understood dialect is RFC compliant one
         * @param null_values which strings to interpret as null values
         * @param type_hints an optional mapping of column index to type if a certain type should be enforced for a column
         * @return input operator
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
         * @return input operator
         */
        static FileInputOperator *fromText(const std::string& pattern,
                                          const ContextOptions& co,
                                          const std::vector<std::string>& null_values,
                                          const SamplingMode& sampling_mode);

        /*!
        * create a new orc File Input operator.
        * @param pattern files to search for
        * @param co ContextOptions, pipeline will take configuration for planning from there
        * return input operator
        */
        static FileInputOperator *fromOrc(const std::string& pattern,
                                         const ContextOptions& co,
                                          const SamplingMode& sampling_mode);

        /*!
         * create new file input operator reading JSON (newline delimited) files.
         * @param pattern pattern to look for files
         * @param unwrap_first_level if true, then the first level is unwrapped. Else, dataset is treated to have a single column.
         * @param treat_heterogenous_lists_as_tuples set to true to lower footprint.
         * @param co context options
         * @return input operator
         */
        static FileInputOperator *fromJSON(const std::string& pattern,
                                           bool unwrap_first_level,
                                           bool treat_heterogenous_lists_as_tuples,
                                           const ContextOptions& co,
                                           const SamplingMode& sampling_mode);


        std::string name() const override {
            switch (_fmt) {
                case FileFormat::OUTFMT_CSV:
                    return "csv";
                case FileFormat::OUTFMT_TEXT:
                    return "txt";
                case FileFormat::OUTFMT_ORC:
                    return "orc";
                case FileFormat::OUTFMT_JSON:
                    return "json";
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

        bool unwrap_first_level() const { return _json_unwrap_first_level; }

        /*!
         * force usage of normal case type for schema & Co.
         */
        void useNormalCase() {
            //auto ml = _optimizedSchema.getMemoryLayout();
            // setSchema(Schema(ml, _normalCaseRowType));
            // _optimizedSchema = Schema(ml, _optimizedNormalCaseRowType);
            //std::cerr<<"DO NOT CALL, deprecated..."<<std::endl;
            _generalCaseRowType = _normalCaseRowType;
            setOutputSchema(Schema(Schema::MemoryLayout::ROW, _generalCaseRowType));
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
         * how many bytes of sampling the current rows in the buffer represent
         * @return size in bytes
         */
        size_t sampleSize() const;

        /*!
         * how many rows are currently stored within sample.
         * @return
         */
        size_t storedSampleRowCount() const;

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
        bool retype(const RetypeConfiguration& conf) override;
        bool retype(const RetypeConfiguration& conf, bool ignore_check_for_str_option=true);

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
            if(PARAM_USE_ROW_TYPE)
                return getInputSchema().getRowType().get_column_count();
            else
                return getInputSchema().getRowType().parameters().size();
        }

        /*!
         * get a mapping which index is mapped to which index after projection pushdown. Empty, if there's no projection pushdown.
         * @return
         */
        std::unordered_map<int, int> projectionMap() const;

        FileFormat fileFormat() const { return _fmt; }
        std::shared_ptr<LogicalOperator> clone(bool cloneParents) override;

        // transfer caches
        void cloneCaches(const FileInputOperator& other);

        void setProjectionDefaults();

        int64_t cost() const override;

        /*!
         * check whether files detected are all empty or not. I.e., there are no files OR the sum of all file sizes is 0.
         * @return true or false
         */
        bool isEmpty() const;

        // HACK quick n dirty serializatio n (targeting only the CSV case...)
        inline nlohmann::json to_json(bool remove_uris=true) const {
            nlohmann::json obj;
            obj["name"] = "input_" + name();
            auto uris = nlohmann::json::array();
            for(auto uri : _fileURIs)
                uris.push_back(uri.toString());
            auto sizes = nlohmann::json::array();
            for(auto s : _sizes)
                sizes.push_back(s);
            if(remove_uris) {
                obj["uris"] = nlohmann::json::array();
                obj["sizes"] = nlohmann::json::array();
                obj["estimatedRowCount"] = 0;
            } else {
                obj["uris"] = uris;
                obj["sizes"] = sizes;
                obj["estimatedRowCount"] = _estimatedRowCount;
            }

            obj["quotechar"] = _quotechar;
            obj["delimiter"] = _delimiter;
            obj["hasHeader"] = _header;
            obj["null_values"] = _null_values;

            obj["fmt"] = (int)_fmt;

            obj["jsonUnwrap"] = _json_unwrap_first_level;
            obj["jsonTuples"] = _json_treat_heterogenous_lists_as_tuples;

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
        void setInputFiles(const std::vector<URI>& uris, const std::vector<size_t>& uri_sizes,
                           bool resample=false, size_t sample_limit=std::numeric_limits<size_t>::max(),
                           bool use_stratified_sampling=false, size_t strata_size=8, size_t samples_per_strata=1,
                           int random_seed=-1);

        // HACK !!!
        static FileInputOperator* from_json(nlohmann::json obj) {

            auto fop = new FileInputOperator();
            fop->_fmt = static_cast<FileFormat>(obj["fmt"].get<int>());
            fop->_quotechar = obj["quotechar"].get<char>();
            fop->_delimiter = obj["delimiter"].get<char>();
            fop->_header = obj["hasHeader"].get<bool>();

            fop->_json_unwrap_first_level = obj["jsonUnwrap"].get<bool>();
            fop->_json_treat_heterogenous_lists_as_tuples = obj["jsonTuples"].get<bool>();
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
            fop->setOutputSchema(schema);
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
                    _json_unwrap_first_level,
                    _json_treat_heterogenous_lists_as_tuples,
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
                _json_unwrap_first_level,
                _json_treat_heterogenous_lists_as_tuples,
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