//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/FileInputOperator.h>
#include <VirtualFileSystem.h>
#include <orc/VirtualInputStream.h>
#include <CSVUtils.h>
#include <CSVStatistic.h>
#include <algorithm>
#include <orc/OrcTypes.h>

namespace tuplex {

    std::vector<tuplex::Partition*> FileInputOperator::getPartitions() {
        return _partitions;
    }

    void FileInputOperator::detectFiles(const std::string& pattern) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        // walk files
        size_t totalInputSize = 0;
        size_t printFactor = 1000;
        VirtualFileSystem::walkPattern(URI(pattern), [&](void *userData, const tuplex::URI &uri, size_t size) {
            _fileURIs.push_back(uri);
            _sizes.push_back(size);
            totalInputSize += size;
            // print every printFactor files out, how many were found
            if (_fileURIs.size() % printFactor == 0) {
            logger.info("found " + pluralize(_fileURIs.size(), "file") + " (" + sizeToMemString(totalInputSize) +
            ") so far...");
            }
            return true;
        });

        logger.info("found " + pluralize(_fileURIs.size(), "file") + " (" + sizeToMemString(totalInputSize) +
        ") to process.");
    }

    aligned_string FileInputOperator::loadSample(size_t sampleSize) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        if(_fileURIs.empty())
            return "";

        // load from first file (?)

        // @TODO: estimate across multiple files/different file positions
        // ==> how to do this better??

        auto uri = _fileURIs.front();
        size_t size = _sizes.front();
        auto vfs = VirtualFileSystem::fromURI(uri);
        auto vf = vfs.open_file(uri, VirtualFileMode::VFS_READ);
        if (!vf) {
            logger.error("could not open file " + uri.toString());
            return "";
        }

        // determine sample size
        if (size > sampleSize)
            sampleSize = core::floorToMultiple(std::min(sampleSize, size), 16ul);
        else {
            sampleSize = core::ceilToMultiple(std::min(sampleSize, size), 16ul);
        }

        assert(sampleSize % 16 == 0); // needs to be divisible by 16...

        // memory must be at least 16
        auto nbytes_sample = sampleSize + 16 + 1;
        aligned_string sample(nbytes_sample, '\0');
        assert(sample.capacity() > 16 && sample.size() > 16);

        // copy out memory from file for analysis
        char *start = new char[sampleSize + 16 + 1];
        // memset last 16 bytes to 0
        assert(start + sampleSize - 16ul >= start);
        assert(sampleSize >= 16ul);
        std::memset(start + sampleSize - 16ul, 0, 16ul);
        // read contents
        size_t bytesRead = 0;
        vf->readOnly(start, sampleSize, &bytesRead); // use read-only here to speed up sampling
        auto end = start + sampleSize;
        start[sampleSize] = 0; // important!

        sample.assign(start, sampleSize+1);
        delete [] start;

        Logger::instance().defaultLogger().info(
                "sampled " + uri.toString() + " on " + sizeToMemString(sampleSize));
        return sample;
    }

    FileInputOperator *FileInputOperator::fromText(const std::string &pattern, const ContextOptions &co,
                                                    const std::vector<std::string> &null_values) {
        return new FileInputOperator(pattern, co, null_values);
    }

    FileInputOperator::FileInputOperator(const std::string& pattern,
            const ContextOptions& co,
            const std::vector<std::string>& null_values) : _null_values(null_values), _estimatedRowCount(0), _sampling_time_s(0.0) {
        auto &logger = Logger::instance().logger("fileinputoperator");
        _fmt = FileFormat::OUTFMT_TEXT;

        _indexBasedHints[0] = python::Type::STRING;

        _quotechar = '\0';
        _delimiter = '\0';
        _header = false;
        _columnNames.reserve(1);

        Timer timer;
        detectFiles(pattern);

        // estimate row count
        auto sample = loadSample(co.CSV_MAX_DETECTION_MEMORY());

        // split into lines, compute average length & scale row estimate up
        auto lines = splitToLines(sample.c_str());
        size_t accLineLength = 0;
        for(auto line : lines)
            accLineLength += line.length();
        double avgLineLength = lines.empty() ? 1.0 : accLineLength / (double)lines.size();
        double estimatedRowCount = 0.0;
        for(auto s : _sizes)
            estimatedRowCount += (double)s / (double)avgLineLength;
        _estimatedRowCount = static_cast<size_t>(std::ceil(estimatedRowCount));

        // store as internal sample
        _sample.clear();
        for(auto line : lines) {
            _sample.push_back(Row(line));
        }
        if(_header && !_sample.empty())
            _sample.erase(_sample.begin());

        // when no null-values are given, simply set to string always
        // else, it's an option type...
        auto rowType = python::Type::makeTupleType({python::Type::STRING});
        _normalCaseRowType = rowType;
        if(!null_values.empty())
            rowType = python::Type::makeTupleType({python::Type::makeOptionType(python::Type::STRING)});

        // get type & assign schema
        setSchema(Schema(Schema::MemoryLayout::ROW, rowType));
        setProjectionDefaults();
#ifndef NDEBUG
        logger.info("Created file input operator for text file");
#endif
        _sampling_time_s = timer.time();
    }

    FileInputOperator *FileInputOperator::fromCsv(const std::string &pattern, const ContextOptions &co,
                                                   option<bool> hasHeader,
                                                   option<char> delimiter,
                                                   option<char> quotechar,
                                                   const std::vector<std::string> &null_values,
                                                   const std::vector<std::string>& column_name_hints,
                                                   const std::unordered_map<size_t, python::Type>& index_based_type_hints,
                                                   const std::unordered_map<std::string, python::Type>& column_based_type_hints) {
        return new FileInputOperator(pattern, co, hasHeader, delimiter, quotechar, null_values, column_name_hints, index_based_type_hints, column_based_type_hints);
    }

    FileInputOperator::FileInputOperator(const std::string &pattern, const ContextOptions &co,
                                         option<bool> hasHeader,
                                         option<char> delimiter,
                                         option<char> quotechar,
                                         const std::vector<std::string> &null_values,
                                         const std::vector<std::string>& column_name_hints,
                                         const std::unordered_map<size_t, python::Type>& index_based_type_hints,
                                         const std::unordered_map<std::string, python::Type>& column_based_type_hints) :
                                                                            _null_values(null_values), _sampling_time_s(0.0) {
        auto &logger = Logger::instance().logger("fileinputoperator");
        _fmt = FileFormat::OUTFMT_CSV;

        // meaningful defaults
        _quotechar = quotechar.value_or('"');
        _delimiter = delimiter.value_or(',');
        _header = hasHeader.value_or(false);

//        // null value check. If empty array, add empty string
//        // @TODO: is this correct??
//#warning " check whether this behavior is correct! Users might wanna have empty strings"
//        if (_null_values.empty())
//            _null_values.emplace_back("");

        Timer timer;
        detectFiles(pattern);

        // infer schema using first file only
        if (!_fileURIs.empty()) {

            auto sample = loadSample(co.CSV_MAX_DETECTION_MEMORY());

            CSVStatistic csvstat(co.CSV_SEPARATORS(), co.CSV_COMMENTS(),
                                 co.CSV_QUOTECHAR(), co.CSV_MAX_DETECTION_MEMORY(),
                                 co.CSV_MAX_DETECTION_ROWS(), co.NORMALCASE_THRESHOLD(), _null_values);

            // @TODO: header is missing??
            // "what about if user specifies header? should be accounted for in estimate for small files!!!"
            csvstat.estimate(sample.c_str(), sample.size(), tuplex::option<bool>::none, delimiter, !co.OPT_NULLVALUE_OPTIMIZATION());

            // estimator for #rows across all CSV files
            double estimatedRowCount = 0.0;
            for(auto s : _sizes) {
                estimatedRowCount += (double)s / (double)csvstat.estimationBufferSize() * (double)csvstat.rowCount();
            }
            _estimatedRowCount = static_cast<size_t>(std::ceil(estimatedRowCount));

            // supplied options override estimate!
            _delimiter = delimiter.value_or(csvstat.delimiter());
            _quotechar = quotechar.value_or(csvstat.quotechar());
            _header = hasHeader.value_or(csvstat.hasHeader());

            // set column names from stat
            // Note: call this BEFORE applying type hints!
            _columnNames = _header ? csvstat.columns() : std::vector<std::string>();

            // NULL Value optimization on or off?
            // Note: if off, can improve performance of estimate...

            // normalcase and exception case types
            auto normalcasetype = csvstat.type();
            auto exceptcasetype = csvstat.superType();

            // type hints?
            // ==> enforce certain type for specific columns!
            if(!index_based_type_hints.empty()) {
                assert(exceptcasetype.isTupleType());
                auto paramsNormal = normalcasetype.parameters();
                auto paramsExcept = exceptcasetype.parameters();
                for(const auto& keyval : index_based_type_hints) {
                    if(keyval.first >= paramsExcept.size()) {
                        // simply ignore, do not error!
                        std::stringstream ss;
                        ss<<"invalid index " + std::to_string(keyval.first) + " for type hint in " + name() +
                            " operator found. Must be between 0 and " + std::to_string(paramsExcept.size() - 1);
                        ss<<". Ignoring type hint for index "<<keyval.first;
                        logger.warn(ss.str());
                    }

                    // update params
                    paramsExcept[keyval.first] = keyval.second;
                    paramsNormal[keyval.first] = keyval.second;

                    _indexBasedHints[keyval.first] = keyval.second;
                }

                normalcasetype = python::Type::makeTupleType(paramsNormal);
                exceptcasetype = python::Type::makeTupleType(paramsExcept); // update row type...
            }

            // apply column based type hints
            if(!column_based_type_hints.empty()) {
                assert(exceptcasetype.isTupleType());
                auto paramsNormal = normalcasetype.parameters();
                auto paramsExcept = exceptcasetype.parameters();
                for(const auto& keyval : column_based_type_hints) {

                    // translate name into index, error if out of range...!
                    size_t idx = -1;
                    auto it = std::find(std::begin(_columnNames), std::end(_columnNames), keyval.first);
                    if(it != std::end(_columnNames))
                        idx = std::distance(std::begin(_columnNames), it);
                    else {
                        // try column name hints to determine an index
                        auto it = std::find(std::begin(column_name_hints), std::end(column_name_hints), keyval.first);
                        if(it != std::end(column_name_hints))
                            idx = std::distance(std::begin(column_name_hints), it);
                    }

                    if(idx >= paramsExcept.size()) {
                        // simply ignore, do not error!
                        std::stringstream ss;
                        ss<<"no column named '"<<keyval.first<<"' found in files, ";
                        ss<<"ignoring type hint '"<<keyval.first<<"' : "<<keyval.second.desc();
                        logger.warn(ss.str());
                    }

                    // update params
                    paramsExcept[idx] = keyval.second;
                    paramsNormal[idx] = keyval.second;

                    _indexBasedHints[idx] = keyval.second;
                }

                normalcasetype = python::Type::makeTupleType(paramsNormal);
                exceptcasetype = python::Type::makeTupleType(paramsExcept); // update row type...
            }

            // get type & assign schema
            _normalCaseRowType = normalcasetype;
            setSchema(Schema(Schema::MemoryLayout::ROW, exceptcasetype));

            // if csv stat produced unknown, issue warning and use empty
            if (csvstat.type() == python::Type::UNKNOWN) {
                setSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
                Logger::instance().defaultLogger().warn("csv stats could not determine type, use () for type.");
            }

            // set defaults for possible projection pushdown...
            setProjectionDefaults();

            // now parse from the allocated buffer all rows and store as internal sample.
            // there should be at least one 3 rows!
            // store as internal sample
            // => use here strlen because _sample is zero terminated!
            assert(sample.back() == '\0');
            _sample = parseRows(sample.c_str(), sample.c_str() + std::min(sample.size() - 1, strlen(sample.c_str())), _null_values, _delimiter, _quotechar);
            // header? ignore first row!
            if(_header && !_sample.empty())
                _sample.erase(_sample.begin());
        } else {
            logger.warn("no input files found, can't infer type from given path: " + pattern);
            setSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
        }
        _sampling_time_s += timer.time();
    }

    FileInputOperator *FileInputOperator::fromOrc(const std::string &pattern, const ContextOptions &co) {
        return new FileInputOperator(pattern, co);
    }

    FileInputOperator::FileInputOperator(const std::string &pattern, const ContextOptions &co): _sampling_time_s(0.0) {

#ifdef BUILD_WITH_ORC
        auto &logger = Logger::instance().logger("fileinputoperator");
        _fmt = FileFormat::OUTFMT_ORC;
        Timer timer;
        detectFiles(pattern);

        if (!_fileURIs.empty()) {
            auto uri = _fileURIs.front();

            using namespace ::orc;
            auto inStream = std::make_unique<orc::VirtualInputStream>(uri);
            ReaderOptions options;
            ORC_UNIQUE_PTR<Reader> reader = createReader(std::move(inStream), options);

            auto &orcType = reader->getType();
            auto numRows = reader->getNumberOfRows();
            std::vector<bool> columnHasNull;
            for (int i = 0; i < orcType.getSubtypeCount(); ++i) {
                columnHasNull.push_back(reader->getColumnStatistics(i + 1)->hasNull());
            }
            auto tuplexType = tuplex::orc::orcRowTypeToTuplex(orcType, columnHasNull);

            _normalCaseRowType = tuplexType;
            _estimatedRowCount = numRows * _fileURIs.size();

            bool hasColumnNames = false;
            for (uint64_t i = 0; i < orcType.getSubtypeCount(); ++i) {
                const auto& name = orcType.getFieldName(i);
                if (!name.empty()) {
                    hasColumnNames = true;
                }
                _columnNames.push_back(name);
            }
            if (!hasColumnNames) {
                _columnNames.clear();
            }

            inStream.reset();
            reader.reset();

            setSchema(Schema(Schema::MemoryLayout::ROW, tuplexType));

            setProjectionDefaults();
        } else {
            logger.warn("no input files found, can't infer type from sample.");
            setSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
        }
        _sampling_time_s += timer.time();
#else
        throw std::runtime_error(MISSING_ORC_MESSAGE);
#endif
    }

    void FileInputOperator::setProjectionDefaults() {// set optimized schema to current one
        // set optimized schema to current one
        _optimizedSchema = getInputSchema();
        _optimizedColumnNames = _columnNames;
        // create per default true array, i.e. serialize all columns
        _columnsToSerialize = std::vector<bool>();

        for (int i = 0; i < outputColumnCount(); ++i)
            _columnsToSerialize.push_back(true);

        _optimizedNormalCaseRowType = _normalCaseRowType;
    }

    std::vector<Row> FileInputOperator::getSample(const size_t num) const {

        if(num > _sample.size()) {
            Logger::instance().defaultLogger().warn("requested " + std::to_string(num)
                                                    + " rows for sampling, but only "
                                                    + std::to_string(_sample.size())
                                                    + " stored. Consider decreasing sample size.");
        }

        // retrieve as many rows as necessary from the first file
        return std::vector<Row>(_sample.begin(), _sample.begin() + std::min(_sample.size(), num));
    }

    void FileInputOperator::selectColumns(const std::vector<size_t> &columnsToSerialize) {
        using namespace std;

        // reset to defaults
        setProjectionDefaults();

        auto schema = getInputSchema();
        auto rowType = schema.getRowType();
        assert(schema.getRowType().isTupleType());
        assert(schema.getRowType().parameters().size() == outputColumnCount());

        // set internal columns to serialize to false
        for (int i = 0; i < _columnsToSerialize.size(); ++i)
            _columnsToSerialize[i] = false;

        vector<string> cols;
        vector<python::Type> colTypes;
        vector<python::Type> colNormalTypes;
        for (auto idx : columnsToSerialize) {
            assert(idx < rowType.parameters().size());
            if (!_columnNames.empty()) {
                assert(idx < _columnNames.size());
                cols.emplace_back(_columnNames[idx]);
            }

            colTypes.emplace_back(rowType.parameters()[idx]);
            colNormalTypes.emplace_back(_normalCaseRowType.parameters()[idx]);

            // set to true for idx
            _columnsToSerialize[idx] = true;
        }

        _optimizedColumnNames = cols;
        _optimizedSchema = Schema(schema.getMemoryLayout(), python::Type::makeTupleType(colTypes));
        _optimizedNormalCaseRowType = python::Type::makeTupleType(colNormalTypes);
    }

    std::unordered_map<int, int> FileInputOperator::projectionMap() const {
        // check if all are positive
        bool all_true = true;
        for(auto keep_col : columnsToSerialize())
            if(!keep_col) {
                all_true = false;
            }

        // trivial case, no map necessary. full row is serialized...
        if(all_true)
            return std::unordered_map<int, int>();

        // mapping case
        std::unordered_map<int, int> m;
        int pos = 0;
        for(int i = 0; i < columnsToSerialize().size(); ++i) {
            if(columnsToSerialize()[i]) {
                m[i] = pos++;
            }
        }
        return m;
    }

    void FileInputOperator::setColumns(const std::vector<std::string> &columnNames) {
        if(_fileURIs.empty())
            return;

        assert(_columnNames.empty());

        if (columnNames.size() != outputColumnCount())
            throw std::runtime_error("number of columns given (" + std::to_string(columnNames.size()) +
                                     ") does not match detected count (" + std::to_string(outputColumnCount()) + ")");

        _columnNames = columnNames;
        _optimizedColumnNames = _columnNames;
    }

    LogicalOperator *FileInputOperator::clone() {
        // make here copy a bit more efficient, i.e. avoid resampling files by using specialized copy constructor (private)
        auto copy = new FileInputOperator(*this); // no children, no parents so all good with this method
        assert(getID() == copy->getID());
        return copy;
    }

    FileInputOperator::FileInputOperator(tuplex::FileInputOperator &other) : _partitions(other._partitions),
                                                                             _fileURIs(other._fileURIs),
                                                                             _sizes(other._sizes),
                                                                             _quotechar(other._quotechar), _delimiter(other._delimiter),
                                                                             _header(other._header),
                                                                             _optimizedSchema(other._optimizedSchema),
                                                                             _columnNames(other._columnNames),
                                                                             _optimizedColumnNames(other._optimizedColumnNames),
                                                                             _columnsToSerialize(other._columnsToSerialize),
                                                                             _null_values(other._null_values),
                                                                             _fmt(other._fmt),
                                                                             _estimatedRowCount(other._estimatedRowCount),
                                                                             _normalCaseRowType(other._normalCaseRowType),
                                                                             _optimizedNormalCaseRowType(other._optimizedNormalCaseRowType),
                                                                             _sample(other._sample),
                                                                             _sampling_time_s(other._sampling_time_s),
                                                                             _indexBasedHints(other._indexBasedHints) {
        // copy members for logical operator
        LogicalOperator::copyMembers(&other);

        LogicalOperator::setDataSet(other.getDataSet());

    }

    int64_t FileInputOperator::cost() const {
        // use number of input rows as cost
        return _estimatedRowCount;
    }

    bool FileInputOperator::isEmpty() const {
        if(_fileURIs.empty() || _sizes.empty())
            return true;

        size_t totalSize = 0;
        for(auto s: _sizes)
            totalSize += s;
        return totalSize == 0;
    }
}