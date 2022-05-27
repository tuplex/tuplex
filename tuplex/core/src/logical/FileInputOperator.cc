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

    aligned_string FileInputOperator::loadSample(size_t sampleSize, const URI& uri, size_t file_size, const SamplingMode& mode) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        if(0 == file_size || uri == URI::INVALID)
            return "";

        auto vfs = VirtualFileSystem::fromURI(uri);
        auto vf = vfs.open_file(uri, VirtualFileMode::VFS_READ);
        if (!vf) {
            logger.error("could not open file " + uri.toString());
            return "";
        }

        // determine sample size
        if (file_size > sampleSize)
            sampleSize = core::floorToMultiple(std::min(sampleSize, file_size), 16ul);
        else {
            sampleSize = core::ceilToMultiple(std::min(sampleSize, file_size), 16ul);
        }

        // depending on sampling mode seek in file!
        switch(mode) {
            case SamplingMode::LAST_ROWS: {
                vf->seek(file_size - sampleSize);
                break;
            }
            case SamplingMode::RANDOM_ROWS: {
                // random seek between 0 and file_size - sampleSize
                auto randomSeekOffset = randi(0ul, file_size - sampleSize);
                vf->seek(randomSeekOffset);
                break;
            }
            default:
                // nothing todo (i.e. first rows mode!)
                break;
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
                                                    const std::vector<std::string> &null_values,
                                                    const SamplingMode& sampling_mode) {
        return new FileInputOperator(pattern, co, null_values, sampling_mode);
    }

    FileInputOperator::FileInputOperator(const std::string& pattern,
            const ContextOptions& co,
            const std::vector<std::string>& null_values,
            const SamplingMode& sampling_mode) : _null_values(null_values), _estimatedRowCount(0), _sampling_time_s(0.0), _samplingMode(sampling_mode) {
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
        // sample using first rows from first file. @TODO: sample across files to provoke better result for general case!
        // auto uri = _fileURIs.front();
        // size_t size = _sizes.front();

        aligned_string sample;
        if(!_fileURIs.empty()) {
            sample = loadSample(co.CSV_MAX_DETECTION_MEMORY(), _fileURIs.front(), _sizes.front(),
                                SamplingMode::FIRST_ROWS);
        }

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
        _firstRowsSample.clear();
        for(auto line : lines) {
            _firstRowsSample.push_back(Row(line));
        }
        if(_header && !_firstRowsSample.empty())
            _firstRowsSample.erase(_firstRowsSample.begin());

        // when no null-values are given, simply set to string always
        // else, it's an option type...
        auto rowType = python::Type::makeTupleType({python::Type::STRING});
        _normalCaseRowType = rowType; // NVO speculation?
        if(!null_values.empty())
            rowType = python::Type::makeTupleType({python::Type::makeOptionType(python::Type::STRING)});
        _generalCaseRowType = rowType;

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
                                                   const std::unordered_map<std::string, python::Type>& column_based_type_hints,
                                                   const SamplingMode& sampling_mode) {
        return new FileInputOperator(pattern, co, hasHeader, delimiter, quotechar, null_values,
                                     column_name_hints, index_based_type_hints, column_based_type_hints, sampling_mode);
    }

    FileInputOperator::FileInputOperator(const std::string &pattern, const ContextOptions &co,
                                         option<bool> hasHeader,
                                         option<char> delimiter,
                                         option<char> quotechar,
                                         const std::vector<std::string> &null_values,
                                         const std::vector<std::string>& column_name_hints,
                                         const std::unordered_map<size_t, python::Type>& index_based_type_hints,
                                         const std::unordered_map<std::string, python::Type>& column_based_type_hints,
                                         const SamplingMode& sampling_mode) :
                                                                            _null_values(null_values), _sampling_time_s(0.0), _samplingMode(sampling_mode) {
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

        auto SAMPLE_SIZE = co.CSV_MAX_DETECTION_MEMORY();

        // infer schema using first file only
        if (!_fileURIs.empty()) {

            aligned_string sample;
            if(!_fileURIs.empty()) {
                sample = loadSample(SAMPLE_SIZE, _fileURIs.front(), _sizes.front(),
                                    SamplingMode::FIRST_ROWS);
            }

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
            _generalCaseRowType = exceptcasetype;
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
            // => use here strlen because _firstRowsSample is zero terminated!
            assert(sample.back() == '\0');
            _firstRowsSample = parseRows(sample.c_str(), sample.c_str() + std::min(sample.size() - 1, strlen(sample.c_str())), _null_values, _delimiter, _quotechar);
            // header? ignore first row!
            if(_header && !_firstRowsSample.empty())
                _firstRowsSample.erase(_firstRowsSample.begin());

            // draw last rows sample from last file & append.
            if(!_fileURIs.empty()) {
                // only draw sample IF > 1 file or file_size > 2 * sample size
                bool draw_sample = _fileURIs.size() >= 2 || _sizes.back() >= 2 * SAMPLE_SIZE;
                if(draw_sample) {
                    auto last_sample = loadSample(SAMPLE_SIZE, _fileURIs.back(), _sizes.back(), SamplingMode::LAST_ROWS);
                    // search CSV beginning
                    auto column_count = inputColumnCount();
                    auto offset = csvFindLineStart(last_sample.c_str(), SAMPLE_SIZE, column_count, csvstat.delimiter(), csvstat.quotechar());
                    if(offset >= 0) {
                        // parse into last rows
                        _lastRowsSample = parseRows(last_sample.c_str(), last_sample.c_str() + std::min(last_sample.size() - 1, strlen(last_sample.c_str())), _null_values, _delimiter, _quotechar);
                    } else {
                        logger.warn("could not find CSV line start in last rows sample.");
                    }
                }
            }

            // @TODO: could draw also additional random sample from data!

        } else {
            logger.warn("no input files found, can't infer type from given path: " + pattern);
            setSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
        }
        _sampling_time_s += timer.time();
    }

    FileInputOperator *FileInputOperator::fromOrc(const std::string &pattern, const ContextOptions &co, const SamplingMode& sampling_mode) {
        return new FileInputOperator(pattern, co, sampling_mode);
    }

    FileInputOperator::FileInputOperator(const std::string &pattern, const ContextOptions &co, const SamplingMode& sampling_mode): _sampling_time_s(0.0), _samplingMode(sampling_mode) {

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
            _generalCaseRowType = tuplexType;
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
//        // set optimized schema to current one
//        _optimizedSchema = getInputSchema();
//        _optimizedColumnNames = _columnNames;
        // create per default true array, i.e. serialize all columns
        _columnsToSerialize = std::vector<bool>();

        for (int i = 0; i < inputColumnCount(); ++i)
            _columnsToSerialize.push_back(true);

//        _optimizedNormalCaseRowType = _normalCaseRowType;
    }

    std::vector<Row> FileInputOperator::getSample(const size_t num) const {
        auto totalSampleCount = _firstRowsSample.size() + _lastRowsSample.size();
        if(num > totalSampleCount) {
#ifndef NEDEBUG
            Logger::instance().defaultLogger().warn("requested " + std::to_string(num)
                                                    + " rows for sampling, but only "
                                                    + std::to_string(totalSampleCount)
                                                    + " stored. Consider decreasing sample size.");
#endif
        }

        // retrieve full sample from first and last rows
        auto sample = _firstRowsSample;
        for(auto row : _lastRowsSample)
            sample.push_back(row);

        // retrieve as many rows as necessary from the combined sample
        return std::vector<Row>(sample.begin(), sample.begin() + std::min(sample.size(), num));
    }

    std::vector<size_t> FileInputOperator::translateOutputToInputIndices(const std::vector<size_t> &output_indices) {
        using namespace std;
        vector<size_t> indices;

        // create map

        unordered_map<size_t, size_t> m; // output index -> original index
        size_t pos = 0;
        for(unsigned i = 0; i < _columnsToSerialize.size(); ++i) {
            if(_columnsToSerialize[i]) {
                m[pos++] = i;
            }
        }

        // make sure this works with current output schema!
        if(m.size() != outputColumnCount()) {
            throw std::runtime_error("INTERNAL ERROR: invalid pushdown map created.");
        }

        // go through output indices
        for(auto idx : output_indices) {
            auto it = m.find(idx);
            if(it == m.end()) {
                // invalid index
                throw std::runtime_error("INTERNAL ERROR: invalid index to select from given. Index is "
                + to_string(idx) + " but allowed range is 0-"+ to_string(pos));
            } else {
                indices.push_back(it->second); // push back original index
            }
        }

        return indices;
    }

    void FileInputOperator::selectColumns(const std::vector<size_t> &columnsToSerialize, bool original_indices) {
        using namespace std;

        // are indices relative to original columns or to currently pushed down columns?
        auto original_col_indices_to_serialize = columnsToSerialize;
        // if so, translate them!
        if(!original_indices)
            original_col_indices_to_serialize = translateOutputToInputIndices(columnsToSerialize);

        // reset to defaults
        setProjectionDefaults();

        auto schema = getInputSchema();
        auto rowType = schema.getRowType();
        assert(schema.getRowType().isTupleType());
        assert(schema.getRowType().parameters().size() == inputColumnCount());

        // set internal (original) column indices to serialize to false
        for (int i = 0; i < _columnsToSerialize.size(); ++i)
            _columnsToSerialize[i] = false;

        for (auto idx : original_col_indices_to_serialize) {
            assert(idx < rowType.parameters().size());

            // set to true for idx
            _columnsToSerialize[idx] = true;
        }
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

        if(_columnNames.empty()) {
            if(columnNames.size() != inputColumnCount())
                throw std::runtime_error("number of columns given (" + std::to_string(columnNames.size()) +
                                         ") does not match detected count (" + std::to_string(inputColumnCount()) + ")");
            _columnNames = columnNames;
            return;
        }

        // check whether it's a match for input Column Count (i.e. full replace) or output column count (projected replace)
        if(columnNames.size() == inputColumnCount()) {
            _columnNames = columnNames;
        } else {
            // must match
            if (columnNames.size() != outputColumnCount())
                throw std::runtime_error("number of columns given (" + std::to_string(columnNames.size()) +
                                         ") does not match projected column count (" + std::to_string(outputColumnCount()) + ")");

            assert(_columnNames.size() == _columnsToSerialize.size());

            // replace columns for kept columns
            unsigned pos = 0;
            for(unsigned i = 0; i < _columnsToSerialize.size(); ++i) {
                if(pos < columnNames.size() && _columnsToSerialize[i]) {
                    _columnNames[i] = columnNames[pos++];
                }
            }
        }
    }

    std::shared_ptr<LogicalOperator> FileInputOperator::clone() {
        // make here copy a bit more efficient, i.e. avoid resampling files by using specialized copy constructor (private)
        auto copy = new FileInputOperator(*this); // no children, no parents so all good with this method
        assert(getID() == copy->getID());
        return std::shared_ptr<LogicalOperator>(copy);
    }

    FileInputOperator::FileInputOperator(tuplex::FileInputOperator &other) : _fileURIs(other._fileURIs),
                                                                             _sizes(other._sizes),
                                                                             _estimatedRowCount(other._estimatedRowCount),
                                                                             _fmt(other._fmt),
                                                                             _quotechar(other._quotechar),
                                                                             _delimiter(other._delimiter),
                                                                             _header(other._header),
                                                                             _null_values(other._null_values),
                                                                             _columnsToSerialize(other._columnsToSerialize),
                                                                             _columnNames(other._columnNames),
                                                                             _normalCaseRowType(other._normalCaseRowType),
                                                                             _generalCaseRowType(other._generalCaseRowType),
                                                                             _indexBasedHints(other._indexBasedHints),
                                                                             _firstRowsSample(other._firstRowsSample),
                                                                             _lastRowsSample(other._lastRowsSample),
                                                                             _samplingMode(other._samplingMode),
                                                                             _sampling_time_s(other._sampling_time_s) {
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

    // HACK !!!!
    void FileInputOperator::setInputFiles(const std::vector<URI> &uris, const std::vector<size_t> &uri_sizes,
                                          bool resample) {
        assert(uris.size() == uri_sizes.size());
        _fileURIs = uris;
        _sizes = uri_sizes;

        if(resample && !_fileURIs.empty()) {
            // only CSV supported...
            if(_fmt != FileFormat::OUTFMT_CSV)
                throw std::runtime_error("only csv supported");

            // NOTE: 256 triggers deoptimization ==> need to study this behavior more carefully...!

            size_t SAMPLE_SIZE = 1024 * 512; // use 256KB each

            // @TODO: rework this...
            aligned_string sample;
            sample = loadSample(SAMPLE_SIZE, _fileURIs.front(), _sizes.front(),
                                SamplingMode::FIRST_ROWS);

            _firstRowsSample = parseRows(sample.c_str(), sample.c_str() + std::min(sample.size() - 1,
                                                                                   strlen(sample.c_str())), _null_values, _delimiter, _quotechar);
            // header? ignore first row!
            if(_header && !_firstRowsSample.empty())
                _firstRowsSample.erase(_firstRowsSample.begin());

            // fetch also last rows sample?
            // only draw sample IF > 1 file or file_size > 2 * sample size
            bool draw_sample = _fileURIs.size() >= 2 || _sizes.back() >= 2 * SAMPLE_SIZE;
            if(draw_sample) {
                auto last_sample = loadSample(SAMPLE_SIZE, _fileURIs.back(), _sizes.back(), SamplingMode::LAST_ROWS);
                // search CSV beginning
                auto column_count = inputColumnCount();
                auto offset = csvFindLineStart(last_sample.c_str(), SAMPLE_SIZE, column_count, _delimiter, _quotechar);
                if(offset >= 0) {
                    // parse into last rows
                    _lastRowsSample = parseRows(last_sample.c_str(), last_sample.c_str() + std::min(last_sample.size() - 1, strlen(last_sample.c_str())), _null_values, _delimiter, _quotechar);
                } else {
                    //logger.warn("could not find CSV line start in last rows sample.");
                }
            }

            // pretty bad but required, else stageplanner/builder will complain
            _estimatedRowCount = _firstRowsSample.size() * ( 1.0 * _sizes.front() / (1.0 * sample.size()));
        }
    }

    bool FileInputOperator::retype(const python::Type& rowType, bool ignore_check_for_str_option) {
        auto col_types = rowType.parameters();
        auto old_col_types = normalCaseSchema().getRowType().parameters();
        auto old_general_col_types = schema().getRowType().parameters();

        assert(old_col_types.size() <= old_general_col_types.size());
        auto& logger = Logger::instance().logger("codegen");

        // check whether number of columns are compatible
        if(col_types.size() != old_col_types.size()) {
            logger.error("Provided incompatible rowtype to retype " + name() +
                         ", provided type has " + pluralize(col_types.size(), "column") + " but optimized schema in operator has "
                         + pluralize(old_col_types.size(), "column"));
            return false;
        }

        auto str_opt_type = python::Type::makeOptionType(python::Type::STRING);

        // go over and check they're compatible!
        for(unsigned i = 0; i < col_types.size(); ++i) {
            auto t = col_types[i];
            if(col_types[i].isConstantValued())
                t = t.underlying();
            if(!python::canUpcastType(t, old_general_col_types[i])) {

                if(!(ignore_check_for_str_option && old_general_col_types[i] == str_opt_type)) {
                    logger.warn("provided specialized type " + col_types[i].desc() + " can't be upcast to "
                                + old_general_col_types[i].desc() + ", ignoring in retype.");
                    col_types[i] = old_col_types[i];
                }
            }
        }

        // type hints have precedence over sampling! I.e., include them here!
        for(const auto& kv : typeHints()) {
            if(kv.first < col_types.size())
                col_types[kv.first] = kv.second;
            else
                logger.error("internal error, invalid type hint (with wrong index?)");
        }

        // retype optimized schema!
        _normalCaseRowType = python::Type::makeTupleType(col_types);

        return true;
    }

    bool FileInputOperator::retype(const std::vector<python::Type>& rowTypes) {
        assert(rowTypes.size() == 1);
        assert(rowTypes.front().isTupleType());
        auto desired_type = rowTypes.front();
        assert(desired_type.isTupleType());
        return retype(desired_type, false);
    }

    std::vector<Row> FileInputOperator::sample(const SamplingMode& mode) {
        auto& logger = Logger::instance().logger("logical");
        if(_fileURIs.empty())
            return {};

        Timer timer;
        std::vector<Row> v;

        // check sampling mode, i.e. how files should be sample?
        std::set<unsigned> sampled_file_indices;
        if(mode & SamplingMode::ALL_FILES) {
            assert(_fileURIs.size() == _sizes.size());
            for(unsigned i = 0; i < _fileURIs.size(); ++i) {
                auto s = sampleFile(_fileURIs[i], _sizes[i], mode);
                std::copy(s.begin(), s.end(), std::back_inserter(v));
                sampled_file_indices.insert(i);
            }
        }

        // sample the first file present?
        if(mode & SamplingMode::FIRST_FILE && sampled_file_indices.find(0) == sampled_file_indices.end()) {
            auto s = sampleFile(_fileURIs.front(), _sizes.front(), mode);
            std::copy(s.begin(), s.end(), std::back_inserter(v));
            sampled_file_indices.insert(0);
        }

        // sample the last file present?
        if(mode & SamplingMode::LAST_FILE && sampled_file_indices.find(_fileURIs.size() - 1) == sampled_file_indices.end()) {
            auto s = sampleFile(_fileURIs.back(), _sizes.back(), mode);
            std::copy(s.begin(), s.end(), std::back_inserter(v));
            sampled_file_indices.insert(_fileURIs.size() - 1);
        }

        // sample a random file?
        if(mode & SamplingMode::RANDOM_FILE && sampled_file_indices.size() < _fileURIs.size()) {
            // form set of valid indices
            std::vector<unsigned> valid_indices;
            for(unsigned i = 0; i < _fileURIs.size(); ++i)
                if(sampled_file_indices.find(i) == sampled_file_indices.end())
                    valid_indices.push_back(i);
            assert(!valid_indices.empty());
            auto random_idx = valid_indices[randi(0ul, valid_indices.size() - 1)];
            logger.debug("Sampling file (random) idx=" + std::to_string(random_idx));
            auto s = sampleFile(_fileURIs[random_idx], _sizes[random_idx], mode);
            std::copy(s.begin(), s.end(), std::back_inserter(v));
            sampled_file_indices.insert(random_idx);
        }

        _sampling_time_s = timer.time();
        logger.debug("Sampling (mode=" + std::to_string(mode) + ") took " + std::to_string(_sampling_time_s) + "s");
        return v;
    }

    std::vector<Row> FileInputOperator::sampleCSVFile(const URI& uri, size_t uri_size, const SamplingMode& mode) {
        throw std::runtime_error("not yet implemented");
        return {};
    }

    std::vector<Row> FileInputOperator::sampleTextFile(const URI& uri, size_t uri_size, const SamplingMode& mode) {
        throw std::runtime_error("not yet implemented");
        return {};
    }

    std::vector<Row> FileInputOperator::sampleORCFile(const URI& uri, size_t uri_size, const SamplingMode& mode) {
        throw std::runtime_error("not yet implemented");
        return {};
    }

}