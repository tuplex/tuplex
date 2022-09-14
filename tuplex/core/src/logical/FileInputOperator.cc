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

//    std::vector<tuplex::Row> parseRows(const char *start, const char *end, const std::vector<std::string>& null_values, char delimiter, char quotechar) {
//        using namespace std;
//        using namespace tuplex;
//        vector<Row> rows;
//
//        ExceptionCode ec;
//        vector<string> cells;
//        size_t num_bytes = 0;
//        const char* p = start;
//        while(p < end && (ec = parseRow(p, end, cells, num_bytes, delimiter, quotechar, false)) == ExceptionCode::SUCCESS) {
//            // convert cells to row
//            auto row = cellsToRow(cells, null_values);
//
//#ifndef NDEBUG
//            // validate
//            python::lockGIL();
//            auto py_row = python::rowToPython(row);
//            python::unlockGIL();
//#endif
//
//            rows.push_back(row);
//            cells.clear();
//            p += num_bytes;
//        }
//
//        return rows;
//    }




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

    aligned_string FileInputOperator::loadSample(size_t sampleSize, const URI& uri, size_t file_size, const SamplingMode& mode, bool use_cache, size_t* file_offset) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        if(0 == file_size || uri == URI::INVALID)
            return "";

        auto key = std::make_tuple(uri, perFileMode(mode));
        if(use_cache) {
            // check if in sample cache already
            auto it = _sampleCache.find(key);
            if(it != _sampleCache.end())
                return it->second;
        }

        auto vfs = VirtualFileSystem::fromURI(uri);
        auto vf = vfs.open_file(uri, VirtualFileMode::VFS_READ);
        if (!vf) {
            logger.error("could not open file " + uri.toString());
            return "";
        }

        if(file_offset)
            *file_offset = 0;

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
                if(file_offset)
                    *file_offset = file_size - sampleSize;
                break;
            }
            case SamplingMode::RANDOM_ROWS: {
                // random seek between 0 and file_size - sampleSize
                auto randomSeekOffset = randi(0ul, file_size - sampleSize);
                vf->seek(randomSeekOffset);
                if(file_offset)
                    *file_offset = randomSeekOffset;
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

        if(use_cache) {
            // put into cache
            _sampleCache[key] = sample;
        }

        return sample;
    }

    FileInputOperator *FileInputOperator::fromText(const std::string &pattern, const ContextOptions &co,
                                                    const std::vector<std::string> &null_values,
                                                    const SamplingMode& sampling_mode) {
        return new FileInputOperator(pattern, co, null_values, sampling_mode);
    }


    size_t FileInputOperator::estimateTextFileRowCount(size_t sample_size, const SamplingMode& mode) {
        if(_fileURIs.empty())
            return 0;

        // take a single sample and then scale over all available text files...
        auto sample = loadSample(sample_size, _fileURIs.front(), _sizes.front(), SamplingMode::FIRST_ROWS);

        // estimate row count
        // split into lines, compute average length & scale row estimate up
        auto lines = splitToLines(sample.c_str());
        size_t accLineLength = 0;
        for(auto line : lines)
            accLineLength += line.length();
        double avgLineLength = lines.empty() ? 1.0 : accLineLength / (double)lines.size();
        double estimatedRowCount = 0.0;
        for(auto s : _sizes)
            estimatedRowCount += (double)s / (double)avgLineLength;
        return static_cast<size_t>(std::ceil(estimatedRowCount));
    }

    FileInputOperator::FileInputOperator(const std::string& pattern,
            const ContextOptions& co,
            const std::vector<std::string>& null_values,
            const SamplingMode& sampling_mode) : _null_values(null_values), _estimatedRowCount(0), _sampling_time_s(0.0), _samplingMode(sampling_mode), _samplingSize(co.SAMPLE_SIZE()), _cachePopulated(false) {

        auto &logger = Logger::instance().logger("fileinputoperator");
        _fmt = FileFormat::OUTFMT_TEXT;
        _quotechar = '\0';
        _delimiter = '\0';
        _header = false;
        _columnNames.reserve(1);

        Timer timer;
        detectFiles(pattern);

        // fill sample cache
        if(!_fileURIs.empty()) {
            // fill sampling cache
            fillFileCache(sampling_mode);

            // run estimation
            _estimatedRowCount = estimateTextFileRowCount(co.CSV_MAX_DETECTION_MEMORY(), sampling_mode);
        } else {
            _estimatedRowCount = 0;
        }

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
        logger.info("Sampling took " + std::to_string(_sampling_time_s));
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

    void FileInputOperator::fillRowCache(SamplingMode mode) {
        auto &logger = Logger::instance().logger("fileinputoperator");

        // based on samples stored, fill the row cache
        if(_sampleCache.empty())
            return;

        Timer timer;

        if(mode & SamplingMode::SINGLETHREADED)
            _rowsSample = sample(mode);
        else
            _rowsSample = multithreadedSample(mode);

        logger.info("Extracting row sample took " + std::to_string(timer.time()) + "s");
    }

    void FileInputOperator::fillFileCache(SamplingMode mode) {
        auto &logger = Logger::instance().logger("fileinputoperator");
        Timer timer;

        // cache already populated?
        // drop!
        if(_cachePopulated) {
            _sampleCache.clear();
            _cachePopulated = false;
        }

        // if neither single-threaded nor multi-threaded are specified, use multi-threaded mode per default.
        if(!(mode & SamplingMode::SINGLETHREADED) && !(mode & SamplingMode::MULTITHREADED)) {
            mode = mode | SamplingMode::MULTITHREADED;
        }

        if(mode & SamplingMode::SINGLETHREADED) {
            // simply fill cache by drawing the proper samples & store rows.
            fillFileCacheSinglethreaded(_samplingMode);
        } else if(mode & SamplingMode::MULTITHREADED) {
            fillFileCacheMultithreaded(_samplingMode);
        } else {
            logger.debug("INTERNAL ERROR, invalid sampling mode.");
        }

        _cachePopulated = true;
        auto duration = timer.time();
        logger.info("Filling sample cache for " + name() + " operator took " + std::to_string(duration) + "s (" + std::to_string(_sampleCache.size()) + " entries, " + pluralize(_rowsSample.size(), "row") + ")");
    }

    void FileInputOperator::fillFileCacheSinglethreaded(SamplingMode mode) {
        using namespace std;
        auto &logger = Logger::instance().logger("fileinputoperator");

        // first, fill the internal cache
        assert(_sampleCache.empty());

        // this works by loading/requesting files in parallel
        std::set<int> file_indices;
        if(mode & SamplingMode::FIRST_FILE && _fileURIs.size() >= 1)
            file_indices.insert(0);
        if(mode & SamplingMode::LAST_FILE && _fileURIs.size() >= 1)
            file_indices.insert(_fileURIs.size() - 1);
        if(mode & SamplingMode::RANDOM_FILE && _fileURIs.size() >= 1)
            file_indices.insert(randi(0ul, _fileURIs.size() - 1));

        if(mode & SamplingMode::ALL_FILES) {
            for(unsigned i = 0; i < _fileURIs.size(); ++i)
                file_indices.insert(i);
        }
        // empty?
        if(file_indices.empty())
            return;

        // now load all the files in parallel into vector!
        vector<unsigned> indices(file_indices.begin(), file_indices.end());
        std::sort(indices.begin(), indices.end());
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto idx = indices[i];
            auto uri = _fileURIs[idx];
            auto size = _sizes[idx];
            auto file_mode = perFileMode(mode);
            auto key = std::make_tuple(uri, file_mode);
            // place into cache
            _sampleCache[key] = loadSample(_samplingSize, uri, size, mode, true);
        }

        logger.info("Sample fetch done.");
    }

    void FileInputOperator::fillFileCacheMultithreaded(SamplingMode mode) {
        using namespace std;
        auto &logger = Logger::instance().logger("fileinputoperator");

        // first, fill the internal cache
        assert(_sampleCache.empty());

        // this works by loading/requesting files in parallel
        std::set<int> file_indices;
        if(mode & SamplingMode::FIRST_FILE && _fileURIs.size() >= 1)
            file_indices.insert(0);
        if(mode & SamplingMode::LAST_FILE && _fileURIs.size() >= 1)
            file_indices.insert(_fileURIs.size() - 1);
        if(mode & SamplingMode::RANDOM_FILE && _fileURIs.size() >= 1)
            file_indices.insert(randi(0ul, _fileURIs.size() - 1));

        if(mode & SamplingMode::ALL_FILES) {
            for(unsigned i = 0; i < _fileURIs.size(); ++i)
                file_indices.insert(i);
        }
        // empty?
        if(file_indices.empty())
            return;

        // now load all the files in parallel into vector!
        vector<unsigned> indices(file_indices.begin(), file_indices.end());
        std::sort(indices.begin(), indices.end());

        vector<std::future<aligned_string>> vf(file_indices.size());
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto idx = indices[i];
            vf[i] = std::async(std::launch::async, [this](const URI& uri, size_t size, const SamplingMode& mode) {
                return loadSample(_samplingSize, uri, size, mode, false);
            }, _fileURIs[idx], _sizes[idx], mode);
        }

        // check till all futures are done
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto idx = indices[i];
            auto uri = _fileURIs[idx];
            auto file_mode = perFileMode(mode);
            auto key = std::make_tuple(uri, file_mode);
            // place into cache
            _sampleCache[key] = vf[i].get();
        }

        logger.info("Parallel sample fetch done.");
    }

    std::vector<Row> FileInputOperator::multithreadedSample(const SamplingMode &mode) {
        using namespace std;
        auto &logger = Logger::instance().logger("fileinputoperator");

        // construct indices from cache
        if(!_cachePopulated)
            fillFileCache(mode);
        set<unsigned> file_indices;
        for(const auto& keyval : _sampleCache) {
            auto uri = std::get<0>(keyval.first);
            auto idx = indexInVector(uri, _fileURIs);
            if(idx < 0)
                throw std::runtime_error("fatal internal error, could not find URI " + uri.toString() + " in file cache.");
            file_indices.insert(static_cast<unsigned>(idx));
        }
        vector<unsigned> indices(file_indices.begin(), file_indices.end());

        // parse now rows for detection etc.
        vector<std::future<vector<Row>>> f_rows(indices.size());
        // inline std::vector<Row> sampleFile(const URI& uri, size_t uri_size, const SamplingMode& mode)
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto idx = indices[i];
            auto uri = _fileURIs[idx];
            auto size = _sizes[idx];
            auto file_mode = perFileMode(mode);

            f_rows[i] = std::async(std::launch::async, [this, file_mode](const URI& uri, size_t size) {
                return sampleFile(uri, size, file_mode);
            }, _fileURIs[idx], _sizes[idx]);
        }

        // combine rows now.
        vector<Row> res;
        for(unsigned i = 0; i < indices.size(); ++i) {
            auto rows = f_rows[i].get();
            std::copy(rows.begin(), rows.end(), std::back_inserter(res));
        }

        logger.info("Parallel sample parse done.");
        return res;
    }

    FileInputOperator::FileInputOperator(const std::string &pattern,
                                         const ContextOptions &co,
                                         option<bool> hasHeader,
                                         option<char> delimiter,
                                         option<char> quotechar,
                                         const std::vector<std::string> &null_values,
                                         const std::vector<std::string>& column_name_hints,
                                         const std::unordered_map<size_t, python::Type>& index_based_type_hints,
                                         const std::unordered_map<std::string, python::Type>& column_based_type_hints,
                                         const SamplingMode& sampling_mode) :
                                                                            _null_values(null_values), _sampling_time_s(0.0), _samplingMode(sampling_mode), _samplingSize(co.SAMPLE_SIZE()), _cachePopulated(false), _estimatedRowCount(0) {
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

            // fill sampling cache
            fillFileCache(sampling_mode);

            // need to load FIRST rows in order to perform header/csv detection.
            aligned_string sample;
            if(!_fileURIs.empty()) {
                sample = loadSample(co.SAMPLE_SIZE(), _fileURIs.front(), _sizes.front(),
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

            // print info on csv header estimation
            if(!hasHeader.has_value()) {
                if(_header)
                    logger.info("Auto-detected presence of a header line in CSV files.");
                else
                    logger.info("Auto-detected there's no header line in CSV files.");
            }

            // set column names from stat
            // Note: call this BEFORE applying type hints!
            _columnNames = _header ? csvstat.columns() : std::vector<std::string>();
            
            // now fill row cache to detect majority type (?)
            fillRowCache(sampling_mode);

            // use sample to detect types
            bool use_independent_columns = true;
            // when NVO is deactivated, normalcase and general case detected here are the same.
            auto normalcasetype = detectMajorityRowType(_rowsSample, co.NORMALCASE_THRESHOLD(), use_independent_columns, co.OPT_NULLVALUE_OPTIMIZATION());

            // general-case type is option[T] version of types encountered in normalcasetype.
            // for null, assume option[str].
            // -> this is required for hyperspecialization upcast + a meaningful assumption for the sample
            auto normalcase_coltypes = normalcasetype.parameters();
            std::vector<python::Type> generalcase_coltypes = normalcase_coltypes;
            for(auto& type : generalcase_coltypes) {
                if(!type.isOptionType()) {
                    if(python::Type::NULLVALUE == type)
                        type = python::Type::makeOptionType(python::Type::STRING);
                    else
                        type = python::Type::makeOptionType(type);
                }
            }
            auto generalcasetype = python::Type::makeTupleType(generalcase_coltypes);
            // NULL Value optimization on or off?
            // Note: if off, can improve performance of estimate...

            // @TODO: change this to be the majority detected row type!
            // normalcase and exception case types
            
            if(normalcasetype == python::Type::UNKNOWN || generalcasetype == python::Type::UNKNOWN) {
                logger.warn("sample-based detection could not infer type, using csvstat type directly.");
                normalcasetype = csvstat.type();
                generalcasetype = csvstat.superType();
            }

            // type hints?
            // ==> enforce certain type for specific columns!
            if(!index_based_type_hints.empty()) {
                assert(generalcasetype.isTupleType());
                auto paramsNormal = normalcasetype.parameters();
                auto paramsExcept = generalcasetype.parameters();
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
                generalcasetype = python::Type::makeTupleType(paramsExcept); // update row type...
            }

            // apply column based type hints
            if(!column_based_type_hints.empty()) {
                assert(generalcasetype.isTupleType());
                auto paramsNormal = normalcasetype.parameters();
                auto paramsExcept = generalcasetype.parameters();
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
                generalcasetype = python::Type::makeTupleType(paramsExcept); // update row type...
            }

            // get type & assign schema
            _normalCaseRowType = normalcasetype;
            _generalCaseRowType = generalcasetype;
            setSchema(Schema(Schema::MemoryLayout::ROW, generalcasetype));

            // if csv stat produced unknown, issue warning and use empty
            if (csvstat.type() == python::Type::UNKNOWN) {
                setSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
                Logger::instance().defaultLogger().warn("csv stats could not determine type, use () for type.");
            }

            // set defaults for possible projection pushdown...
            setProjectionDefaults();

//            // now parse from the allocated buffer all rows and store as internal sample.
//            // there should be at least one 3 rows!
//            // store as internal sample
//            // => use here strlen because _firstRowsSample is zero terminated!
//            assert(sample.back() == '\0');
//            _firstRowsSample = parseRows(sample.c_str(), sample.c_str() + std::min(sample.size() - 1, strlen(sample.c_str())), _null_values, _delimiter, _quotechar);
//            // header? ignore first row!
//            if(_header && !_firstRowsSample.empty())
//                _firstRowsSample.erase(_firstRowsSample.begin());
//
//            // draw last rows sample from last file & append.
//            if(!_fileURIs.empty()) {
//                // only draw sample IF > 1 file or file_size > 2 * sample size
//                bool draw_sample = _fileURIs.size() >= 2 || _sizes.back() >= 2 * _samplingSize;
//                if(draw_sample) {
//                    auto last_sample = loadSample(_samplingSize, _fileURIs.back(), _sizes.back(), SamplingMode::LAST_ROWS);
//                    // search CSV beginning
//                    auto column_count = inputColumnCount();
//                    auto offset = csvFindLineStart(last_sample.c_str(), _samplingSize, column_count, csvstat.delimiter(), csvstat.quotechar());
//                    if(offset >= 0) {
//                        // parse into last rows
//                        _lastRowsSample = parseRows(last_sample.c_str(), last_sample.c_str() + std::min(last_sample.size() - 1, strlen(last_sample.c_str())), _null_values, _delimiter, _quotechar);
//                    } else {
//                        logger.warn("could not find CSV line start in last rows sample.");
//                    }
//                }
//            }
//
//            // @TODO: could draw also additional random sample from data!

        } else {
            logger.warn("no input files found, can't infer type from given path: " + pattern);
            setSchema(Schema(Schema::MemoryLayout::ROW, python::Type::EMPTYTUPLE));
        }
        _sampling_time_s += timer.time();
    }

    FileInputOperator *FileInputOperator::fromOrc(const std::string &pattern, const ContextOptions &co, const SamplingMode& sampling_mode) {
        return new FileInputOperator(pattern, co, sampling_mode);
    }

    FileInputOperator::FileInputOperator(const std::string &pattern, const ContextOptions &co, const SamplingMode& sampling_mode): _sampling_time_s(0.0), _samplingMode(sampling_mode), _samplingSize(co.SAMPLE_SIZE()), _cachePopulated(false) {

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
        if(!_cachePopulated)
            const_cast<FileInputOperator*>(this)->fillFileCache(_samplingMode);
        if(_rowsSample.empty())
            const_cast<FileInputOperator*>(this)->fillRowCache(_samplingMode);

        if(!_cachePopulated || _rowsSample.empty())
            throw std::runtime_error("need to populate cache first");

        // restrict to num
        if(num <= _rowsSample.size()) {
            // select num random rows...
            // use https://en.wikipedia.org/wiki/Reservoir_sampling the optimal algorithm there to fetch the sample quickly...
            // i.e., https://www.geeksforgeeks.org/reservoir-sampling/
            return randomSampleFromReservoir(_rowsSample, num);
        } else {
            // need to increase sample size!
            Logger::instance().defaultLogger().warn("requested " + std::to_string(num)
                                                    + " rows for sampling, but only "
                                                    + std::to_string(_rowsSample.size())
                                                    + " stored. Consider decreasing sample size. Returning all available rows.");
            return _rowsSample;
        }
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

    std::shared_ptr<LogicalOperator> FileInputOperator::clone(bool cloneParents) {
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
                                                                             _rowsSample(other._rowsSample),
                                                                             _cachePopulated(other._cachePopulated),
                                                                             _samplingMode(other._samplingMode),
                                                                             _sampling_time_s(other._sampling_time_s),
                                                                             _samplingSize(other._samplingSize) {
        // copy members for logical operator
        LogicalOperator::copyMembers(&other);
        LogicalOperator::setDataSet(other.getDataSet());

        // copy cache (?)
        for (const auto& kv: other._sampleCache) { _sampleCache[kv.first] = kv.second; }
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
            // clear sample cache
            _cachePopulated = false;
            _sampleCache.clear();
            _rowsSample.clear(); // reset row samples!
            fillFileCache(_samplingMode);

            _estimatedRowCount = 100000; // @TODO.

//            // only CSV supported...
//            if(_fmt != FileFormat::OUTFMT_CSV)
//                throw std::runtime_error("only csv supported");
//
//            // NOTE: 256 triggers deoptimization ==> need to study this behavior more carefully...!
//
//            size_t SAMPLE_SIZE = 1024 * 512; // use 256KB each
//
//            // @TODO: rework this...
//            aligned_string sample;
//            sample = loadSample(SAMPLE_SIZE, _fileURIs.front(), _sizes.front(),
//                                SamplingMode::FIRST_ROWS);
//
//            _firstRowsSample = parseRows(sample.c_str(), sample.c_str() + std::min(sample.size() - 1,
//                                                                                   strlen(sample.c_str())), _null_values, _delimiter, _quotechar);
//            // header? ignore first row!
//            if(_header && !_firstRowsSample.empty())
//                _firstRowsSample.erase(_firstRowsSample.begin());
//
//            // fetch also last rows sample?
//            // only draw sample IF > 1 file or file_size > 2 * sample size
//            bool draw_sample = _fileURIs.size() >= 2 || _sizes.back() >= 2 * SAMPLE_SIZE;
//            if(draw_sample) {
//                auto last_sample = loadSample(SAMPLE_SIZE, _fileURIs.back(), _sizes.back(), SamplingMode::LAST_ROWS);
//                // search CSV beginning
//                auto column_count = inputColumnCount();
//                auto offset = csvFindLineStart(last_sample.c_str(), SAMPLE_SIZE, column_count, _delimiter, _quotechar);
//                if(offset >= 0) {
//                    // parse into last rows
//                    _lastRowsSample = parseRows(last_sample.c_str(), last_sample.c_str() + std::min(last_sample.size() - 1, strlen(last_sample.c_str())), _null_values, _delimiter, _quotechar);
//                } else {
//                    //logger.warn("could not find CSV line start in last rows sample.");
//                }
//            }
//
//            // pretty bad but required, else stageplanner/builder will complain
//            _estimatedRowCount = _firstRowsSample.size() * ( 1.0 * _sizes.front() / (1.0 * sample.size()));
        }
    }

    bool FileInputOperator::retype(const python::Type& input_row_type, bool is_projected_row_type, bool ignore_check_for_str_option) {
        assert(input_row_type.isTupleType());
        auto col_types = input_row_type.parameters();
        auto old_col_types = normalCaseSchema().getRowType().parameters();
        auto old_general_col_types = schema().getRowType().parameters();

        size_t num_projected_columns = getOptimizedOutputSchema().getRowType().parameters().size();

        assert(old_col_types.size() <= old_general_col_types.size());
        auto& logger = Logger::instance().logger("codegen");

        // check whether number of columns are compatible
        if(is_projected_row_type) {
            if(col_types.size() != num_projected_columns) {
                logger.error("Provided incompatible (projected) rowtype to retype " + name() +
                             ", provided type has " + pluralize(col_types.size(), "column") + " but optimized schema in operator has "
                             + pluralize(old_col_types.size(), "column"));
                return false;
            }
        } else {
            if(col_types.size() != old_col_types.size()) {
                logger.error("Provided incompatible rowtype to retype " + name() +
                             ", provided type has " + pluralize(col_types.size(), "column") + " but optimized schema in operator has "
                             + pluralize(old_col_types.size(), "column"));
                return false;
            }
        }


        auto str_opt_type = python::Type::makeOptionType(python::Type::STRING);

        // go over and check they're compatible!
        for(unsigned i = 0; i < col_types.size(); ++i) {
            auto t = col_types[i];
            if(col_types[i].isConstantValued())
                t = t.underlying();

            // old type
            unsigned lookup_index = i;

            if(is_projected_row_type) {
                lookup_index = reverseProjectToReadIndex(i);
            }

            if(!python::canUpcastType(t, old_general_col_types[lookup_index])) {

                if(!(ignore_check_for_str_option && old_general_col_types[lookup_index] == str_opt_type)) {
                    logger.warn("provided specialized type " + col_types[i].desc() + " can't be upcast to "
                                + old_general_col_types[lookup_index].desc() + ", ignoring in retype.");
                    col_types[i] = old_col_types[lookup_index];
                }
            }
        }

        // type hints have precedence over sampling! I.e., include them here!
        for(const auto& kv : typeHints()) {
            auto idx = kv.first;
            // are we having a projected type? then lookup in projection map!
            auto m = projectionMap();
            assert(!m.empty());
            if(is_projected_row_type)
                idx = m[idx]; // get projected index...

            if(idx < col_types.size()) {
                col_types[idx] = kv.second;
            } else
                logger.error("internal error, invalid type hint (with wrong index?)");
        }

        // retype optimized schema!
        // -> this requires reprojection if possible!
        if(is_projected_row_type) {
            // set normal-case type with updated projected fields
            auto nc_col_types = _normalCaseRowType.parameters();
            auto m = projectionMap();
            for(auto kv : m) {
                assert(kv.second < col_types.size());
                nc_col_types[kv.first] = col_types[kv.second];
            }
            _normalCaseRowType = python::Type::makeTupleType(nc_col_types);
        } else {
            _normalCaseRowType = python::Type::makeTupleType(col_types);
        }
        return true;
    }

    bool FileInputOperator::retype(const python::Type& input_row_type, bool is_projected_row_type) {
        assert(input_row_type.isTupleType());
        return retype(input_row_type, is_projected_row_type, false);
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

        logger.debug("Sampling (mode=" + std::to_string(mode) + ") took " + std::to_string(timer.time()) + "s");
        return v;
    }

    std::vector<Row> FileInputOperator::sampleCSVFile(const URI& uri, size_t uri_size, const SamplingMode& mode) {
        auto& logger = Logger::instance().logger("logical");
        std::vector<Row> v;
        assert(mode & SamplingMode::FIRST_ROWS || mode & SamplingMode::LAST_ROWS || mode & SamplingMode::RANDOM_ROWS);

        if(0 == uri_size || uri == URI::INVALID) {
            logger.debug("empty file, can't obtain sample from it");
            return {};
        }

        SamplingMode m = mode;

        // if uri_size < file_size -> first rows only
        if(uri_size <= _samplingSize)
            m = SamplingMode::FIRST_ROWS;

        // check file sampling modes & then load the samples accordingly
        if(m & SamplingMode::FIRST_ROWS) {
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::FIRST_ROWS, true);
            // parse as rows using the settings detected.
            v = parseRows(sample.c_str(), sample.c_str() + std::min(sample.size() - 1, strlen(sample.c_str())),
                          _null_values, _delimiter, _quotechar);

            // header? ignore first row!
            if(_header && !v.empty())
                v.erase(v.begin());
        }

        if(m & SamplingMode::LAST_ROWS) {
            // the smaller of remaining and sample size!
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::LAST_ROWS, true, &file_offset);
            size_t offset = 0;
            if(!v.empty()) {
                if(uri_size < 2 * _samplingSize) {
                    offset = _samplingSize - (uri_size - _samplingSize);
                    assert(offset <= _samplingSize);
                }
                auto rows = parseRows(sample.c_str() + offset, sample.c_str() + std::min(sample.size() - 1, strlen(sample.c_str())),
                                            _null_values, _delimiter, _quotechar);
                // offset = 0?
                if(file_offset == 0) {
                    // header? ignore first row!
                    if(_header && !rows.empty())
                        rows.erase(rows.begin());
                }

                std::copy(rows.begin(), rows.end(), std::back_inserter(v));

            } else {
                v = parseRows(sample.c_str(), sample.c_str() + std::min(sample.size() - 1, strlen(sample.c_str())),
                                _null_values, _delimiter, _quotechar);

                // offset = 0?
                if(file_offset == 0) {
                    // header? ignore first row!
                    if(_header && !v.empty())
                        v.erase(v.begin());
                }
            }
        }

        // most complicated: random -> make sure no overlap with first/last rows
        if(m & SamplingMode::RANDOM_ROWS) {
            // @TODO: there could be overlap with first/last rows.
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::RANDOM_ROWS, true, &file_offset);
            // parse as rows using the settings detected.
            auto rows = parseRows(sample.c_str(), sample.c_str() + std::min(sample.size() - 1, strlen(sample.c_str())),
                          _null_values, _delimiter, _quotechar);

            // offset = 0?
            if(file_offset == 0) {
                // header? ignore first row!
                if(_header && !rows.empty())
                    rows.erase(rows.begin());
            }

            std::copy(rows.begin(), rows.end(), std::back_inserter(v));
        }

        return v;
    }

    static Row lineToRow(const aligned_string& line, const std::set<std::string>& null_values, bool includes_newline) {
        // process line
        auto line_without_newline = (includes_newline && line.back() == '\n') ? line.substr(0, line.size() -1) : line;
        assert(line_without_newline.back() != '\n' || line_without_newline.size() < line.size());
        auto it = null_values.find(line_without_newline.c_str());
        if(it != null_values.end())
            return Row(Field::null());
        else
            return Row(line.c_str());
    }

    static std::vector<Row> parseTextRows(const aligned_string& sample, const std::set<std::string>& null_values) {
        std::vector<Row> v;
        // parse lines (ignore \r\n line endings?)
        unsigned markbegin = 0;
        unsigned markend = 0;
        for (int i = 0; i < sample.length(); ++i) {
            if(sample[i] == '\0')
                return v;

            // Windows End of Line (EOL) characters – Carriage Return (CR) & Line Feed (LF)
            bool weol = (i + 1 < sample.length() && sample[i] == '\r' && sample[i+1] == '\n');
            if(sample[i] == '\n' || weol) {
                markend = i;
                auto line = sample.substr(markbegin, markend - markbegin);
                if(weol)
                    line[line.size() - 1] = '\n'; // convert line feed.

                v.push_back(lineToRow(line, null_values, true));
                markbegin = (i + 1);
            }
        }

        // end of line?
        if(markbegin != markend) {
            auto line = sample.substr(markbegin, markend - markbegin);
            v.push_back(lineToRow(line, null_values, true));
        }
        return v;
    }

    std::vector<Row> FileInputOperator::sampleTextFile(const URI& uri, size_t uri_size, const SamplingMode& mode) {
        auto& logger = Logger::instance().logger("logical");
        std::vector<Row> v;
        assert(mode & SamplingMode::FIRST_ROWS || mode & SamplingMode::LAST_ROWS || mode & SamplingMode::RANDOM_ROWS);

        if(0 == uri_size || uri == URI::INVALID) {
            logger.debug("empty file, can't obtain sample from it");
            return {};
        }

        SamplingMode m = mode;

        std::set<std::string> null_values(_null_values.begin(), _null_values.end());

        // if uri_size < file_size -> first rows only
        if(uri_size <= _samplingSize)
            m = SamplingMode::FIRST_ROWS;

        // check file sampling modes & then load the samples accordingly
        if(m & SamplingMode::FIRST_ROWS) {
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::FIRST_ROWS, true);
            v = parseTextRows(sample, null_values);
        }

        if(m & SamplingMode::LAST_ROWS) {
            // the smaller of remaining and sample size!
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::LAST_ROWS, true, &file_offset);
            size_t offset = 0;
            if(!v.empty()) {
                if(uri_size < 2 * _samplingSize) {
                    offset = _samplingSize - (uri_size - _samplingSize);
                    assert(offset <= _samplingSize);
                }
                auto rows = parseTextRows(sample, null_values);
                // offset != 0? =? remove first row b.c. it may be partial row
                if(file_offset != 0) {
                    // header? ignore first row!
                    if(!rows.empty())
                        rows.erase(rows.begin());
                }

                std::copy(rows.begin(), rows.end(), std::back_inserter(v));

            } else {
                v = parseTextRows(sample, null_values);

                // offset = 0?
                if(file_offset != 0) {
                    // ignore first row b.c. may be partial row...
                    if(!v.empty())
                        v.erase(v.begin());
                }
            }
        }

        // most complicated: random -> make sure no overlap with first/last rows
        if(m & SamplingMode::RANDOM_ROWS) {
            // @TODO: there could be overlap with first/last rows.
            size_t file_offset = 0;
            auto sample = loadSample(_samplingSize, uri, uri_size, SamplingMode::RANDOM_ROWS, true, &file_offset);
            // parse as rows using the settings detected.
            auto rows = parseTextRows(sample, null_values);

            // offset = 0?
            if(file_offset != 0) {
                // ignore first row b.c. partial
                if(!rows.empty())
                    rows.erase(rows.begin());
            }

            // erase last row, b.c. partial
            if(!rows.empty())
                rows.erase(rows.end() - 1);

            std::copy(rows.begin(), rows.end(), std::back_inserter(v));
        }

        return v;
    }

    std::vector<Row> FileInputOperator::sampleORCFile(const URI& uri, size_t uri_size, const SamplingMode& mode) {
        throw std::runtime_error("not yet implemented");
        return {};
    }
}