//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <CSVStatistic.h>
#include <Logger.h>
#include <Timer.h>
#include <StatUtils.h>
#include <StringUtils.h>
#include <limits>

namespace tuplex {
    python::Type CSVStatistic::mapCSVTypeToPythonType(const CSVType &csvtype) {
        switch(csvtype) {
            case CSVType::POSITIVEFLOAT:
            case CSVType::FLOAT:
                return python::Type::F64;
            case CSVType::POSITIVEINTEGER:
            case CSVType::INTEGER:
                return python::Type::I64;
            case CSVType::STRING:
            case CSVType::JSONSTRING:
                return python::Type::STRING;
            case CSVType::BOOLEAN:
                return python::Type::BOOLEAN;
            case CSVType::NULLVALUE:
                return python::Type::NULLVALUE;
            default:
                throw std::runtime_error("unknown csv type");
        }
    }

    void CSVStatistic::estimate(const char *start,
                                const size_t size,
                                tuplex::option<bool> hasUTF8,
                                tuplex::option<char> separator,
                                bool disableNullColumns) {
        using namespace std;
        auto& logger = Logger::instance().logger("csvstats");
        Timer timer;
        auto end = start + size;

        std::stringstream ss;

        // CSV Statistics revised
        // step 1: does file contain UTF8 chars?
        // 1 fullscan necessary
        if(!hasUTF8.has_value())
            hasUTF8 = containsUTF8(start, size);
        ss<<"utf8 detection took: "<<timer.time()<<"s "<<endl;
        timer.reset();
        //logger.info("CSV contains UTF8 chars? " +to_string(hasUTF8.value()));

        // step 2: finding quotechar
        // --> in combination with separator candidates
        // i.e. search for separator|quotechar and quotechar|separator occurences
        // further newline|quotechar and quotechar|quotechar
        // skipped here because tuplex is rfc4180 compliant. I.e. quotechar is "
        // that's it

        // step 3: infer separator (, is most likely)
        if(!separator.has_value())
            separator = inferSeparator(start, end, _separators, _quotechar, _maxDetectionRows);
        ss<<"finding sep took: "<<timer.time()<<"s "<<endl;
        timer.reset();
        _delimiter = separator.value();

        // step 4: with the selected separator, infer types & header
        // 4.1 has Header?
        auto numColumns = detectColumnCount(start, end, separator.value());
        auto hasHeader = detectHeader(start, end, separator.value(), numColumns);
        _header = hasHeader;
        ss<<"numColumns/header detection took: "<<timer.time()<<"s "<<endl;
        timer.reset();

        // 4.2 types
        auto types = detectTypes(start, end, separator.value(), numColumns, _quotechar, _maxDetectionRows, hasHeader, _null_values);
        ss<<"type detection took: "<<timer.time()<<"s "<<endl;
        timer.reset();

        // --> ignore rows with wrong count rows.

        // 4.3 individual stats
        // --> if string, check for json
        // --> quoted/unquoted?
        // --> whitespace after separator, after value?
        // --> length?
        // ignore header if present, else not
        auto stats = computeColumnStats(start, end, separator.value(), types, hasHeader, _threshold, _quotechar,
                                        _null_values);

        // make sure there is at least one column
        if(!stats.empty()) {
            _rowCount = stats.front().rowCount;
            _bufSize = stats.front().bufSize;
        }

        // form row type depending on stats
        // convert to python type
        vector<python::Type> pyTypes;
        vector<python::Type> pySuperTypes;
        assert(types.size() == stats.size());
        for (int i = 0; i < types.size(); ++i) {
            auto stat = stats[i];

            // check if viable candidate for delayed parsing optimization...
            if(stat.maxDequotedCellSize <= SMALL_CELL_MAX_SIZE)
                _smallCellIndices.push_back(i);

            // detect type
            auto type = mapCSVTypeToPythonType(types[i]);
            pySuperTypes.push_back(python::Type::makeOptionType(type)); // always use the larger option type.


            assert(types[i] != CSVType::NULLVALUE || stat.null_fraction >= 0.999);

            // a certain fraction of records might be NULL values!
            // if this fraction is less than threshold, treat NULL vals as exception
            // else if fraction is higher than 1.0 - threshold, treat actual vals as exception.
            // nullable? ==> make option type!
            auto et = 1.0 - _threshold; // _threshold is normal case threshold!
            assert(et < 0.5); // else this makes no sense...
            if(stat.null_fraction > et && stat.null_fraction < 1.0 - et)
                type = python::Type::makeOptionType(type);
            else if(stat.null_fraction >= 1.0 - et) {
                if(disableNullColumns)  {
                    // when null-value optimization is deactivated, use a more conservative approach to not yield NULL columns...
                    type =  python::Type::makeOptionType(type);

                    if(type == python::Type::NULLVALUE) {
                        Logger::instance().defaultLogger().warn("null value optimization deactivated, detected only null strings in column " + std::to_string(i) + ", defaulting to type Option[string]");
                        type = python::Type::makeOptionType(python::Type::STRING);
                    }
                }
                else
                    type = python::Type::NULLVALUE; // assume only null values in this column!
            }

            pyTypes.push_back(type);
        }

        _rowType = python::Type::makeTupleType(pyTypes);
        _rowSuperType = python::Type::makeTupleType(pySuperTypes);

        // step 5: aggregate statistics after type is decided
        // --> value range
        // --> min/max
        // -->
        ss<<"CSV statistics:"<<endl;
        ss<<"---------------"<<endl;
        ss<<"header:\t\t"<<(hasHeader ? "yes" : "no")<<endl;
        ss<<"separator:\t"<<charToString(separator.value())<<endl;
        ss<<"#columns:\t"<<numColumns<<endl;
        ss<<endl;
        // individual columns
        _columnNames.clear();
        _columnNames.reserve(types.size());

        // if header, get column names
        if(hasHeader) {
            // parse headers
            size_t numBytes = 0;
            auto ec = parseRow(start, start + size,
                    _columnNames, numBytes, separator.value(), '"', 0);
            assert(ExceptionCode::SUCCESS == ec);

        }
        // do not assign dummy column names
        //else
        // for(int i = 0; i < types.size(); ++i)
        //    _columnNames.push_back("column" + to_string(i+1));

        for(int i = 0; i < types.size(); ++i) {
            ss<<"-- "<<(hasHeader ? _columnNames[i] : "column" + to_string(i+1))<<endl;
            ss<<"\t\ttype:\t"<<csvtypeToString(types[i])<<endl;
            ss<<"\t\testimated python type:\t"<<pyTypes[i].desc()<<endl;
            // if options are true, print out
            auto stat = stats[i];
            ss<<"\t\tnull:\t"<<stat.null_fraction*100.0<<"%"<<endl;
            if(stat.quoted.has_value())
                ss<<"\t\tquoted:\t"<<boolToString(stat.quoted.value())<<endl;
            if(stat.length.has_value())
                ss<<"\t\tlength:\t"<<stat.length.value()<<endl;
        }

        // print everything out when in debug mode
#ifndef NDEBUG
        cout<<ss.str()<<endl;
#endif
    }
}