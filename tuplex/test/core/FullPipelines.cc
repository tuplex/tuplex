//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "FullPipelines.h"
#include <string>
#include <vector>

namespace tuplex {

    std::vector<std::string> pipelineAsStrs(DataSet& ds) {
        using namespace std;

        vector<string> stringifiedResult;

        if(ds.isError()) {
            cerr<<"error dataset received in pipelineAsStrs"<<endl;
            return vector<string>{};
        }

        auto res = ds.collectAsVector();
        for(const auto& r : res)
            stringifiedResult.emplace_back(r.toPythonString());
        return stringifiedResult;
    }

    std::string flightNormalizeCol(const std::string &col) {
        std::string s;

        if(col.empty())
            return col;

        tuplex::splitString(col, '_', [&](const std::string &p) {
            s += tuplex::char2str(toupper(p[0]));
            for (int j = 1; j < p.length(); ++j)
                s += tuplex::char2str(std::tolower(p[j]));
        });

        return s;
    }

    DataSet& flightPipelineMini(Context& ctx,
                            const std::string& bts_path,
                            const std::string carrier_path,
                            const std::string& airport_path,
                            bool cache) {
        using namespace tuplex;
        using namespace std;

        auto cleanCode_c = "def cleanCode(t):\n"
                           "    if t[\"CancellationCode\"] == 'A':\n"
                           "        return 'carrier'\n"
                           "    elif t[\"CancellationCode\"] == 'B':\n"
                           "        return 'weather'\n"
                           "    elif t[\"CancellationCode\"] == 'C':\n"
                           "        return 'national air system'\n"
                           "    elif t[\"CancellationCode\"] == 'D':\n"
                           "        return 'security'\n"
                           "    else:\n"
                           "        return None";

        auto divertedCode_c = "def divertedUDF(row):\n"
                              "    diverted = row['Diverted']\n"
                              "    ccode = row['CancellationCode']\n"
                              "    if diverted:\n"
                              "        return 'diverted'\n"
                              "    else:\n"
                              "        if ccode:\n"
                              "            return ccode\n"
                              "        else:\n"
                              "            return 'None'";

        auto fillInTimes_C = "def fillInTimesUDF(row):\n"
                             "    ACTUAL_ELAPSED_TIME = row['ActualElapsedTime']\n"
                             "    if row['DivReachedDest']:\n"
                             "        if float(row['DivReachedDest']) > 0:\n"
                             "            return float(row['DivActualElapsedTime'])\n"
                             "        else:\n"
                             "            return ACTUAL_ELAPSED_TIME\n"
                             "    else:\n"
                             "        return ACTUAL_ELAPSED_TIME";

        // the main table
        auto &ds = ctx.csv(bts_path);

        ds = ds.renameColumn("ORIGIN", "Origin")
                .renameColumn("DEST", "Dest")
                .renameColumn("OP_UNIQUE_CARRIER", "OpUniqueCarrier");

        if(cache)
            ds = ds.cache();

        // output schema?
        cout<<"bts output schema: "<<ds.schema().getRowType().desc()<<endl;

        auto& ds2 = ctx.csv(carrier_path);

        if(cache)
            ds2 = ds2.cache();
        // output schema?
        cout<<"carrier output schema: "<<ds2.schema().getRowType().desc()<<endl;

        // load airport table and process it. Requires to join
        auto& ds3 = ctx.csv(airport_path,
                            vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                           "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                           "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                            option<bool>::none, option<char>(':'));
        if(cache)
            ds3 = ds3.cache();
         // output schema?
        cout<<"airport output schema: "<<ds3.schema().getRowType().desc()<<endl;

         auto& ds_all = ds.join(ds2,  std::string("OpUniqueCarrier"), std::string("Code"))
                 .leftJoin(ds3, std::string("Origin"), std::string("IATACode"),std::string(), std::string(), std::string("Origin"), std::string())
                 .leftJoin(ds3, std::string("Dest"), std::string("IATACode"),std::string(), std::string(), std::string("Dest"), std::string());


        //auto& ds_all = ds.join(ds2.filter(UDF("lambda x: len(x[0]) > 0")),  std::string("OpUniqueCarrier"), std::string("Code"));

        return ds_all;
    }

     DataSet& flightPipeline(Context& ctx,
                                const std::string& bts_path,
                                const std::string carrier_path,
                                const std::string& airport_path,
                                bool cache,
                                bool withIgnores) {
            using namespace tuplex;
            using namespace std;

            auto cleanCode_c = "def cleanCode(t):\n"
                               "    if t[\"CancellationCode\"] == 'A':\n"
                               "        return 'carrier'\n"
                               "    elif t[\"CancellationCode\"] == 'B':\n"
                               "        return 'weather'\n"
                               "    elif t[\"CancellationCode\"] == 'C':\n"
                               "        return 'national air system'\n"
                               "    elif t[\"CancellationCode\"] == 'D':\n"
                               "        return 'security'\n"
                               "    else:\n"
                               "        return None";

            auto divertedCode_c = "def divertedUDF(row):\n"
                                  "    diverted = row['Diverted']\n"
                                  "    ccode = row['CancellationCode']\n"
                                  "    if diverted:\n"
                                  "        return 'diverted'\n"
                                  "    else:\n"
                                  "        if ccode:\n"
                                  "            return ccode\n"
                                  "        else:\n"
                                  "            return 'None'";

            auto fillInTimes_C = "def fillInTimesUDF(row):\n"
                                 "    ACTUAL_ELAPSED_TIME = row['ActualElapsedTime']\n"
                                 "    if row['DivReachedDest']:\n"
                                 "        if float(row['DivReachedDest']) > 0:\n"
                                 "            return float(row['DivActualElapsedTime'])\n"
                                 "        else:\n"
                                 "            return ACTUAL_ELAPSED_TIME\n"
                                 "    else:\n"
                                 "        return ACTUAL_ELAPSED_TIME";

            // the main table
            auto &ds = ctx.csv(bts_path);

            // fetch columns + rename
            auto cols = ds.columns();
            vector<string> renamed_cols;
            for (int i = 0; i < cols.size(); ++i)
                renamed_cols.emplace_back(flightNormalizeCol(cols[i]));
            for (int i = 0; i < cols.size(); ++i)
                ds = ds.renameColumn(cols[i], renamed_cols[i]);

            if(cache)
                ds = ds.cache();

            // output schema?
            cout<<"bts output schema: "<<ds.schema().getRowType().desc()<<endl;

            auto& ds1 =
                    ds.withColumn("OriginCity", UDF("lambda x: x['OriginCityName'][:x['OriginCityName'].rfind(',')].strip()"))
                            .withColumn("OriginState",
                                        UDF("lambda x: x['OriginCityName'][x['OriginCityName'].rfind(',')+1:].strip()"))
                            .withColumn("DestCity", UDF("lambda x: x['DestCityName'][:x['DestCityName'].rfind(',')].strip()"))
                            .withColumn("DestState", UDF("lambda x: x['DestCityName'][x['DestCityName'].rfind(',')+1:].strip()"))
                            .mapColumn("CrsArrTime", UDF("lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100) if x else None"))
                            .mapColumn("CrsDepTime", UDF("lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100) if x else None"))
                            .withColumn("CancellationCode", UDF(cleanCode_c))
                            .mapColumn("Diverted", UDF("lambda x: True if x > 0 else False"))
                            .mapColumn("Cancelled", UDF("lambda x: True if x > 0 else False"))
                            .withColumn("CancellationReason", UDF(divertedCode_c))
                            .withColumn("ActualElapsedTime", UDF(fillInTimes_C));
            if(withIgnores)
                ds1 = ds1.ignore(ExceptionCode::TYPEERROR);

            auto defunct_c = "def extractDefunctYear(t):\n"
                             "  x = t['Description']\n"
                             "  desc = x[x.rfind('-')+1:x.rfind(')')].strip()\n"
                             "  return int(desc) if len(desc) > 0 else None";

            auto filterDefunctFlights_c = "def filterDefunctFlights(row):\n"
                                          "    year = row['Year']\n"
                                          "    airlineYearDefunct = row['AirlineYearDefunct']\n"
                                          "\n"
                                          "    if airlineYearDefunct:\n"
                                          "        return int(year) < int(airlineYearDefunct)\n"
                                          "    else:\n"
                                          "        return True";
            // load carrier lookup table & process it

            auto& ds2 = ctx.csv(carrier_path);

            if(cache)
                ds2 = ds2.cache();
            // output schema?
            cout<<"carrier output schema: "<<ds2.schema().getRowType().desc()<<endl;

            ds2 = ds2.withColumn("AirlineName", UDF("lambda x: x[1][:x[1].rfind('(')].strip()"))
                    .withColumn("AirlineYearFounded", UDF("lambda x: int(x[1][x[1].rfind('(')+1:x[1].rfind('-')])"))
                    .withColumn( "AirlineYearDefunct", UDF(defunct_c));

            // load airport table and process it. Requires to join
            auto& ds3 = ctx.csv(airport_path,
                                vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                               "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                               "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                                option<bool>::none, option<char>(':'));
            if(cache)
                ds3 = ds3.cache();

            // output schema?
            cout<<"airport output schema: "<<ds3.schema().getRowType().desc()<<endl;

            ClosureEnvironment ce;
         ce.importModuleAs("string", "string");
            ds3 = ds3.mapColumn("AirportName", UDF("lambda x: string.capwords(x) if x else None", "", ce))
                    .mapColumn("AirportCity", UDF("lambda x: string.capwords(x) if x else None", "", ce));

            auto& ds_all = ds1.join(ds2,  std::string("OpUniqueCarrier"), std::string("Code"))
                    .leftJoin(ds3, std::string("Origin"), std::string("IATACode"),std::string(), std::string(), std::string("Origin"), std::string())
                    .leftJoin(ds3, std::string("Dest"), std::string("IATACode"),std::string(), std::string(), std::string("Dest"), std::string())
                    .mapColumn("Distance", UDF("lambda x: x / 0.00062137119224"))
                    .mapColumn("AirlineName", UDF("lambda s: s.replace('Inc.', '') \\\n"
                                                  "                                              .replace('LLC', '') \\\n"
                                                  "                                              .replace('Co.', '').strip()"))
                    .renameColumn("OriginLongitudeDecimal", "OriginLongitude")
                    .renameColumn("OriginLatitudeDecimal", "OriginLatitude")
                    .renameColumn("DestLongitudeDecimal", "DestLongitude")
                    .renameColumn("DestLatitudeDecimal", "DestLatitude")
                    .renameColumn("OpUniqueCarrier", "CarrierCode")
                    .renameColumn("OpCarrierFlNum", "FlightNumber")
                    .renameColumn("DayOfMonth", "Day")
                    .renameColumn("AirlineName", "CarrierName")
                    .renameColumn("Origin", "OriginAirportIATACode")
                    .renameColumn("Dest", "DestAirportIATACode")
                    .filter(UDF(filterDefunctFlights_c));

            // map int columns
            vector<string> numeric_cols{"ActualElapsedTime", "AirTime", "ArrDelay",
                                        "CarrierDelay", "CrsElapsedTime",
                                        "DepDelay", "LateAircraftDelay", "NasDelay",
                                        "SecurityDelay", "TaxiIn", "TaxiOut", "WeatherDelay"};

            for(const auto& c : numeric_cols)
                ds_all = ds_all.mapColumn(c, UDF("lambda x: int(x) if x else None"));

            vector<string> colsToSelect{"CarrierName", "CarrierCode", "FlightNumber", "Day", "Month", "Year", "DayOfWeek",
                                        "OriginCity", "OriginState", "OriginAirportIATACode", "OriginLongitude",
                                        "OriginLatitude", "OriginAltitude", "DestCity", "DestState", "DestAirportIATACode",
                                        "DestLongitude", "DestLatitude", "DestAltitude", "Distance", "CancellationReason",
                                        "Cancelled", "Diverted", "CrsArrTime", "CrsDepTime", "ActualElapsedTime", "AirTime",
                                        "ArrDelay", "CarrierDelay", "CrsElapsedTime", "DepDelay", "LateAircraftDelay",
                                        "NasDelay", "SecurityDelay", "TaxiIn", "TaxiOut", "WeatherDelay", "AirlineYearFounded",
                                        "AirlineYearDefunct"};

            return ds_all.selectColumns(colsToSelect);
        }

    DataSet& apacheLogsPipeline(Context& ctx, const std::string& logs_path) {
        using namespace tuplex;
        using namespace std;

        auto extractFields = "def extractFields(x):\n"
                             "    y = x.strip()\n"
                             "\n"
                             "    i = y.find(' ')\n"
                             "    ip = y[:i]\n"
                             "    y = y[i+1:]\n"
                             "\n"
                             "    i = y.find(' ')\n"
                             "    client_id = y[:i]\n"
                             "    y = y[i+1:]\n"
                             "\n"
                             "    i = y.find(' ')\n"
                             "    user_id = y[:i]\n"
                             "    y = y[i+1:]\n"
                             "\n"
                             "    i = y.find(']')\n"
                             "    date = y[:i][1:]\n"
                             "    y = y[i+2:]\n"
                             "\n"
                             "    y = y[y.find('\"')+1:]\n"
                             "\n"
                             "    method = '-'\n"
                             "    endpoint = '-'\n"
                             "    protocol = '-'\n"
                             "    if y.find(' ') < y.rfind('\"'):\n"
                             "        i = y.find(' ')\n"
                             "        method = y[:i]\n"
                             "        y = y[i+1:]\n"
                             "\n"
                             "        i = y.find(' ')\n" // needs to be any whitespace
                             "        endpoint = y[:i]\n"
                             "        y = y[i+1:]\n"
                             "\n"
                             "        i = y.rfind('\"')\n"
                             "        protocol = y[:i]\n"
                             "        protocol = protocol[protocol.rfind(' ')+1:]\n"
                             "        y = y[i+2:]\n"
                             "    else:\n"
                             "        i = y.rfind('\"')\n"
                             "        y = y[i+2:]\n"
                             "\n"
                             "    i = y.find(' ')\n"
                             "    response_code = y[:i]\n"
                             "    content_size = y[i+1:]\n"
                             "\n"
                             "    return {'ip': ip,\n"
                             "            'client_id': client_id,\n"
                             "            'user_id': user_id,\n"
                             "            'date': date,\n"
                             "            'method': method,\n"
                             "            'endpoint': re.sub('^/~[^/]+/(.*)', '/~' + ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for t in range(10)]) + '/$1', endpoint),\n"
                             "            'protocol': protocol,\n"
                             "            'response_code': int(response_code),\n"
                             "            'content_size': 0 if content_size == '-' else int(content_size)}";

        auto extractFieldsRegex = "def extractFieldsRegex(logline):\n"
                                 "    match = re.search(r'^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+)\\s*(\\S*)\\s*\" (\\d{3}) (\\S+)', logline)\n"
                                 "\n"
                                 "    if(match):\n"
                                 "        return {'ip': match[1],\n"
                                 "                'client_id': match[2],\n"
                                 "                'user_id': match[3],\n"
                                 "                'date': match[4],\n"
                                 "                'method': match[5],\n"
                                 "                'endpoint': re.sub('^/~[^/]+/(.*)', '/~' + ''.join(['a', 'b', 'c']) + '/$1', match[6]),\n"
                                 "                'protocol': match[7],\n"
                                 "                'response_code': int(match[8]),\n"
                                 "                'content_size': 0 if match[9] == '-' else int(match[9])}\n"
                                 "    else:\n"
                                 "        return {'ip': '',\n"
                                 "                'client_id': '',\n"
                                 "                'user_id': '',\n"
                                 "                'date': '',\n"
                                 "                'method': '',\n"
                                 "                'endpoint': '',\n"
                                 "                'protocol': '',\n"
                                 "                'response_code': 0,\n"
                                 "                'content_size': 0}";

        auto bad_ips = ctx.csv("../resources/bad_ips_all.txt");
        ClosureEnvironment ce;
        ce.importModuleAs("re", "re");
        ce.importModuleAs("random", "random");
        auto requests = ctx.text(logs_path).map(UDF(extractFieldsRegex, "", ce));
        requests.setColumns({"ip", "client_id", "user_id", "date", "method", "endpoint", "protocol", "response_code", "content_size"});
        return requests.join(bad_ips, string("ip"), string("BadIPs"));
    }

    extern DataSet& zillowPipeline(Context& ctx,
                                   const std::string& zillow_path,
                                   bool cache) {
        using namespace std;
        // Zillow pipeline is:
        //     # Tuplex pipeline
        //    c.csv(file_path) \
        //     .withColumn("bedrooms", extractBd) \
        //     .filter(lambda x: x['bedrooms'] < 10) \
        //     .withColumn("type", extractType) \
        //     .filter(lambda x: x['type'] == 'house') \
        //     .withColumn("zipcode", lambda x: '%05d' % int(x['postal_code'])) \
        //     .mapColumn("city", lambda x: x[0].upper() + x[1:].lower()) \
        //     .withColumn("bathrooms", extractBa) \
        //     .withColumn("sqft", extractSqft) \
        //     .withColumn("offer", extractOffer) \
        //     .withColumn("price", extractPrice) \
        //     .filter(lambda x: 100000 < x['price'] < 2e7) \
        //     .selectColumns(["url", "zipcode", "address", "city", "state",
        //                     "bedrooms", "bathrooms", "sqft", "offer", "type", "price"]) \
        //     .tocsv("expout.csv")


        auto extractBd_c = "def extractBd(x):\n"
                           "    val = x['facts and features']\n"
                           "    max_idx = val.find(' bd')\n"
                           "    if max_idx < 0:\n"
                           "        max_idx = len(val)\n"
                           "    s = val[:max_idx]\n"
                           "\n"
                           "    # find comma before\n"
                           "    split_idx = s.rfind(',')\n"
                           "    if split_idx < 0:\n"
                           "        split_idx = 0\n"
                           "    else:\n"
                           "        split_idx += 2\n"
                           "    r = s[split_idx:]\n"
                           "    return int(r)";
        auto extractType_c = "def extractType(x):\n"
                             "    t = x['title'].lower()\n"
                             "    type = 'unknown'\n"
                             "    if 'condo' in t or 'apartment' in t:\n"
                             "        type = 'condo'\n"
                             "    if 'house' in t:\n"
                             "        type = 'house'\n"
                             "    return type";

        auto extractBa_c = "def extractBa(x):\n"
                           "    val = x['facts and features']\n"
                           "    max_idx = val.find(' ba')\n"
                           "    if max_idx < 0:\n"
                           "        max_idx = len(val)\n"
                           "    s = val[:max_idx]\n"
                           "\n"
                           "    # find comma before\n"
                           "    split_idx = s.rfind(',')\n"
                           "    if split_idx < 0:\n"
                           "        split_idx = 0\n"
                           "    else:\n"
                           "        split_idx += 2\n"
                           "    r = s[split_idx:]\n"
                           "    return int(r)";

        auto extractSqft_c = "def extractSqft(x):\n"
                             "    val = x['facts and features']\n"
                             "    max_idx = val.find(' sqft')\n"
                             "    if max_idx < 0:\n"
                             "        max_idx = len(val)\n"
                             "    s = val[:max_idx]\n"
                             "\n"
                             "    split_idx = s.rfind('ba ,')\n"
                             "    if split_idx < 0:\n"
                             "        split_idx = 0\n"
                             "    else:\n"
                             "        split_idx += 5\n"
                             "    r = s[split_idx:]\n"
                             "    r = r.replace(',', '')\n"
                             "    return int(r)";

        auto extractOffer_c = "def extractOffer(x):\n"
                              "    offer = x['title'].lower()\n"
                              "\n"
                              "    if 'sale' in offer:\n"
                              "        offer = 'sale'\n"
                              "    elif 'rent' in offer:\n"
                              "        offer = 'rent'\n"
                              "    elif 'sold' in offer:\n"
                              "        offer = 'sold'\n"
                              "    elif 'foreclos' in offer.lower():\n"
                              "        offer = 'foreclosed'\n"
                              "    else:\n"
                              "        offer = 'unknown'\n"
                              "\n"
                              "    return offer";;

        // this function has a symbol table issue... needs to be resolved ...
//        auto extractPrice_c = "def extractPrice(x):\n"
//                              "    price = x['price']\n"
//                              "\n"
//                              "    if x['offer'] == 'sold':\n"
//                              "        # price is to be calculated using price/sqft * sqft\n"
//                              "        val = x['facts and features']\n"
//                              "        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]\n"
//                              "        r = s[s.find('$')+1:s.find(', ') - 1]\n"
//                              "        price_per_sqft = int(r)\n"
//                              "        price = price_per_sqft * x['sqft']\n"
//                              "    elif x['offer'] == 'rent':\n"
//                              "        max_idx = price.rfind('/')\n"
//                              "        price = int(price[1:max_idx].replace(',', ''))\n"
//                              "    else:\n"
//                              "        # take price from price column\n"
//                              "        price = int(price[1:].replace(',', ''))\n"
//                              "\n"
//                              "    return price";

        // this works...
        auto extractPrice_c = "def extractPrice(x):\n"
                              "    price = x['price']\n"
                              "    p = 0\n"
                              "    if x['offer'] == 'sold':\n"
                              "        # price is to be calculated using price/sqft * sqft\n"
                              "        val = x['facts and features']\n"
                              "        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]\n"
                              "        r = s[s.find('$')+1:s.find(', ') - 1]\n"
                              "        price_per_sqft = int(r)\n"
                              "        p = price_per_sqft * x['sqft']\n"
                              "    elif x['offer'] == 'rent':\n"
                              "        max_idx = price.rfind('/')\n"
                              "        p = int(price[1:max_idx].replace(',', ''))\n"
                              "    else:\n"
                              "        # take price from price column\n"
                              "        p = int(price[1:].replace(',', ''))\n"
                              "\n"
                              "    return p";

        // cache file first or not?
        if(cache) {

            // cache data & select only required columns to simulate pushdown
            vector<string> required_cols{"title", "address", "city", "state",
                                         "postal_code", "price", "facts and features", "url"};
            auto& dsCached = ctx.csv(zillow_path).selectColumns(required_cols).cache();
            return dsCached.withColumn("bedrooms", UDF(extractBd_c))
                    .filter(UDF("lambda x: x['bedrooms'] < 10"))
                    .withColumn("type", UDF(extractType_c))
                    .filter(UDF("lambda x: x['type'] == 'house'"))
                    .withColumn("zipcode", UDF("lambda x: '%05d' % int(x['postal_code'])"))
                    .mapColumn("city", UDF("lambda x: x[0].upper() + x[1:].lower()"))
                    .withColumn("bathrooms", UDF(extractBa_c))
                    .withColumn("sqft", UDF(extractSqft_c))
                    .withColumn("offer", UDF(extractOffer_c))
                    .withColumn("price", UDF(extractPrice_c))
                    .filter(UDF("lambda x: 100000 < x['price'] < 2e7"))
                    .selectColumns(vector<string>{"url", "zipcode", "address", "city", "state",
                                                  "bedrooms", "bathrooms", "sqft", "offer",
                                                  "type", "price"});
        } else {
            return ctx.csv(zillow_path)
                    .withColumn("bedrooms", UDF(extractBd_c))
                    .filter(UDF("lambda x: x['bedrooms'] < 10"))
                    .withColumn("type", UDF(extractType_c))
                    .filter(UDF("lambda x: x['type'] == 'house'"))
                    .withColumn("zipcode", UDF("lambda x: '%05d' % int(x['postal_code'])"))
                    .mapColumn("city", UDF("lambda x: x[0].upper() + x[1:].lower()"))
                    .withColumn("bathrooms", UDF(extractBa_c))
                    .withColumn("sqft", UDF(extractSqft_c))
                    .withColumn("offer", UDF(extractOffer_c))
                    .withColumn("price", UDF(extractPrice_c))
                    .filter(UDF("lambda x: 100000 < x['price'] < 2e7"))
                    .selectColumns(vector<string>{"url", "zipcode", "address", "city", "state",
                                                  "bedrooms", "bathrooms", "sqft", "offer",
                                                  "type", "price"});
        }
    }

    DataSet& serviceRequestsPipeline(Context& ctx, const std::string& service_path) {
        using namespace std;


        // do this:
        // ------------
        // but low priority.


        // Prio1: exploring the dictionary optimization
        // Prio1b: add more time wise logging to Tuplex.... (i.e. compute time / IO)
        // Prio2: finishing the null-value optimization
        //        (might need more than one day, might not be worth it) // Prio2: the 311 sample query
        // Prio3: where the time is going...
        //


        // notice the null check at the beginning...
        auto fix_zip_codes_c = "def fix_zip_codes(zips):\n"
                               "    if not zips:\n"
                               "         return None\n"
                               "    # Truncate everything to length 5 \n"
                               "    s = zips[:5]\n"
                               "    \n"
                               "    # Set 00000 zip codes to nan\n"
                               "    if s == '00000':\n"
                               "         return None\n"
                               "    else:\n"
                               "         return s";

        // TODO: Need to force type hint here as string to make above code executable
        // ==> other version should be super simple and just require fix of bad records via resolve...

        // force type of column to string, Incident Zip is column #8!
        return ctx.csv(service_path, vector<string>{},
                option<bool>::none,option<char>::none, '"',
                vector<string>{"Unspecified", "NO CLUE", "NA", "N/A", "0", ""},
                unordered_map<size_t, python::Type>{{8, python::Type::makeOptionType(python::Type::STRING)}})
        .mapColumn("Incident Zip", UDF(fix_zip_codes_c))
        .selectColumns(vector<string>{"Incident Zip"})
        .unique();
    }
}

// ----------------------------------------------------------------------------------------//
// testing here for all the pipelines (allows easy debugging)                              //
// ----------------------------------------------------------------------------------------//

class PipelinesTest : public PyTest {};

#ifdef BUILD_WITH_AWS

TEST_F(PipelinesTest, GithubLambdaVersion) {
//#ifdef SKIP_AWS_TESTS
//    GTEST_SKIP();
//#endif

    using namespace std;
    using namespace tuplex;
    auto co = testOptions();

    co.set("tuplex.backend", "lambda");
    co.set("tuplex.scratchDir", "s3://tuplex/scratch");
    co.set("tuplex.useLLVMOptimizer", "true");

    Context c(co);


    // create github based (JSON) pipeline.


}

TEST_F(PipelinesTest, ZillowAWS) {
#ifdef SKIP_AWS_TESTS
    GTEST_SKIP();
#endif
    using namespace std;
    using namespace tuplex;
    auto co = testOptions();

    co.set("tuplex.backend", "lambda");
    co.set("tuplex.scratchDir", "s3://tuplex/scratch");
    co.set("tuplex.useLLVMOptimizer", "true");

    Context c(co);

    // perform a simple test with a remote file & transfer to local result
    auto uri = URI("s3://tuplex/data/zillow100KB.csv");

    // 10GB example
    uri = URI("s3://tuplex/data/100GB/*.csv");

    // fully fledged Zillow pipeline
    Timer timer;
    zillowPipeline(c, uri.toPath()).tocsv("s3://tuplex/scratch/zillow_output.csv");
    cout<<"PIPELINE DONE IN "<<timer.time()<<" SECONDS"<<endl;

    //c.csv(uri.toPath()).selectColumns(vector<string>{"facts and features"}).tocsv("s3://tuplex/scratch/output.csv");

    // reading back to local memory.
//    auto res = c.csv(uri.toPath()).selectColumns(vector<string>{"facts and features"}).collectAsVector();
//
//    for(const auto& r : res) {
//        cout<<r.toPythonString()<<endl;
//    }
}

#endif // BUILD_WITH_AWS

TEST_F(PipelinesTest, ZillowConfigHarness) {
    using namespace tuplex;
    using namespace std;

    auto zpath = "../resources/pipelines/zillow/zillow_noexc.csv";
    vector<bool> cache_values{false, true};


    for(auto cache: cache_values) {
        // for reference deactivate all options!
        auto opt_ref = testOptions();
        opt_ref.set("tuplex.runTimeMemory", "128MB");
        opt_ref.set("tuplex.executorCount", "0"); // single-threaded
        opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
        opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
        opt_ref.set("tuplex.optimizer.selectionPushdown", "false");
        opt_ref.set("tuplex.optimizer.generateParser", "false");

        Context c_ref(opt_ref);
        auto ref = pipelineAsStrs(zillowPipeline(c_ref, zpath, cache));

        // with LLVM optimizers enabled
        auto opt_wLLVMOpt = opt_ref;
        opt_wLLVMOpt.set("tuplex.useLLVMOptimizer", "true");
        Context c_wLLVMOpt(opt_wLLVMOpt);
        auto r_wLLVMOpt = pipelineAsStrs(zillowPipeline(c_wLLVMOpt, zpath, cache));
        compareStrArrays(r_wLLVMOpt, ref, true);

        // with projection pushdown enabled
        auto opt_proj = opt_ref;
        opt_proj.set("tuplex.optimizer.selectionPushdown", "true");
        Context c_proj(opt_proj);
        auto r_proj = pipelineAsStrs(zillowPipeline(c_proj, zpath, cache));
        compareStrArrays(r_proj, ref, true);

        // with projection pushdown + LLVM Optimizers
        auto opt_proj_wLLVMOpt = opt_ref;
        opt_proj_wLLVMOpt.set("tuplex.optimizer.selectionPushdown", "true");
        opt_proj_wLLVMOpt.set("tuplex.useLLVMOptimizer", "true");
        Context c_proj_wLLVMOpt(opt_proj_wLLVMOpt);
        auto r_proj_wLLVMOpt = pipelineAsStrs(zillowPipeline(c_proj_wLLVMOpt, zpath, cache));
        compareStrArrays(r_proj_wLLVMOpt, ref, true);

        // with null value optimization (i.e. getting rid off them!)
        auto opt_null = opt_ref;
        opt_null.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
        Context c_null(opt_null);
        auto r_null = pipelineAsStrs(zillowPipeline(c_null, zpath, cache));
        compareStrArrays(r_null, ref, true);

        // with null value + proj
        auto opt_null_proj = opt_ref;
        opt_null_proj.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
        opt_null_proj.set("tuplex.optimizer.selectionPushdown", "true");
        Context c_null_proj(opt_null_proj);
        auto r_null_proj = pipelineAsStrs(zillowPipeline(c_null_proj, zpath, cache));
        compareStrArrays(r_null_proj, ref, true);

        // with null value + proj + llvmopt
        auto opt_null_proj_opt = opt_ref;
        opt_null_proj_opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
        opt_null_proj_opt.set("tuplex.optimizer.selectionPushdown", "true");
        opt_null_proj_opt.set("tuplex.useLLVMOptimizer", "true");
        Context c_null_proj_opt(opt_null_proj_opt);
        auto r_null_proj_opt = pipelineAsStrs(zillowPipeline(c_null_proj_opt, zpath, cache));
        compareStrArrays(r_null_proj_opt, ref, true);

        // with projection pushdown + LLVM Optimizers + generated parser
        auto opt_proj_wLLVMOpt_parse = opt_ref;
        opt_proj_wLLVMOpt_parse.set("tuplex.optimizer.selectionPushdown", "true");
        opt_proj_wLLVMOpt_parse.set("tuplex.useLLVMOptimizer", "true");
        opt_proj_wLLVMOpt_parse.set("tuplex.optimizer.generateParser", "true");
        Context c_proj_wLLVMOpt_parse(opt_proj_wLLVMOpt_parse);
        auto r_proj_wLLVMOpt_parse = pipelineAsStrs(zillowPipeline(c_proj_wLLVMOpt_parse, zpath, cache));
        compareStrArrays(r_proj_wLLVMOpt_parse, ref, true);

        // NULL value OPTIMIZATION
        // with projection pushdown + LLVM Optimizers + generated parser + null value opt
        auto opt_proj_wLLVMOpt_parse_null = opt_ref;
        opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.selectionPushdown", "true");
        opt_proj_wLLVMOpt_parse_null.set("tuplex.useLLVMOptimizer", "true");
        opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.generateParser", "true");
        opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
        Context c_proj_wLLVMOpt_parse_null(opt_proj_wLLVMOpt_parse_null);
        auto r_proj_wLLVMOpt_parse_null = pipelineAsStrs(zillowPipeline(c_proj_wLLVMOpt_parse_null, zpath, cache));
        // b.c. null value opt destroys order, sort both arrays
        std::sort(r_proj_wLLVMOpt_parse_null.begin(), r_proj_wLLVMOpt_parse_null.end());
        std::sort(ref.begin(), ref.end());
        compareStrArrays(r_proj_wLLVMOpt_parse_null, ref, true);
    }
}

TEST_F(PipelinesTest, ServiceRequestsConfigHarnessNVOvsNormal) {
    using namespace tuplex;
    using namespace std;

    // for reference - deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "false");
    opt_ref.set("tuplex.optimizer.generateParser", "false");

    auto opt_nvo = opt_ref;
    opt_nvo.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");

    EXPECT_EQ(opt_ref.OPT_NULLVALUE_OPTIMIZATION(), false);
    EXPECT_EQ(opt_nvo.OPT_NULLVALUE_OPTIMIZATION(), true);


    // now with NVO
    Context c_nvo(opt_nvo);
    auto res_nvo = pipelineAsStrs(serviceRequestsPipeline(c_nvo));
    std::cout<<"NVO result has "<<pluralize(res_nvo.size(), "row")<<std::endl;

    Context c_ref(opt_ref);
    auto ref = pipelineAsStrs(serviceRequestsPipeline(c_ref));

    // sort both (b.c. order might be different)
    std::sort(ref.begin(), ref.end());
    std::sort(res_nvo.begin(), res_nvo.end());

    // // print out both?
    // cout<<"ref | nvo"<<endl;
    // for(size_t i = 0; i < std::min(ref.size(), res_nvo.size()); ++i)
    //     cout<<ref[i]<<" | "<<res_nvo[i]<<endl;


    // compare arrays
    EXPECT_EQ(ref.size(), res_nvo.size());
    for(size_t i = 0; i < std::min(ref.size(), res_nvo.size()); ++i) {
        EXPECT_EQ(ref[i], res_nvo[i]);
    }

   // @TODO: doesn't work yet because no type hint for the Incident column. Tuplex (rightfully) assumes an integer column and therefore, won't exec that code even.
   // ==> solutions: 1) type hint for input OR 2) add resolver with csv parse/dict resolve logic to touch up problem :)

   // to make this comparable to weld, need to add unique!
   // also, they restrict the parsing -.- no real support thus...

   // version 1: with resolve operator!
   auto& ctx = c_ref;

   //   auto fix_zip_codes_c = "def fix_zip_codes(zips):\n"
    //                               "    # Truncate everything to length 5 \n"
    //                               "    s = zips[:5]\n"
    //                               "    \n"
    //                               "    # Set 00000 zip codes to nan\n"
    //                               "    if s == '00000':\n"
    //                               "         return None\n"
    //                               "    else:\n"
    //                               "         return s";
    //
    //        // TODO: Need to force type hint here as string to make above code executable
    //        // ==> other version should be super simple and just require fix of bad records via resolve...
    //        return ctx.csv(service_path, vector<string>{}, option<bool>::none,option<char>::none, '"', vector<string>{"NO CLUE", "N/A", "0"})
    //        .mapColumn("Incident Zip", UDF(fix_zip_codes_c));



//    using namespace std;
//    string service_path="../resources/pipelines/311/311-service-requests.sample.csv";
//    ctx.csv(service_path, vector<string>{}, option<bool>::none,option<char>::none, '"', vector<string>{"NO CLUE", "N/A", "0", ""})
//    .selectColumns(vector<string>{"Incident Zip"}).show(10);
}

TEST_F(PipelinesTest, FlightsWithPyResolver) {
    // using sample 2018_01 without pushdown leads to several rows getting rejected at parsing stage
    // => need to resolve via interpreter b.c. no fast code path can do that!
    using namespace tuplex;

    // for reference - deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.driverMemory", "4GB");
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "false");
    opt_ref.set("tuplex.csv.operatorReordering", "false");
    // opt_ref.set("tuplex.optimizer.generateParser", "true");
    opt_ref.set("tuplex.optimizer.generateParser", "true");

    // opt_ref.set("tuplex.optimizer.generateParser", "false");

    Context c_ref(opt_ref);
    std::string bts_path="../resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv";
    // this file causes
    //Row 5593 exception: value error
    //    => serialized bad row to memory: 1282 bytes
    //Row 6254 exception: value error
    //    => serialized bad row to memory: 1288 bytes
    //Row 7114 exception: value error
    //    => serialized bad row to memory: 1280 bytes
    //Row 7725 exception: value error
    //    => serialized bad row to memory: 1268 bytes
    //Row 8632 exception: value error
    //    => serialized bad row to memory: 1281 bytes
    //Row 9378 exception: value error
    //    => serialized bad row to memory: 1286 bytes
    //Row 9441 exception: value error
    //    => serialized bad row to memory: 1289 bytes
    //Row 9511 exception: value error
    //    => serialized bad row to memory: 1284 bytes

    using namespace tuplex;
    using namespace std;
    auto ref = pipelineAsStrs(flightPipeline(c_ref, bts_path));


    //@TODO: experiment regarding the exception handling paths/etc



    // @TODO: write test with csv file / no generated reader
    // where row functor throws exception on single row.
    // i.e a/ b where b is sometimes 0. => important so exceptions get tracked...
}


TEST_F(PipelinesTest, FlightDevWithColumnPyFallback) {
    // test pipeline over several context configurations
    using namespace tuplex;

    // for reference deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.driverMemory", "4GB");
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "false"); // disable for now, prob errors later...

    // Note: when null value opt is turned off AND selection pushdown is off,
    // then on 2009 there are 8 rows where errors are produced on columns which actually do not
    // matter


    opt_ref.set("tuplex.optimizer.generateParser", "true"); // do not use par
    opt_ref.set("tuplex.inputSplitSize", "128MB"); // probably something wrong with the reader, again??

    Context c_ref(opt_ref);
    std::string bts_path="../resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv";

    using namespace tuplex;
    using namespace std;

    // do csv b.c. it's easier...
    auto fillInTimes_C = "def fillInTimesUDF(row):\n"
                         "    ACTUAL_ELAPSED_TIME = row['ActualElapsedTime']\n"
                         "    if row['DivReachedDest']:\n"
                         "        if float(row['DivReachedDest']) > 0:\n"
                         "            return float(row['DivActualElapsedTime'])\n"
                         "        else:\n"
                         "            return ACTUAL_ELAPSED_TIME\n"
                         "    else:\n"
                         "        return ACTUAL_ELAPSED_TIME";

    // --> python output is the other option/row output i.e. merging things together
    c_ref.csv(bts_path)
    .renameColumn("ACTUAL_ELAPSED_TIME", "ActualElapsedTime")
    .renameColumn("DIV_REACHED_DEST", "DivReachedDest")
    .renameColumn("DIV_ACTUAL_ELAPSED_TIME", "DivActualElapsedTime")
    .withColumn("ActualElapsedTime", UDF(fillInTimes_C))
    .resolve(ExceptionCode::TYPEERROR, UDF("lambda x: 0.0")) // DIV_ACTUAL_ELAPSED_TIME is None, so the python code fails.
    .selectColumns({"ActualElapsedTime"})
    .tocsv("pyresolve.csv");
}

// to be resolved...
TEST_F(PipelinesTest, FlightDevToFixWithPurePythonPipeline) {
    // test pipeline over several context configurations
    using namespace tuplex;

    // for reference deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.driverMemory", "4GB");
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "false"); // disable for now, prob errors later...

    // Note: when null value opt is turned off AND selection pushdown is off,
    // then on 2009 there are 8 rows where errors are produced on columns which actually do not
    // matter

    opt_ref.set("tuplex.optimizer.generateParser", "true"); // do not use par
    opt_ref.set("tuplex.inputSplitSize", "128MB"); // probably something wrong with the reader, again??

    Context c_ref(opt_ref);
    std::string bts_path="../resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv";

    // do csv b.c. it's easier...
    // --> python output is the other option/row output i.e. merging things together
    flightPipeline(c_ref, bts_path).tocsv("pyresolve.csv");
}

TEST_F(PipelinesTest, TypeErrorFlightPipeline) {
    using namespace tuplex;

    // exploratory test...
    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.driverMemory", "4GB");
    opt.set("tuplex.executorCount", "15"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "true"); // deactivate
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    opt.set("tuplex.optimizer.selectionPushdown", "true"); // disable for now, prob errors later...
    opt.set("tuplex.optimizer.generateParser", "false"); // do not use parser generation
    opt.set("tuplex.inputSplitSize", "128MB"); // probably something wrong with the reader, again??
    opt.set("tuplex.resolveWithInterpreterOnly", "false");

    Context ctx(opt);


    // Checks:
    // 1.) Is selectionPushdown NOT working with interpreter only resolve??
    // ---> yup, exactly that is the issue!!! i.e. interpreteronly == true AND optimizer.selectionPushdown == true
    // 2.) what is the typeError that occurs??

    // check //~ expansion in the future...
    auto bts_paths = "../resources/flights_on_time_performance_2019_01.sample.csv";
    auto carrier_path="../resources/pipelines/flights/L_CARRIER_HISTORY.csv";
    auto airport_path="../resources/pipelines/flights/GlobalAirportDatabase.txt";
    flightPipeline(ctx, bts_paths, carrier_path, airport_path, false, true).tocsv("test_flights_output");
}


TEST_F(PipelinesTest, FlightNullValueCacheMini) {
    // test pipeline over several context configurations
    using namespace tuplex;

    // for reference deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "256MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorMemory", "4GB");
    opt_ref.set("tuplex.driverMemory", "4GB");
    opt_ref.set("tuplex.partitionSize", "32MB");
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "true"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "true"); // disable for now, prob errors later...
    opt_ref.set("tuplex.optimizer.generateParser", "false"); // do not use par
    opt_ref.set("tuplex.inputSplitSize", "64MB"); // probably something wrong with the reader, again??
    opt_ref.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    Context c_ref(opt_ref);
    std::string bts_path="../resources/pipelines/flights/flights_on_time_performance_2019_01.10k-sample.csv";
    auto carrier_path="../resources/pipelines/flights/L_CARRIER_HISTORY.csv";
    auto airport_path="../resources/pipelines/flights/GlobalAirportDatabase.txt";

    // create with null-value opt
    auto opt = opt_ref;
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    Context c(opt);
    flightPipelineMini(c, bts_path,carrier_path, airport_path, true).tocsv(URI("flight_output/"));


    // create ref
    flightPipelineMini(c_ref, bts_path,carrier_path, airport_path, true).tocsv(URI("flight_output_ref/"));



    // time to load the files (each is one)
    auto ref_content = fileToString(URI("flight_output_ref/part0.csv"));
    auto res_content = fileToString(URI("flight_output/part0.csv"));

    // need to split into lines
    auto ref = splitToLines(ref_content);
    auto res = splitToLines(res_content);

    ASSERT_EQ(ref.size(), res.size());

    // compare headers
    EXPECT_EQ(ref[0], res[0]);

    // sort both array
    std::sort(ref.begin(), ref.end());
    std::sort(res.begin(), res.end());

    for(int i = 0; i < std::min(ref.size(), res.size()); ++i) {
        EXPECT_EQ(ref[0], res[0]);
    }
}

#warning "fails, b.c. hashtable output for resolvetask is required. Would need 2h to implement..."
//TEST_F(PipelinesTest, FlightValueError) {
//    using namespace tuplex;
//    Context c;
//
//    flightPipeline(c, "../resources/pipelines/flights/value_error.csv").tocsv("flight_test");
//
//    // => to process this 100% correctly, need interpreter resolve
//    // for this need to implement join in interpreter backup!
//    // // 2019_09 leads to a single row error
//    // flightPipeline(c, ".../flights/data/flights_on_time_performance_2019_09.csv").tocsv("flight_test");
//}

TEST_F(PipelinesTest, ZillowDev) {
    // test pipeline over several context configurations
    using namespace tuplex;

    // for reference deactivate all options!
    auto opt_ref = testOptions();

    //     conf = {"webui.enable" : False,
    //            "executorCount" : 16,
    //            "executorMemory" : "2G",
    //            "driverMemory" : "2G",
    //            "partitionSize" : "16MB",
    //            "runTimeMemory" : "128MB",
    //            "inputSplitSize":"64MB",
    //            "useLLVMOptimizer" : True,
    //            "optimizer.retypeUsingOptimizedInputSchema" : False,
    //            "optimizer.selectionPushdown" : True,
    //            "optimizer.generateParser": False}


    //     conf = {"webui.enable" : False,
    //            "executorCount" : 16,
    //            "executorMemory" : "2G",
    //            "driverMemory" : "2G",
    //            "partitionSize" : "32MB",
    //            "runTimeMemory" : "128MB",
    //            "useLLVMOptimizer" : True,
    //            "optimizer.retypeUsingOptimizedInputSchema" : False,
    //            "optimizer.selectionPushdown" : True}
    opt_ref.set("tuplex.runTimeMemory", "256MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorMemory", "2GB");
    opt_ref.set("tuplex.driverMemory", "2GB");
    opt_ref.set("tuplex.partitionSize", "16MB");
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "true"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "true"); // disable for now, prob errors later...
    opt_ref.set("tuplex.optimizer.generateParser", "true"); // do not use par => wrong parse for some cell here!
    opt_ref.set("tuplex.inputSplitSize", "64MB"); // probably something wrong with the reader, again??
    //opt_ref.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    Context c_ref(opt_ref);

    auto z_path = "../resources/pipelines/zillow/zillow_noexc.csv";

    zillowPipeline(c_ref, z_path, true).tocsv(URI("zillow_output/"));
}

// This test fails on travis. No idea why, needs to be fixed at some point -.-
TEST_F(PipelinesTest, ApacheDev) {
    // test pipeline over several context configurations
    using namespace tuplex;

    std::string logs_path = "../resources/pipelines/weblogs/logs.sample.txt";

    // for reference deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "false");
    opt_ref.set("tuplex.optimizer.generateParser", "false");
    opt_ref.set("tuplex.optimizer.filterPushdown", "true");
    opt_ref.set("tuplex.optimizer.sharedObjectPropagation", "true");
    // opt_ref.set("tuplex.inputSplitSize", "2MB");

    Context c_ref(opt_ref);
    auto ref = pipelineAsStrs(apacheLogsPipeline(c_ref, logs_path));

    std::cout << ref.size() << " rows\n";

    // @TODO: this test should be better written with a backup to actually compute these numbers and not used fixed ones
    ASSERT_EQ(ref.size(), 3437);
    for(unsigned long i=0; i<10; i++) {
        std::cout << i << ": " << ref[i] << "\n";
    }

    // NULL value OPTIMIZATION
    // with projection pushdown + LLVM Optimizers + generated parser + null value opt
    auto opt_proj_wLLVMOpt_parse_null = opt_ref;
    opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.selectionPushdown", "true");
    opt_proj_wLLVMOpt_parse_null.set("tuplex.useLLVMOptimizer", "true");
    opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.generateParser", "true");
    opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    Context c_proj_wLLVMOpt_parse_null(opt_proj_wLLVMOpt_parse_null);
    auto r_proj_wLLVMOpt_parse_null = pipelineAsStrs(apacheLogsPipeline(c_ref, logs_path));
    // b.c. null value opt destroys order, sort both arrays
    std::sort(r_proj_wLLVMOpt_parse_null.begin(), r_proj_wLLVMOpt_parse_null.end());
    std::sort(ref.begin(), ref.end());
    compareStrArrays(r_proj_wLLVMOpt_parse_null, ref, true);
}

TEST_F(PipelinesTest, NullValueApache) {
    using namespace tuplex;
    using namespace std;

    string logs_path = "../resources/pipelines/weblogs/logs.sample.txt";
    string ip_path = "../resources/pipelines/weblogs/ip_blacklist.csv";
    // for reference deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.executorCount", "4"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "true");
    opt_ref.set("tuplex.optimizer.generateParser", "false");
    opt_ref.set("tuplex.optimizer.filterPushdown", "true");
    opt_ref.set("tuplex.optimizer.sharedObjectPropagation", "true");

    // split_regex pipeline
    //         ctx.text(','.join(perf_paths))
    //        .map(lambda x: {'logline':x})
    //        .withColumn("ip", extract_ip)
    //        .withColumn("client_id", extract_client_id)
    //        .withColumn("user_id", extract_user_id)
    //        .withColumn("date", extract_date)
    //        .withColumn("method", extract_method)
    //        .withColumn("endpoint", extract_endpoint)
    //        .withColumn("protocol", extract_protocol)
    //        .withColumn("response_code", extract_response_code)
    //        .withColumn("content_size", extract_content_size)
    //        .filter(lambda x: len(x['endpoint']) > 0)
    //        .mapColumn("endpoint", randomize_udf)

    auto extract_ip_c = "def extract_ip(x):\n"
    "    match = re.search(\"(^\\\\S+) \", x['logline'])\n"
    "    if match:\n"
    "        return match[1]\n"
    "    else:\n"
    "        return ''\n";

    auto extract_client_c = "def extract_client_id(x):\n"
    "    match = re.search(\"^\\\\S+ (\\\\S+) \", x['logline'])\n"
    "    if match:\n"
    "        return match[1]\n"
    "    else:\n"
    "        return ''\n";
    auto extract_user_c = "def extract_user_id(x):\n"
    "    match = re.search(\"^\\\\S+ \\\\S+ (\\\\S+) \", x['logline'])\n"
    "    if match:\n"
    "        return match[1]\n"
    "    else:\n"
    "        return ''\n";

    auto extract_date_c = "def extract_date(x):\n"
                          "    match = re.search(r\"^.*\\[([\\w:/]+\\s[+\\-]\\d{4})\\]\", x['logline'])\n"
                          "    if match:\n"
                          "        return match[1]\n"
                          "    else:\n"
                          "        return ''";

    auto extract_method_c = "def extract_method(x):\n"
    "    match = re.search('^.*\"(\\\\S+) \\\\S+\\\\s*\\\\S*\\\\s*\"', x['logline'])\n"
    "    if match:\n"
    "        return match[1]\n"
    "    else:\n"
    "        return ''\n";

    auto extract_endpoint_c = "def extract_endpoint(x):\n"
    "    match = re.search('^.*\"\\\\S+ (\\\\S+)\\\\s*\\\\S*\\\\s*\"', x['logline'])\n"
    "    if match:\n"
    "        return match[1]\n"
    "    else:\n"
    "        return ''\n";

    auto extract_protocol_c = "def extract_protocol(x):\n"
    "    match = re.search('^.*\"\\\\S+ \\\\S+\\\\s*(\\\\S*)\\s*\"', x['logline'])\n"
    "    if match:\n"
    "        return match[1]\n"
    "    else:\n"
    "        return ''\n";

    auto extract_response_c = "def extract_response_code(x):\n"
    "    match = re.search('^.*\" (\\\\d{3}) ', x['logline'])\n"
    "    if match:\n"
    "        return int(match[1])\n"
    "    else:\n"
    "        return -1\n";

    auto extract_content_c = "def extract_content_size(x):\n"
    "    match = re.search('^.*\" \\\\d{3} (\\\\S+)', x['logline'])\n"
    "    if match:\n"
    "        return 0 if match[1] == '-' else int(match[1])\n"
    "    else:\n"
    "        return -1";

    auto randomize_c = "def randomize_udf(x):\n"
    "    return re.sub('^/~[^/]+', '/~' + ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for t in range(10)]), x)";

    Context ctx(opt_ref);

    ClosureEnvironment ce;
    ce.importModuleAs("random", "random");
    ce.importModuleAs("re", "re");

    auto& ds_ips = ctx.csv(ip_path);
    ctx.text(logs_path)
            .map(UDF("lambda x: {'logline':x}"))
            .withColumn("ip", UDF(extract_ip_c, "", ce))
            .withColumn("client_id", UDF(extract_client_c, "", ce))
            .withColumn("user_id", UDF(extract_user_c, "", ce))
            .withColumn("date", UDF(extract_date_c, "", ce))
            .withColumn("method", UDF(extract_method_c, "", ce))
            .withColumn("endpoint", UDF(extract_endpoint_c, "", ce))
            .withColumn("protocol", UDF(extract_protocol_c, "", ce))
            .withColumn("response_code", UDF(extract_response_c, "", ce))
            .withColumn("content_size", UDF(extract_content_c, "", ce))
            .filter(UDF("lambda x: len(x['endpoint']) > 0"))
            .mapColumn("endpoint", UDF(randomize_c, "", ce))
            .join(ds_ips, string("ip"), string("BadIPs"))
            .selectColumns(vector<string>{"ip", "endpoint"}).tocsv("logs.csv");

    auto content = fileToString("logs.part0.csv");
    EXPECT_EQ(content, "ip,endpoint\n"
                       "1.1.209.108,/research/finance/\n");
}


TEST_F(PipelinesTest, FlightWithIgnore) {
    using namespace tuplex;
    using namespace std;
    std::string bts_path="../resources/pipelines/flights/flights_on_time_performance_2019_01.10k-sample.csv";
    std::string carrier_path="../resources/pipelines/flights/L_CARRIER_HISTORY.csv";
    std::string airport_path="../resources/pipelines/flights/GlobalAirportDatabase.txt";

    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "false");
    opt_ref.set("tuplex.optimizer.generateParser", "false");
    opt_ref.set("tuplex.optimizer.filterPushdown", "false");

    auto opt_proj_wLLVMOpt_parse_null = opt_ref;
    opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.selectionPushdown", "true");
//    opt_proj_wLLVMOpt_parse_null.set("tuplex.useLLVMOptimizer", "true");
//    opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.generateParser", "true");
//    opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    Context c_proj_wLLVMOpt_parse_null(opt_proj_wLLVMOpt_parse_null);

    flightPipeline(c_proj_wLLVMOpt_parse_null, bts_path, carrier_path, airport_path, false, true).tocsv(testName + ".csv");

}

TEST_F(PipelinesTest, CarriersOnly) {
    // test pipeline over several context configurations
    using namespace tuplex;
    using namespace std;
    std::string bts_path="../resources/pipelines/flights/flights_on_time_performance_2019_01.10k-sample.csv";
    std::string carrier_path="../resources/pipelines/flights/L_CARRIER_HISTORY.csv";
    std::string airport_path="../resources/pipelines/flights/GlobalAirportDatabase.txt";
    vector<bool> cache_values{false, true};

    // for reference deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "false");
    opt_ref.set("tuplex.optimizer.generateParser", "false");
    opt_ref.set("tuplex.optimizer.mergeExceptionsInOrder", "false");

    Context c_ref(opt_ref);

    // only carrier part

    auto defunct_c = "def extractDefunctYear(t):\n"
                     "  x = t['Description']\n"
                     "  desc = x[x.rfind('-')+1:x.rfind(')')].strip()\n"
                     "  return int(desc) if len(desc) > 0 else None";


    // load carrier lookup table & process it
    auto& ds2 = c_ref.csv(carrier_path);

    // output schema?
    cout<<"carrier output schema: "<<ds2.schema().getRowType().desc()<<endl;

    ds2 = ds2.withColumn("AirlineName", UDF("lambda x: x[1][:x[1].rfind('(')].strip()"))
            .withColumn("AirlineYearFounded", UDF("lambda x: int(x[1][x[1].rfind('(')+1:x[1].rfind('-')])"))
            .withColumn( "AirlineYearDefunct", UDF(defunct_c));

    auto ref = pipelineAsStrs(ds2.selectColumns({"Code"}).unique());

    for(auto r : ref)
        std::cout<<r<<std::endl;
}

TEST_F(PipelinesTest, FlightConfigHarness) {
    // test pipeline over several context configurations
    using namespace tuplex;
    using namespace std;
    std::string bts_path="../resources/pipelines/flights/flights_on_time_performance_2019_01.10k-sample.csv";
    std::string carrier_path="../resources/pipelines/flights/L_CARRIER_HISTORY.csv";
    std::string airport_path="../resources/pipelines/flights/GlobalAirportDatabase.txt";
    vector<bool> cache_values{false, true};

    for(auto cache: cache_values) {
        // for reference deactivate all options!
        auto opt_ref = testOptions();
        opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
        opt_ref.set("tuplex.executorCount", "0"); // single-threaded
        opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
        opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
        opt_ref.set("tuplex.optimizer.selectionPushdown", "false");
        opt_ref.set("tuplex.optimizer.generateParser", "false");
        opt_ref.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
        // Note: all resolve with interpreter work, except for when projection pushdown is used.
        //       then there's an error in the interpreter path. Reported as Bug issue #https://github.com/LeonhardFS/Tuplex/issues/247
        opt_ref.set("tuplex.resolveWithInterpreterOnly", "false");

        Context c_ref(opt_ref);
        auto ref = pipelineAsStrs(flightPipeline(c_ref, bts_path, carrier_path, airport_path, cache));

        // with LLVM optimizers enabled
        auto opt_wLLVMOpt = opt_ref;
        opt_wLLVMOpt.set("tuplex.useLLVMOptimizer", "true");
        Context c_wLLVMOpt(opt_wLLVMOpt);
        auto r_wLLVMOpt = pipelineAsStrs(flightPipeline(c_wLLVMOpt, bts_path, carrier_path, airport_path, cache));
        compareStrArrays(r_wLLVMOpt, ref, true);

        // with projection pushdown enabled
        auto opt_proj = opt_ref;
        opt_proj.set("tuplex.optimizer.selectionPushdown", "true");
        Context c_proj(opt_proj);
        auto r_proj = pipelineAsStrs(flightPipeline(c_proj, bts_path, carrier_path, airport_path, cache));

        compareStrArrays(r_proj, ref, true);

        // with projection pushdown + LLVM Optimizers
        auto opt_proj_wLLVMOpt = opt_ref;
        opt_proj_wLLVMOpt.set("tuplex.optimizer.selectionPushdown", "true");
        opt_proj_wLLVMOpt.set("tuplex.useLLVMOptimizer", "true");
        Context c_proj_wLLVMOpt(opt_proj_wLLVMOpt);
        auto r_proj_wLLVMOpt = pipelineAsStrs(flightPipeline(c_proj_wLLVMOpt, bts_path, carrier_path, airport_path, cache));
        compareStrArrays(r_proj_wLLVMOpt, ref, true);

        // with projection pushdown + LLVM Optimizers + generated parser
        auto opt_proj_wLLVMOpt_parse = opt_ref;
        opt_proj_wLLVMOpt_parse.set("tuplex.optimizer.selectionPushdown", "true");
        opt_proj_wLLVMOpt_parse.set("tuplex.useLLVMOptimizer", "true");
        opt_proj_wLLVMOpt_parse.set("tuplex.optimizer.generateParser", "true");
        Context c_proj_wLLVMOpt_parse(opt_proj_wLLVMOpt_parse);
        auto r_proj_wLLVMOpt_parse = pipelineAsStrs(flightPipeline(c_proj_wLLVMOpt_parse, bts_path, carrier_path, airport_path, cache));
        compareStrArrays(r_proj_wLLVMOpt_parse, ref, true);

        // NULL value OPTIMIZATION
        // with projection pushdown + LLVM Optimizers + generated parser + null value opt
        auto opt_proj_wLLVMOpt_parse_null = opt_ref;
        opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.selectionPushdown", "true");
        opt_proj_wLLVMOpt_parse_null.set("tuplex.useLLVMOptimizer", "true");
        opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.generateParser", "true");
        opt_proj_wLLVMOpt_parse_null.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
        Context c_proj_wLLVMOpt_parse_null(opt_proj_wLLVMOpt_parse_null);
        auto r_proj_wLLVMOpt_parse_null = pipelineAsStrs(flightPipeline(c_proj_wLLVMOpt_parse_null, bts_path, carrier_path, airport_path, cache));
        // b.c. null value opt destroys order, sort both arrays
        std::sort(r_proj_wLLVMOpt_parse_null.begin(), r_proj_wLLVMOpt_parse_null.end());
        std::sort(ref.begin(), ref.end());
        compareStrArrays(r_proj_wLLVMOpt_parse_null, ref, true);
    }
}


TEST_F(PipelinesTest, GoogleTrace) {
    // checking that compilation for google pipeline works...
    using namespace tuplex;
    using namespace std;

    // for reference deactivate all options!
    auto opt_ref = testOptions();
    opt_ref.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt_ref.set("tuplex.executorCount", "0"); // single-threaded
    opt_ref.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt_ref.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt_ref.set("tuplex.optimizer.selectionPushdown", "false");
    opt_ref.set("tuplex.optimizer.generateParser", "false");

    Context c_ref(opt_ref);

    vector<string> machine_event_cols{"time", "machine_ID", "event_type", "platform_ID", "CPUs", "Memory"};
    c_ref.csv("../resources/pipelines/gtrace/machine_events.csv", machine_event_cols)
         .filter(UDF("lambda active_machines: active_machines[2] == 0"))
         .filter(UDF("lambda active_machines: active_machines[5] != 0.0"))
         .show();

    // check bug cases:
    // .filter(lambda active_machines: active_machines['Memory'] != 0.0) => this prints out garbage
}