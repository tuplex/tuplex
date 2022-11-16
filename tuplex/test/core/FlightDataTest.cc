//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Python.h>
#include <gtest/gtest.h>
#include <PythonHelpers.h>
#include <jit/RuntimeInterface.h>
#include <ContextOptions.h>
#include <vector>
#include <Utils.h>
#include "gtest/gtest.h"
#include <Context.h>
#include "TestUtils.h"
#include "FullPipelines.h"
#include <CSVUtils.h>
#include <ErrorDataSet.h>

class FlightDataTest : public PyTest {};


// other functions to test:
// lambda x: None
// make this work with python code together!!!



TEST_F(FlightDataTest, boolConv) {
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    auto res = c.parallelize({Row(10), Row(0), Row(-10)}).map(
            UDF("lambda x: True if x > 0 else False")).collectAsVector();

    vector<Row> ref{Row(true), Row(false), Row(false)};

    ASSERT_EQ(res.size(), ref.size());

    for(int i = 0; i < ref.size(); ++i) {
#ifndef NDEBUG
        cout<<"res: "<<res[i].toPythonString()<<" ref: "<<ref[i].toPythonString()<<endl;
#endif
        EXPECT_EQ(res[i].toPythonString(), ref[i].toPythonString());
    }

}

TEST_F(FlightDataTest, cleanCodeUDF) {
    using namespace tuplex;
    using namespace std;

    // not working!!! need to fix...

    Context c(microTestOptions());

    auto cleanCode_c = "def cleanCode(t):\n"
                       "  if t == 'A':\n"
                       "    return 'carrier'\n"
                       "  elif t == 'B':\n"
                       "    return 'weather'\n"
                       "  elif t == 'C':\n"
                       "    return 'national air system'\n"
                       "  elif t == 'D':\n"
                       "    return 'security'\n"
                       "  else:\n"
                       "    return None";

    auto res = c.parallelize({Row(""), Row("A"), Row("B"),
                              Row("C"), Row("D"), Row("E")}).map(
            UDF(cleanCode_c)).collectAsVector();

    vector<Row> ref{Row(option<string>::none),
                    Row(option<string>("carrier")),
                    Row(option<string>("weather")),
                    Row(option<string>("national air system")),
                    Row(option<string>("security")),
                    Row(option<string>::none)};

    ASSERT_EQ(res.size(), ref.size());

    for(int i = 0; i < ref.size(); ++i) {
#ifndef NDEBUG
        cout<<"res: "<<res[i].toPythonString()<<" ref: "<<ref[i].toPythonString()<<endl;
#endif
        EXPECT_EQ(res[i].toPythonString(), ref[i].toPythonString());
    }
}


TEST(TupleIndices, OptionTypes) {
    auto os = python::Type::makeOptionType(python::Type::STRING);
    auto t = python::Type::makeTupleType({os, os});

    // check indices of struct
    auto indices = tuplex::codegen::getTupleIndices(t, 0);
    auto valueOffset = std::get<0>(indices);
    auto sizeOffset = std::get<1>(indices);
    auto bitmapPos = std::get<2>(indices);

    EXPECT_EQ(valueOffset, 1);
    EXPECT_EQ(sizeOffset, 3);
    EXPECT_EQ(bitmapPos, 0);

    indices = tuplex::codegen::getTupleIndices(t, 1);
    valueOffset = std::get<0>(indices);
    sizeOffset = std::get<1>(indices);
    bitmapPos = std::get<2>(indices);

    EXPECT_EQ(valueOffset, 2);
    EXPECT_EQ(sizeOffset, 4);
    EXPECT_EQ(bitmapPos, 1);



    // extensive test
    auto typeStr = "(Option[i64],Option[i64],Option[i64],Option[i64],Option[i64],Option[str],Option[str],"
                   "Option[i64],Option[str],Option[str],Option[i64],Option[i64],Option[i64],Option[i64],"
                   "Option[str],Option[str],Option[str],Option[i64],Option[str],Option[i64],Option[i64],"
                   "Option[i64],Option[i64],Option[str],Option[str],Option[str],Option[i64],Option[str],"
                   "Option[i64],Option[i64],Option[i64],Option[f64],Option[f64],Option[f64],Option[i64],Option[i64],"
                   "Option[f64],Option[i64],Option[i64],Option[f64],Option[i64],Option[i64],Option[f64],Option[f64],"
                   "Option[f64],Option[i64],Option[i64],Option[f64],Option[str],Option[f64],Option[f64],Option[f64],"
                   "Option[f64],Option[f64],Option[f64],Option[i64],Option[f64],Option[f64],Option[f64],Option[f64],"
                   "Option[f64],Option[str],Option[str],Option[str],Option[i64],Option[str],Option[str],"
                   "Option[str],Option[str],Option[str],Option[str],Option[str],Option[str],"
                   "Option[str],Option[str],Option[str],Option[str],Option[str],Option[str],"
                   "Option[str],Option[str],Option[str],Option[str],Option[str],Option[str],"
                   "Option[str],Option[str],Option[str],Option[str],Option[str],Option[str],"
                   "Option[str],Option[str],Option[str],Option[str],Option[str],Option[str],"
                   "Option[str],Option[str],Option[str],Option[str],Option[str],Option[str],"
                   "Option[str],Option[str],Option[str],Option[str],Option[str],Option[str],"
                   "Option[str])";

    auto largeType = python::decodeType(typeStr);

    ASSERT_TRUE(largeType.isTupleType());

    std::cout<<"column 35 (34): "<<largeType.parameters()[34].desc()<<std::endl;
    std::cout<<"column 35 (35): "<<largeType.parameters()[35].desc()<<std::endl;


    // now test offsets
    int numoptional = 0;
    int first_str = -1;
    int pos = 0;
    int num_str_opt = 0;
    int num_i64_opt = 0;
    int num_f64_opt = 0;
    for(auto param : largeType.parameters()) {
        if(param.isOptionType())
            numoptional++;
        if(first_str < 0 && param.withoutOption() == python::Type::STRING)
            first_str = pos;
        if(param == python::Type::makeOptionType(python::Type::I64))
            num_i64_opt++;
        if(param == python::Type::makeOptionType(python::Type::F64))
            num_f64_opt++;
        if(param == python::Type::makeOptionType(python::Type::STRING))
            num_str_opt++;
        pos++;
    }
    EXPECT_EQ(numoptional, largeType.parameters().size());
    EXPECT_EQ(largeType.parameters().size(), num_i64_opt + num_f64_opt + num_str_opt);

    // get LLVM type & check it
    auto env = std::make_shared<tuplex::codegen::LLVMEnvironment>();
    tuplex::codegen::FlattenedTuple ft(env.get());
    ft.init(largeType);
    auto llvmType = ft.getLLVMType();

    ASSERT_TRUE(llvmType->isStructTy());

    // amount of elements: +1 for bitmap, then +1 for each i64/f64, +2 for each str
    EXPECT_EQ(llvmType->getStructNumElements(), 1 + num_i64_opt + num_f64_opt + 2 * num_str_opt);

    // how many bits are there?
    int numbitmapels = core::ceilToMultiple(numoptional, 64) / 64;
    EXPECT_EQ(numbitmapels, 2);

    // size of first var len argument
    indices = tuplex::codegen::getTupleIndices(largeType, first_str);
    EXPECT_EQ(std::get<0>(indices), 1 + first_str);

    EXPECT_EQ(std::get<1>(indices), 1 + largeType.parameters().size());

}

TEST_F(FlightDataTest, DefunctYearFunc) {
    using namespace tuplex;
    using namespace std;

    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    Context c(opt);

    string file_path = "../resources/pipelines/flights/L_CARRIER_HISTORY.csv";

    // // for testing, there is a small sample within the resources folder
    // file_path = "../resources/L_CARRIER_HISTORY_sample.csv";

    string py_ref_code = "import csv\n"
                         "file_path = '" + file_path + "'\n"
                         "res = []\n"
                         "\n"
                         "with open(file_path) as csvfile:\n"
                         "    reader = csv.DictReader(csvfile)\n"
                         "    for row in reader:\n"
                         "        d = {}\n"
                         "        \n"
                         "        name_udf = lambda x: x[:x.rfind('(')].strip()\n"
                         "        founded_udf = lambda x: int(x[x.rfind('(')+1:x.rfind('-')])\n"
                         "        \n"
                         "        \n"
                         "        def extractDefunctYear(x):\n"
                         "            desc = x[x.rfind('-')+1:x.rfind(')')].strip()\n"
                         "            return int(desc) if len(desc) > 0 else None\n"
                         "        \n"
                         "        d['Code'] = row['Code']\n"
                         "        d['Description'] = row['Description']\n"
                         "        d['AirlineName'] = name_udf(row['Description'])\n"
                         "        d['AirlineYearFounded'] = founded_udf(row['Description'])\n"
                         "        d['AirlineYearDefunct'] = extractDefunctYear(row['Description'])\n"
                         "        res.append(d)\n";

    // execute python code to get reference!

    python::lockGIL();
    auto resObj = python::runAndGet(py_ref_code, "res");
    ASSERT_TRUE(resObj);
    ASSERT_TRUE(PyList_Check(resObj));

    // fetch res
    vector<Row> ref;
    for(int i = 0; i < PyList_Size(resObj); ++i) {
        auto item = PyList_GET_ITEM(resObj, i);
        ASSERT_TRUE(item);

        // defunct is option
        auto defunct = PyDict_GetItemString(item, "AirlineYearDefunct");

        auto defunct_var = defunct == Py_None ? option<int64_t>::none : option<int64_t>(PyLong_AsLongLong(defunct));

        ref.push_back(Row({python::PyString_AsString(PyDict_GetItemString(item, "Code")),
                           python::PyString_AsString(PyDict_GetItemString(item, "Description")),
                           python::PyString_AsString(PyDict_GetItemString(item, "AirlineName")),
                           PyLong_AsLongLong(PyDict_GetItemString(item, "AirlineYearFounded")),
                           defunct_var}));
    }
    python::unlockGIL();

    auto defunct_c = "def extractDefunctYear(t):\n"
                     "  x = t['Description']\n"
                     "  desc = x[x.rfind('-')+1:x.rfind(')')].strip()\n"
                     "  return int(desc) if len(desc) > 0 else None";

    // original test...
    auto res = c.csv(file_path).withColumn("AirlineName", UDF("lambda x: x[1][:x[1].rfind('(')].strip()"))
                .withColumn("AirlineYearFounded", UDF("lambda x: int(x[1][x[1].rfind('(')+1:x[1].rfind('-')])"))
                .withColumn( "AirlineYearDefunct", UDF(defunct_c))
                .collectAsVector();

    // compare ref with res rows.
    ASSERT_EQ(res.size(), ref.size());

    for(int i = 0; i < ref.size(); ++i) {
#ifndef NDEBUG
        if(i < 10)
            cout<<"res: "<<res[i].toPythonString()<<" ref: "<<ref[i].toPythonString()<<endl;
#endif
        EXPECT_EQ(res[i].toPythonString(), ref[i].toPythonString());
    }
}

TEST_F(FlightDataTest, AirportCleaning) {
    using namespace tuplex;
    using namespace std;

    string path = "../resources/GlobalAirportDatabase_sample.txt";

    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate

    // projection pushdown enabled!
    opt.set("tuplex.optimizer.selectionPushdown", "true");

    // null value opt!
    // opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");

    //
    // opt.set("tuplex.normalcaseThreshold", "0.6"); // use this to specify what to select...
    //

    Context c(opt);

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    c.csv(path,
          vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                         "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                         "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
          option<bool>::none, option<char>(':'), '"', vector<string>{"", "N/A"})

          // test, add select Columns here...
          .selectColumns({"AirportName", "Altitude", "AirportCity", "IATACode"})
          .mapColumn("AirportName", UDF("lambda x: string.capwords(x) if x else None", "", ce))
          .mapColumn("AirportCity", UDF("lambda x: string.capwords(x) if x else None", "", ce))
          .mapColumn("Altitude", UDF("lambda x: x + 1.0")) // logical plan optimization projection pushdown should remove this!
          .selectColumns(vector<string>{"AirportName", "AirportCity"})
          .show();
}

TEST_F(FlightDataTest, SampleInputSplit) {
    using namespace tuplex;
    using namespace std;

    auto path = "../resources/flights_on_time_performance_2019_01.sample.csv";

    // use a ref context which reads in the whole file + then test different input split sizes...
    auto opt_ref = microTestOptions();
    opt_ref.set("tuplex.inputSplitSize", "16MB"); // big split, sample file is tiny
    opt_ref.set("tuplex.executorCount", "0"); // single threaded
    opt_ref.set("tuplex.optimizer.generateParser", "true");

    Context c_ref(opt_ref);
    auto ref = pipelineAsStrs(c_ref.csv(path));
    ASSERT_EQ(ref.size(), 19); // 19 rows!

    vector<size_t> splitSizes{512, 1024, 2048, 4096, 8192};

    for(auto splitSize : splitSizes) {
        // for testing
        auto opt = microTestOptions();
        opt.set("tuplex.inputSplitSize", std::to_string(splitSize) + "B"); // artificially introduce splits...
        opt.set("tuplex.executorCount", "0"); // single threaded
        opt.set("tuplex.optimizer.generateParser", "true");

        Context c(opt);
        auto res = pipelineAsStrs(c.csv(path));

        ASSERT_EQ(res.size(), ref.size());
        for(int i = 0; i < std::min(res.size(), ref.size()); ++i) {
            EXPECT_EQ(res[i], ref[i]);
        }
    }
}

TEST_F(FlightDataTest, LeftJoin) {
    using namespace tuplex;
    using namespace std;

    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    //opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate

    // deactivate NULL value optimization, it's not working yet -.-
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");

    // Notes: prob need to adjust columns in projection pushdown because of change in map operator...
    // also need to overwrite for rename/empty UDF...
    opt.set("tuplex.optimizer.selectionPushdown", "true");

    Context c(opt);

    string path = "../resources/joins/flight_mini_sample_2019_01.csv";
    string airport_path = "../resources/joins/GlobalAirportDatabase_leftjoin.txt"; // restricted set of airports so not for any ORIGIN entry in the above sample there is an airport...

    // TODO: add to csv reader null_values as configurable option...

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    auto ds_airport = c.csv(airport_path,
          vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                         "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                         "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
          option<bool>::none, option<char>(':'))

    // problem here with mapColumn...
    // need to fix it...
            .mapColumn("AirportName", UDF("lambda x: string.capwords(x) if x else None", "", ce))
            .mapColumn("AirportCity", UDF("lambda x: string.capwords(x) if x else None", "", ce));

//    c.csv(path).leftJoin(ds_airport, std::string("ORIGIN"), std::string("IATACode")).show();

        // test code
    c.csv(path).leftJoin(ds_airport, std::string("ORIGIN"), std::string("IATACode"))
    .selectColumns(vector<string>{"FL_DATE", "DEP_DELAY", "AirportCity", "AirportName", "ICAOCode"}).show();

    //cols selected should be FL_DATE, AirPortName, AirportCity, ORIGIN
}


std::vector<std::string> leftJoinPipelineI(tuplex::Context& ctx) {
    using namespace std;
    using namespace tuplex;

    string path = "../resources/joins/flight_mini_sample_2019_01.csv";
    string airport_path = "../resources/joins/GlobalAirportDatabase_leftjoin.txt"; // restricted set of airports so not for any ORIGIN entry in the above sample there is an airport...

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    auto ds_airport = ctx.csv(airport_path,
                            vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                           "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                           "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                            option<bool>::none, option<char>(':'))
                    // column producing output that is finally omitted, i.e. can be removed from plan.
            .withColumn("not_needed", UDF("lambda x: x['Country']"))
            .mapColumn("AirportName", UDF("lambda x: string.capwords(x) if x else None", "", ce))
            .mapColumn("AirportCity", UDF("lambda x: string .capwords(x) if x else None", "", ce));

    // join & collect result as string!
    auto& ds = ctx.csv(path).leftJoin(ds_airport, std::string("ORIGIN"), std::string("IATACode"))
            .selectColumns(vector<string>{"FL_DATE", "DEP_DELAY", "AirportCity", "AirportName", "ICAOCode"});

    auto res = ds.collectAsVector();

    vector<string> stringifiedResult;
    for(auto r : res)
        stringifiedResult.push_back(r.toPythonString());

    return stringifiedResult;
}

std::vector<std::string> leftJoinPipelineII(tuplex::Context& ctx) {
    using namespace std;
    using namespace tuplex;

    string path = "../resources/joins/flight_mini_sample_2019_01.csv";
    string airport_path = "../resources/joins/GlobalAirportDatabase_leftjoin.txt"; // restricted set of airports so not for any ORIGIN entry in the above sample there is an airport...

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    auto ds_airport = ctx.csv(airport_path,
                              vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                             "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                             "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                              option<bool>::none, option<char>(':'))
            .mapColumn("AirportName", UDF("lambda x: string.capwords(x) if x else None", "", ce))
            .selectColumns(vector<string>{"AirportCity", "IATACode", "AirportName"})
            .mapColumn("AirportCity", UDF("lambda x: string .capwords(x) if x else None", "", ce));

    // join & collect result as string!
    auto res = ctx.csv(path).leftJoin(ds_airport, std::string("ORIGIN"), std::string("IATACode"))
            .selectColumns(vector<string>{"FL_DATE", "DEP_DELAY", "AirportCity", "AirportName"}).collectAsVector();

    vector<string> stringifiedResult;
    for(auto r : res)
        stringifiedResult.push_back(r.toPythonString());

    return stringifiedResult;
}

std::vector<std::string> leftJoinPipelineIII(tuplex::Context& ctx) {
    using namespace std;
    using namespace tuplex;

    string path = "../resources/joins/flight_mini_sample_2019_01.csv";
    string airport_path = "../resources/joins/GlobalAirportDatabase_leftjoin.txt"; // restricted set of airports so not for any ORIGIN entry in the above sample there is an airport...

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    auto ds_airport = ctx.csv(airport_path,
                              vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                             "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                             "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                              option<bool>::none, option<char>(':'))
            .mapColumn("AirportName", UDF("lambda x: string.capwords(x) if x else None", "", ce))
            .withColumn("added", UDF("lambda x: 'hello world'"))
            .withColumn("added2", UDF("lambda x: 'hello world'"))
            .withColumn("added3", UDF("lambda x: '__' + x['added2']"))
            .filter(UDF("lambda x: x['Altitude'] > 0"))
            .selectColumns(vector<string>{"AirportCity", "IATACode", "AirportName", "added"})
            .mapColumn("AirportCity", UDF("lambda x: string .capwords(x) if x else None", "", ce));

    // join & collect result as string!
    auto res = ctx.csv(path).leftJoin(ds_airport, std::string("ORIGIN"), std::string("IATACode"))
            .selectColumns(vector<string>{"added", "FL_DATE", "DEP_DELAY", "AirportCity", "AirportName"})
            .filter(UDF("lambda x: len(x['AirportName']) > 0 if x['AirportName'] else False"))
            .collectAsVector();

    // TODO: booleans and/or not yet working correctly
    // => .filter(UDF("lambda x: x['AirportName'] and len(x['AirportName']) > 0"))

    vector<string> stringifiedResult;
    for(auto r : res)
        stringifiedResult.push_back(r.toPythonString());

    return stringifiedResult;
}

std::vector<std::string> leftJoinPipelineIIIman(tuplex::Context& ctx) {
    using namespace std;
    using namespace tuplex;

    string path = "../resources/joins/flight_mini_sample_2019_01.csv";
    string airport_path = "../resources/joins/GlobalAirportDatabase_leftjoin.txt"; // restricted set of airports so not for any ORIGIN entry in the above sample there is an airport...

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    auto& ds_airport = ctx.csv(airport_path,
                              vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                             "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                             "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                              option<bool>::none, option<char>(':'))
            .filter(UDF("lambda x: x['Altitude'] > 0"))
            .withColumn("added", UDF("lambda x: 'hello world'"))
            .selectColumns(vector<string>{"AirportCity", "IATACode", "AirportName", "added"})
            .filter(UDF("lambda x: len(x['AirportName']) > 0"));

    // join & collect result as string!
    auto res = ctx.csv(path).leftJoin(ds_airport, std::string("ORIGIN"), std::string("IATACode"))
            //.selectColumns(vector<string>{"added", "FL_DATE", "DEP_DELAY", "AirportCity", "AirportName"})
            .collectAsVector();

    vector<string> stringifiedResult;
    for(auto r : res)
        stringifiedResult.push_back(r.toPythonString());

    return stringifiedResult;
}

std::vector<std::string> leftJoinPipelineIV(tuplex::Context& ctx) {
    using namespace std;
    using namespace tuplex;

    string path = "../resources/joins/flight_mini_sample_2019_01.csv";
    string airport_path = "../resources/joins/GlobalAirportDatabase_leftjoin.txt"; // restricted set of airports so not for any ORIGIN entry in the above sample there is an airport...

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    auto& ds_airport = ctx.csv(airport_path,
                              vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                             "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                             "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                              option<bool>::none, option<char>(':'))
            .mapColumn("AirportName", UDF("lambda x: string.capwords(x) if x else None", "", ce))
            .renameColumn("IATACode", "IATA")
            .filter(UDF("lambda x: x['Altitude'] > 0"))
            .selectColumns(vector<string>{"AirportCity", "IATA", "AirportName"})
            .mapColumn("AirportCity", UDF("lambda x: string .capwords(x) if x else None", "", ce));

    // join & collect result as string!
    auto res = ctx.csv(path).leftJoin(ds_airport, std::string("ORIGIN"), std::string("IATA"))
            .selectColumns(vector<string>{"FL_DATE", "DEP_DELAY", "AirportCity", "AirportName", "ORIGIN"})
            .filter(UDF("lambda x: len(x['AirportName']) > 0 if x['AirportName'] else False"))
            .renameColumn("ORIGIN", "IATACode")
            .collectAsVector();

    vector<string> stringifiedResult;
    for(const auto& r : res)
        stringifiedResult.push_back(r.toPythonString());

    return stringifiedResult;
}

TEST_F(FlightDataTest, SelectAndFilterPushdown) {
    using namespace std;
    using namespace tuplex;
    string airport_path = "../resources/pipelines/flights/GlobalAirportDatabase.txt"; // restricted set of airports so not for any ORIGIN entry in the above sample there is an airport...

    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate

    // deactivate NULL value optimization
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt.set("tuplex.optimizer.selectionPushdown", "true");

    Context ctx(opt);

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    auto& ds_airport = ctx.csv(airport_path,
                               vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                                              "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                              "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
                               option<bool>::none, option<char>(':'))
            .mapColumn("AirportName", UDF("lambda x: string.capwords(x) if x else None", "", ce))
            .renameColumn("IATACode", "IATA")
            .selectColumns(vector<string>{"AirportCity", "IATA", "Altitude", "AirportName"})
            .mapColumn("AirportCity", UDF("lambda x: string .capwords(x) if x else None", "", ce))
            .filter(UDF("lambda x: x['Altitude'] > 0"));

    auto res = pipelineAsStrs(ds_airport);
}

TEST_F(FlightDataTest, WeirdFilterIssue) {
    using namespace std;
    using namespace tuplex;

    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate

    // deactivate NULL value optimization
    opt.set("tuplex.optimizer.filterPushdown", "true");
    opt.set("tuplex.optimizer.selectionPushdown", "false");

    Context ctx(opt);

    string path = "../resources/joins/flight_mini_sample_2019_01.csv";
    string airport_path = "../resources/joins/GlobalAirportDatabase_leftjoin.txt"; // restricted set of airports so not for any ORIGIN entry in the above sample there is an airport...

    ClosureEnvironment ce;
    ce.importModuleAs("string", "string");

    // test
    ctx.csv(airport_path,
            vector<string>{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees", "LatitudeMinutes",
                           "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                           "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal", "LongitudeDecimal"},
            option<bool>::none, option<char>(':'))
    .filter(UDF("lambda x: x['Altitude'] > 0"))
    .mapColumn("AirportName", UDF("lambda x: string.capwords(x) if x else None", "", ce))
    .withColumn("added", UDF("lambda x: 'hello world'"))
    .withColumn("added2", UDF("lambda x: 'hello world'"))
    .withColumn("added3", UDF("lambda x: '__' + x['added2']"))
    .selectColumns(vector<string>{"AirportCity", "IATACode", "AirportName", "added"})
    .mapColumn("AirportCity", UDF("lambda x: string .capwords(x) if x else None", "", ce))
    .filter(UDF("lambda x: len(x['AirportName']) > 0")).show();
}

TEST_F(FlightDataTest, ProjectionPushdown) {
    using namespace tuplex;
    using namespace std;

    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate

    // deactivate NULL value optimization
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt.set("tuplex.optimizer.selectionPushdown", "true");
    opt.set("tuplex.optimizer.filterPushdown", "true");

    auto opt_ref = opt;
    opt_ref.set("tuplex.optimizer.selectionPushdown", "false");
    opt_ref.set("tuplex.optimizer.filterPushdown", "false");

    // test a couple pipelines using the two different context objects.

    Context c(opt);
    Context c_ref(opt_ref);

    // pipeline I: select after JOIN
    {
        auto res = leftJoinPipelineI(c);

        for(int i = 0; i < 5; ++i)
            cout<<res[i]<<endl;
        auto ref = leftJoinPipelineI(c_ref);

        ASSERT_EQ(res.size(), ref.size());
        for (int i = 0; i < res.size(); ++i) {
            EXPECT_EQ(res[i], ref[i]);
        }
    }

    // pipeline II: select before and after join + redundant select
    {
        auto res = leftJoinPipelineII(c);
        auto ref = leftJoinPipelineII(c_ref);

        ASSERT_EQ(res.size(), ref.size());
        for (int i = 0; i < res.size(); ++i) {
            EXPECT_EQ(res[i], ref[i]);
        }
    }

    // pipeline III: withCol, mapCol, filter + map before/after join operators...
    {
        // TODO: pushdown of filter which accesses a renamed column? => test for that!!!

        auto ref = leftJoinPipelineIII(c_ref);
        std::cout<<"reference:\n----\n";
        for(auto r : ref) {
            std::cout<<r<<std::endl;
        }

        auto res = leftJoinPipelineIII(c);
        std::cout<<"result:\n----\n";
        for(auto r : res) {
            std::cout<<r<<std::endl;
        }

        ASSERT_EQ(res.size(), ref.size());
        for (int i = 0; i < res.size(); ++i) {
            EXPECT_EQ(res[i], ref[i]);
        }
    }

    // pipeline IV: rename everywhere
    {
        auto res = leftJoinPipelineIV(c);
        auto ref = leftJoinPipelineIV(c_ref);

        ASSERT_GT(res.size(), 0);
        ASSERT_EQ(res.size(), ref.size());
        for (int i = 0; i < res.size(); ++i) {
            EXPECT_EQ(res[i], ref[i]);
        }
    }

    // pipeline V: couple ops, but no projection pushdown possible...
    // ==> i.e. add renames but NO real map!
}

// @TODO: write a test for selection pushdown with the following variants:

// 1.) include 2x map, filter, join, withColumn, mapColumn
// mapColumn after

// this a basic test with activated projection pushdown where nor selectColumns nor map operator is used.
// it should therefore still use all columns per default
TEST_F(FlightDataTest, ProjectionPushdownInvariant) {
    using namespace tuplex;
    using namespace std;

    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate

    // deactivate NULL value optimization
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt.set("tuplex.optimizer.selectionPushdown", "true");

    Context c(opt);

    auto &ds = c.parallelize({Row(1, 2, 3, 4), Row(5, 6, 7, 8)}, {"a", "b", "c", "d"})
            .filter(UDF("lambda x: x['a'] % 2 == 1"))
            .mapColumn("b", UDF("lambda x: x * x"))
            .withColumn("e", UDF("lambda x: x['a']"));

    auto res = pipelineAsStrs(ds);
    ASSERT_EQ(res.size(), 2);
    EXPECT_EQ(res[0], "(1,4,3,4,1)");
}


TEST_F(FlightDataTest, NullGeneratedParserTest) {
    using namespace tuplex;
    using namespace std;

    // production
    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt.set("tuplex.optimizer.generateParser", "true"); // activate generated parser!!!

    Context c(opt);
    auto path = "../resources/flights_on_time_performance_2019_01.sample.csv";

    c.csv(path).show(3);

    // compare to context with regular parser...
    auto opt_ref = opt;
    opt.set("tuplex.optimizer.generateParser", "false"); // activate generated parser!!!
    Context c_ref(opt_ref);


    auto v_ref = c_ref.csv(path).collectAsVector();
    auto v = c.csv(path).collectAsVector();

    // compare
    for(int i = 0; i < std::min(v.size(), v_ref.size()); ++i) {
        EXPECT_EQ(v[i].toPythonString(), v_ref[i].toPythonString());
    }
    ASSERT_EQ(v.size(), v_ref.size());
}

TEST_F(FlightDataTest, LargeFilePipeline) {
    using namespace tuplex;
    using namespace std;

    //    // @TODO: glob for ~ doesn't work yet.
    // @TODO: sampling is off, produces many errors because of assuming either NULL or not NULL
    // ==> for test forced to option types!

    // production
    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    //opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate

    //// TODO: fix here null values!!!
    // opt.set("tuplex.optimizer.generateParser", "true");

    opt.set("tuplex.optimizer.generateParser", "false");
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "false");
    opt.set("tuplex.optimizer.operatorReordering", "false");

    // bbsn00 settings
    opt.set("tuplex.inputSplitSize", "64MB");
    opt.set("tuplex.runTimeMemory", "340MB");
    opt.set("tuplex.optimizer.generateParser", "true");
    opt.set("tuplex.optimizer.selectionPushdown", "true");

    Context c(opt);

    // @TODO: no automatic selection of smaller table yet here, need to implement that..
    // @TODO: There still seems to be a bug with None/Null for FIRST_DEP_TIME ??? ==> problem is in generated parser! I.e. fix this there...
    // @TODO: To speed up code generation, pass pointers! get rid off some weird legacy code...

    auto& ds = flightPipeline(c);
    ds.tocsv(URI("tuplex_output.csv"));
}

TEST_F(FlightDataTest, FallbackPythonDict) {
    using namespace tuplex;
    using namespace std;

    // check that auto-dict unwrapping in fallback mode works

    // production
    auto opt = testOptions();
    opt.set("tuplex.runTimeMemory", "128MB"); // join might require a lot of runtime memory!!!
    opt.set("tuplex.executorCount", "0"); // single-threaded
    opt.set("tuplex.useLLVMOptimizer", "false"); // deactivate
    opt.set("tuplex.optimizer.generateParser", "false");
    opt.set("tuplex.optimizer.retypeUsingOptimizedInputSchema", "true");
    opt.set("tuplex.optimizer.operatorReordering", "false");
    opt.set("tuplex.optimizer.selectionPushdown", "false");

    Context c(opt);

    auto v = c.csv("../resources/flights_on_time_performance_2019_01.sample.csv")
            .map(UDF("lambda x: {'origin': x['ORIGIN_AIRPORT_ID'], 'dest': x['DEST_AIRPORT_ID']}")).collectAsVector();

    ASSERT_NE(v.size(), 0);

}



// TODO: following code doesn't work yet
//     auto cleanCode_c = "def cleanCode(t):\n"
//                       "  t = t[\"CANCELLATION_CODE\"]\n"
//                       "  if t == 'A':\n"
//                       "    return 'carrier'\n"
//                       "  elif t == 'B':\n"
//                       "    return 'weather'\n"
//                       "  elif t == 'C':\n"
//                       "    return 'national air system'\n"
//                       "  elif t == 'D':\n"
//                       "    return 'security'\n"
//                       "  else:\n"
//                       "    return None";