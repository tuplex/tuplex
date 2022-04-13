//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 2/25/2022                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Python.h>
#include <gtest/gtest.h>
#include <ContextOptions.h>
#include <vector>
#include <Utils.h>
#include <Context.h>
#include "../core/TestUtils.h"
#include "../core/FullPipelines.h"
#include <CSVUtils.h>
#include <CSVStatistic.h>
#include <parser/Parser.h>
#include <tracing/TraceVisitor.h>



#include <physical/codegen/StagePlanner.h>

class SamplingTest : public PyTest {};


// @TODO:
// func traceFlightsPipeline(rows, pip) ....
// func traceRows(...) ... --> convert rows to pyobjects & trace them then!

// then, draw sample from all files, trace all these rows. check accesses

// then, draw sample each file, check accesses.

// can delay parsing be differently used???


// in the trace it would be interesting to know for each input var, how often it get's accessed.
// -> that's important for the delayed parsing optimization.
// if it's only accessed once (or never?) then could use delayed parsing if it's a small string item or so!

TEST_F(SamplingTest, BasicAccessChecks) {
    using namespace std;
    using namespace tuplex;

    // i.e., here x is accessed twice!
    const std::string code_simple = "lambda x: (x + 1) * x";
    const std::string code_def = "def f(x):\n\ty = 2 * x\n\tx = 20\n\treturn y\n";

    const std::string code_multiparam = "lambda a, b, c, d, e, f: a + c * (f + 1)";
    const std::string code_tupledef = "def f(x):\n\treturn x[1] + x[45] + x[2] * x[10] + 2\n";
    // for this func here, x should be accessed once!
    // def f(x):
    //   y = 2 * x
    //   x = 20
    //   return y


    // @TODO: need to account for the cases of a, b, c
    // and the tuple...
    // => dict unrolling?

    auto ast_simple = tuplex::parseToAST(code_simple);
    EXPECT_TRUE(ast_simple);
    auto ast_def = tuplex::parseToAST(code_def);
    EXPECT_TRUE(ast_def);

    auto ast_multiparam = tuplex::parseToAST(code_multiparam);
    EXPECT_TRUE(ast_multiparam);
    auto ast_tupledef = tuplex::parseToAST(code_tupledef);
    EXPECT_TRUE(ast_tupledef);

    python::lockGIL();

    TraceVisitor tv;
    tv.recordTrace(ast_simple, PyLong_FromLong(10));
    EXPECT_EQ(tv.columnAccesses().front(), 2);

    // check one
    TraceVisitor tv_def;
    tv_def.recordTrace(ast_def, PyLong_FromLong(10));
    EXPECT_EQ(tv_def.columnAccesses().front(), 1);
    tv_def.recordTrace(ast_def, PyLong_FromLong(42));
    EXPECT_EQ(tv_def.columnAccesses().front(), 2);

    // now trace tuple based solution...!
    // case 1: a, b, c, ...
    // case 2: x[0], x[34], ...

    auto t1 = PyTuple_New(6);
    for(int i = 0 ; i < 6; ++i)
        PyTuple_SET_ITEM(t1, i, PyLong_FromLong(i));
    TraceVisitor tv_multiparam;
    tv_multiparam.recordTrace(ast_multiparam, t1);
    auto acc1 = tv_multiparam.columnAccesses();

    ASSERT_EQ(acc1.size(), 6);
    EXPECT_EQ(acc1[0], 1);
    EXPECT_EQ(acc1[1], 0);
    EXPECT_EQ(acc1[2], 1);
    EXPECT_EQ(acc1[3], 0);
    EXPECT_EQ(acc1[4], 0);
    EXPECT_EQ(acc1[5], 1);

    // check now tuple syntax
    auto t2 = PyTuple_New(120);
    for(int i = 0 ; i < 120; ++i)
        PyTuple_SET_ITEM(t2, i, PyLong_FromLong(i));
    TraceVisitor tv_tupledef;
    tv_tupledef.recordTrace(ast_tupledef, t2);
    auto acc2 = tv_tupledef.columnAccesses();

    ASSERT_EQ(acc2.size(), 120);
    for(int i = 0 ; i < 120; ++i) {
        if(i == 1 || i == 2 || i == 45 || i == 10) {
            EXPECT_EQ(acc2[i], 1);
        } else {
            EXPECT_EQ(acc2[i], 0);
        }
    }

    // done.
    python::unlockGIL();
}

// Experiment: what we want to do is check for each flights file, how many delayed parsing optimizations we should apply.
// i.e., let's do that via rule of how often the access is ok.
// this helps with serializing etc.!

namespace tuplex {


}

TEST_F(SamplingTest, FlightsTracing) {
    using namespace std;
    using namespace tuplex;
    using namespace tuplex::codegen;

    string f_path = "../resources/flights_on_time_performance_2019_01.sample.csv";

    // check for larger files
    // sample with start AND end?
    f_path = "/Users/leonhards/Downloads/flights/flights_on_time_performance_2003_06.csv";

    auto content = fileToString(f_path);
    // parse into rows

    auto sample_size = std::min(content.length(), 1024 * 1024 * 2ul);

    // sample first 2MB and last 2MB?
    auto rows = parseRows(content.c_str(), content.c_str() + sample_size, {""});

    if(content.length() > 1024 * 1024 * 2ul + 2) {
        auto offset = content.length() - sample_size - 2;
        auto lastPtr = content.c_str() + offset;
        auto info = findLineStart(lastPtr, sample_size, 2, 110);
        offset += info.offset;
        auto last_rows = parseRows(content.c_str() + offset, content.c_str() + content.length(), {""});
        cout<<"found "<<pluralize(last_rows.size(), "last row")<<endl;

        std::copy(last_rows.begin(), last_rows.end(), std::back_inserter(rows));
    }


    // drop the first row because it's the header...
    auto header = rows.front();
    rows = std::vector<Row>(rows.begin() + 1, rows.end());

    cout<<"parsed "<<rows.size()<<" rows"<<endl;
//    for(auto row : rows) {
//        cout<<row.getRowType().desc()<<endl;
//    }
    // trace some stage of the pipeline now!
    DetectionStats ds;
    ds.detect(rows);

    cout<<"Following columns detected to be constant: "<<ds.constant_column_indices()<<endl;
    // print out which rows are considered constant (and with which values!)
    for(auto idx : ds.constant_column_indices()) {
        cout<<" - "<<header.get(idx).desc()<<": "<<ds.constant_row.get(idx).desc()<<" : "<<ds.constant_row.get(idx).getType().desc()<<endl;
    }
}


TEST_F(SamplingTest, TypeAnnotation) {
    using namespace std;
    using namespace tuplex;

    // parse a tree
//    auto code = "lambda row: (row[0], row[1], row[2])";
    auto code = "def f(row):\n"
                "\tx = row[0]\n"
                "\ty = row[1]\n"
                "\treturn (x, y, 42, row[2])\n";
    UDF udf(code);

    // make constants!
    auto row_type = python::Type::makeTupleType({python::Type::makeConstantValuedType(python::Type::I64, "2003"),
                                                 python::Type::makeConstantValuedType(python::Type::I64, "2003"),
                                                 python::Type::F64});
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, row_type));

    // fetch output

    cout<<"result is: \n"<<udf.getInputSchema().getRowType().desc()<<" -> "<<udf.getOutputSchema().getRowType().desc()<<endl;
}

TEST_F(SamplingTest, FlightsSpecializedVsGeneralValueImputation) {
    using namespace std;
    using namespace tuplex;
    ContextOptions opt = ContextOptions::defaults();
    opt.set("tuplex.executorCount", "0");

    opt.set("tuplex.optimizer.nullValueOptimization", "true"); // this yields exceptions... -> turn off! or perform proper type resampling...

    //opt.set("tuplex.optimizer.nullValueOptimization", "false"); // this also doesn't work -.-

    opt.set("tuplex.resolveWithInterpreterOnly", "true"); // -> this doesn't work with hyper-specialization yet.

    // hyperspecialization setting

    // opt.set("tuplex.backend", "lambda");
    opt.set("tuplex.experimental.hyperspecialization", "true");
    Context ctx(opt);

    // specialize access with columns vs. non-columns
    // --> need to figure this automatically out.

    // i.e. turn off null-value optimization for files or not?

    auto null_based_file = "/Users/leonhards/Downloads/flights/flights_on_time_performance_2003_01.csv";
    auto non_null_based_file = "/Users/leonhards/Downloads/flights/flights_on_time_performance_2013_01.csv"; // do not need to set values...


//    // test with specialization
//    auto& ds = flightPipeline(ctx, null_based_file);
//    cout<<"columns: "<<ds.columns()<<endl;
//    auto v = ds.takeAsVector(5);

auto code = "def fill_in_delays(row):\n"
            "    # want to fill in data for missing carrier_delay, weather delay etc.\n"
            "    # only need to do that prior to 2003/06\n"
            "    \n"
            "    year = row['YEAR']\n"
            "    month = row['MONTH']\n"
            "    arr_delay = row['ARR_DELAY']\n"
            "    \n"
            "    if year == 2003 and month < 6 or year < 2003:\n"
            "        # fill in delay breakdown using model and complex logic\n"
            "        if arr_delay is None:\n"
            "            # stays None, because flight arrived early\n"
            "            # if diverted though, need to add everything to div_arr_delay\n"
            "            return {'year' : year, 'month' : month,\n"
            "                    'day' : row['DAY_OF_MONTH'],\n"
            "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
            "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
            "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
            "                    'dest': row['DEST_AIRPORT_ID'],\n"
            "                    'distance' : row['DISTANCE'],\n"
            "                    'dep_delay' : row['DEP_DELAY'],\n"
            "                    'arr_delay': None,\n"
            "                    'carrier_delay' : None,\n"
            "                    'weather_delay': None,\n"
            "                    'nas_delay' : None,\n"
            "                    'security_delay': None,\n"
            "                    'late_aircraft_delay' : None}\n"
            "        elif arr_delay < 0.:\n"
            "            # stays None, because flight arrived early\n"
            "            # if diverted though, need to add everything to div_arr_delay\n"
            "            return {'year' : year, 'month' : month,\n"
            "                    'day' : row['DAY_OF_MONTH'],\n"
            "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
            "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
            "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
            "                    'dest': row['DEST_AIRPORT_ID'],\n"
            "                    'distance' : row['DISTANCE'],\n"
            "                    'dep_delay' : row['DEP_DELAY'],\n"
            "                    'arr_delay': row['ARR_DELAY'],\n"
            "                    'carrier_delay' : None,\n"
            "                    'weather_delay': None,\n"
            "                    'nas_delay' : None,\n"
            "                    'security_delay': None,\n"
            "                    'late_aircraft_delay' : None}\n"
            "        elif arr_delay < 5.:\n"
            "            # it's an ontime flight, just attribute any delay to the carrier\n"
            "            carrier_delay = arr_delay\n"
            "            # set the rest to 0\n"
            "            # ....\n"
            "            return {'year' : year, 'month' : month,\n"
            "                    'day' : row['DAY_OF_MONTH'],\n"
            "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
            "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
            "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
            "                    'dest': row['DEST_AIRPORT_ID'],\n"
            "                    'distance' : row['DISTANCE'],\n"
            "                    'dep_delay' : row['DEP_DELAY'],\n"
            "                    'arr_delay': row['ARR_DELAY'],\n"
            "                    'carrier_delay' : carrier_delay,\n"
            "                    'weather_delay': None,\n"
            "                    'nas_delay' : None,\n"
            "                    'security_delay': None,\n"
            "                    'late_aircraft_delay' : None}\n"
            "        else:\n"
            "            # use model to determine everything and set into (join with weather data?)\n"
            "            # i.e., extract here a couple additional columns & use them for features etc.!\n"
            "            crs_dep_time = float(row['CRS_DEP_TIME'])\n"
            "            crs_arr_time = float(row['CRS_ARR_TIME'])\n"
            "            crs_elapsed_time = float(row['CRS_ELAPSED_TIME'])\n"
            "            carrier_delay = 1024 + 2.7 * crs_dep_time - 0.2 * crs_elapsed_time\n"
            "            weather_delay = 2000 + 0.09 * carrier_delay * (carrier_delay - 10.0)\n"
            "            nas_delay = 3600 * crs_dep_time / 10.0\n"
            "            security_delay = 7200 / crs_dep_time\n"
            "            late_aircraft_delay = (20 + crs_arr_time) / (1.0 + crs_dep_time)\n"
            "            return {'year' : year, 'month' : month,\n"
            "                    'day' : row['DAY_OF_MONTH'],\n"
            "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
            "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
            "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
            "                    'dest': row['DEST_AIRPORT_ID'],\n"
            "                    'distance' : row['DISTANCE'],\n"
            "                    'dep_delay' : row['DEP_DELAY'],\n"
            "                    'arr_delay': row['ARR_DELAY'],\n"
            "                    'carrier_delay' : carrier_delay,\n"
            "                    'weather_delay': weather_delay,\n"
            "                    'nas_delay' : nas_delay,\n"
            "                    'security_delay': security_delay,\n"
            "                    'late_aircraft_delay' : late_aircraft_delay}\n"
            "    else:\n"
            "        # just return it as is\n"
            "        return {'year' : year, 'month' : month,\n"
            "                'day' : row['DAY_OF_MONTH'],\n"
            "                'carrier': row['OP_UNIQUE_CARRIER'],\n"
            "                'flightno' : row['OP_CARRIER_FL_NUM'],\n"
            "                'origin': row['ORIGIN_AIRPORT_ID'],\n"
            "                'dest': row['DEST_AIRPORT_ID'],\n"
            "                'distance' : row['DISTANCE'],\n"
            "                'dep_delay' : row['DEP_DELAY'],\n"
            "                'arr_delay': row['ARR_DELAY'],\n"
            "                'carrier_delay' : row['CARRIER_DELAY'],\n"
            "                'weather_delay':row['WEATHER_DELAY'],\n"
            "                'nas_delay' : row['NAS_DELAY'],\n"
            "                'security_delay': row['SECURITY_DELAY'],\n"
            "                'late_aircraft_delay' : row['LATE_AIRCRAFT_DELAY']}";

    // @TODO: test fails for mapColumn - why?
    // i.e. the last return is considered to be {str, unknown} ??

    // value imputing pipeline (super simple!)
//    auto& ds = ctx.csv(null_based_file).map(UDF(code));

    // this file here should get folded!
    // => i.e. no expensive code is required!
//    auto& ds = ctx.csv(non_null_based_file).map(UDF(code));
    auto& ds = ctx.csv(null_based_file).map(UDF(code));

    ds.tocsv("test_output.csv");

    //ds.show(5);
}

TEST_F(SamplingTest, S3FileWrite) {

    using namespace std;
    using namespace tuplex;

    // create a large buffer
    size_t large_buf_size = 80 * 1024 * 1024;
    auto large_buf = new uint8_t[large_buf_size];
    memset(reinterpret_cast<void *>(large_buf_size), 42, large_buf_size);

    // test uri
    auto test_uri = URI("s3://tuplex-leonhard/experiments/s3write/test.bin");
    auto outputURI = test_uri;
    auto vfs = VirtualFileSystem::fromURI(outputURI);
    auto mode = VirtualFileMode::VFS_OVERWRITE | VirtualFileMode::VFS_WRITE;
    mode |= VirtualFileMode::VFS_TEXTMODE;

    auto file = tuplex::VirtualFileSystem::open_file(outputURI, mode);
    if(!file)
        throw std::runtime_error("could not open " + outputURI.toPath() + " to write output");

    file->write(large_buf, 1024); // write tiny header
    file->write(large_buf, large_buf_size);
    file->close();

    delete [] large_buf;
}

TEST_F(SamplingTest, UpcastFix) {
    //auto from = "(i64,i64,i64,str,i64,i64,i64,f64,f64,f64,Option[f64],Option[f64],Option[f64],_Constant[Option[f64],value=0.00000],Option[f64])";
    //auto to = "(_Constant[i64,value=2003],_Constant[i64,value=10],i64,_Constant[str,value='AA'],i64,i64,i64,f64,f64,f64,Option[f64],Option[f64],Option[f64],Option[f64],Option[f64])
    using namespace tuplex;

    // upcast option?
    auto f64opt = python::Type::makeOptionType(python::Type::F64);
    EXPECT_TRUE(python::canUpcastType(python::Type::makeConstantValuedType(f64opt, "0.000"), f64opt));
}

TEST_F(SamplingTest, FlightsLambdaVersion) {
    using namespace std;
    using namespace tuplex;

    // for testing purposes, store here the root path to the flights data (simple ifdef)
#ifdef __APPLE__
    // leonhards macbook
    string flights_root = "/Users/leonhards/Downloads/flights/";
#else
    // BBSN00
    string flights_root = "/hot/data/flights_all/";
#endif

    string s3_flights_root = "s3://tuplex-public/data/flights_all/";

    auto code = "def fill_in_delays(row):\n"
                "    # want to fill in data for missing carrier_delay, weather delay etc.\n"
                "    # only need to do that prior to 2003/06\n"
                "    \n"
                "    year = row['YEAR']\n"
                "    month = row['MONTH']\n"
                "    arr_delay = row['ARR_DELAY']\n"
                "    \n"
                "    if year == 2003 and month < 6 or year < 2003:\n"
                "        # fill in delay breakdown using model and complex logic\n"
                "        if arr_delay is None:\n"
                "            # stays None, because flight arrived early\n"
                "            # if diverted though, need to add everything to div_arr_delay\n"
                "            return {'year' : year, 'month' : month,\n"
                "                    'day' : row['DAY_OF_MONTH'],\n"
                "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                "                    'dest': row['DEST_AIRPORT_ID'],\n"
                "                    'distance' : row['DISTANCE'],\n"
                "                    'dep_delay' : row['DEP_DELAY'],\n"
                "                    'arr_delay': None,\n"
                "                    'carrier_delay' : None,\n"
                "                    'weather_delay': None,\n"
                "                    'nas_delay' : None,\n"
                "                    'security_delay': None,\n"
                "                    'late_aircraft_delay' : None}\n"
                "        elif arr_delay < 0.:\n"
                "            # stays None, because flight arrived early\n"
                "            # if diverted though, need to add everything to div_arr_delay\n"
                "            return {'year' : year, 'month' : month,\n"
                "                    'day' : row['DAY_OF_MONTH'],\n"
                "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                "                    'dest': row['DEST_AIRPORT_ID'],\n"
                "                    'distance' : row['DISTANCE'],\n"
                "                    'dep_delay' : row['DEP_DELAY'],\n"
                "                    'arr_delay': row['ARR_DELAY'],\n"
                "                    'carrier_delay' : None,\n"
                "                    'weather_delay': None,\n"
                "                    'nas_delay' : None,\n"
                "                    'security_delay': None,\n"
                "                    'late_aircraft_delay' : None}\n"
                "        elif arr_delay < 5.:\n"
                "            # it's an ontime flight, just attribute any delay to the carrier\n"
                "            carrier_delay = arr_delay\n"
                "            # set the rest to 0\n"
                "            # ....\n"
                "            return {'year' : year, 'month' : month,\n"
                "                    'day' : row['DAY_OF_MONTH'],\n"
                "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                "                    'dest': row['DEST_AIRPORT_ID'],\n"
                "                    'distance' : row['DISTANCE'],\n"
                "                    'dep_delay' : row['DEP_DELAY'],\n"
                "                    'arr_delay': row['ARR_DELAY'],\n"
                "                    'carrier_delay' : carrier_delay,\n"
                "                    'weather_delay': None,\n"
                "                    'nas_delay' : None,\n"
                "                    'security_delay': None,\n"
                "                    'late_aircraft_delay' : None}\n"
                "        else:\n"
                "            # use model to determine everything and set into (join with weather data?)\n"
                "            # i.e., extract here a couple additional columns & use them for features etc.!\n"
                "            crs_dep_time = float(row['CRS_DEP_TIME'])\n"
                "            crs_arr_time = float(row['CRS_ARR_TIME'])\n"
                "            crs_elapsed_time = float(row['CRS_ELAPSED_TIME'])\n"
                "            carrier_delay = 1024 + 2.7 * crs_dep_time - 0.2 * crs_elapsed_time\n"
                "            weather_delay = 2000 + 0.09 * carrier_delay * (carrier_delay - 10.0)\n"
                "            nas_delay = 3600 * crs_dep_time / 10.0\n"
                "            security_delay = 7200 / crs_dep_time\n"
                "            late_aircraft_delay = (20 + crs_arr_time) / (1.0 + crs_dep_time)\n"
                "            return {'year' : year, 'month' : month,\n"
                "                    'day' : row['DAY_OF_MONTH'],\n"
                "                    'carrier': row['OP_UNIQUE_CARRIER'],\n"
                "                    'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                "                    'origin': row['ORIGIN_AIRPORT_ID'],\n"
                "                    'dest': row['DEST_AIRPORT_ID'],\n"
                "                    'distance' : row['DISTANCE'],\n"
                "                    'dep_delay' : row['DEP_DELAY'],\n"
                "                    'arr_delay': row['ARR_DELAY'],\n"
                "                    'carrier_delay' : carrier_delay,\n"
                "                    'weather_delay': weather_delay,\n"
                "                    'nas_delay' : nas_delay,\n"
                "                    'security_delay': security_delay,\n"
                "                    'late_aircraft_delay' : late_aircraft_delay}\n"
                "    else:\n"
                "        # just return it as is\n"
                "        return {'year' : year, 'month' : month,\n"
                "                'day' : row['DAY_OF_MONTH'],\n"
                "                'carrier': row['OP_UNIQUE_CARRIER'],\n"
                "                'flightno' : row['OP_CARRIER_FL_NUM'],\n"
                "                'origin': row['ORIGIN_AIRPORT_ID'],\n"
                "                'dest': row['DEST_AIRPORT_ID'],\n"
                "                'distance' : row['DISTANCE'],\n"
                "                'dep_delay' : row['DEP_DELAY'],\n"
                "                'arr_delay': row['ARR_DELAY'],\n"
                "                'carrier_delay' : row['CARRIER_DELAY'],\n"
                "                'weather_delay':row['WEATHER_DELAY'],\n"
                "                'nas_delay' : row['NAS_DELAY'],\n"
                "                'security_delay': row['SECURITY_DELAY'],\n"
                "                'late_aircraft_delay' : row['LATE_AIRCRAFT_DELAY']}";

    // @TODO: test fails for mapColumn - why?
    // i.e. the last return is considered to be {str, unknown} ??

    // value imputing pipeline (super simple!)
//    auto& ds = ctx.csv(null_based_file).map(UDF(code));

    // this file here should get folded!
    // => i.e. no expensive code is required!
//    auto& ds = ctx.csv(non_null_based_file).map(UDF(code));

//    // output URI
//    // ==> important!
//    std::string s3_output = "s3://tuplex-leonhard/experiments/flights_hyper";
//    //auto& ds = ctx.csv(null_based_file).map(UDF(code)); // fix: /aws/lambda/tuplex-lambda-runner?
//    auto& ds = ctx.csv(non_null_based_file).map(UDF(code)); // fix: /aws/lambda/tuplex-lambda-runner?
//
//    ds.tocsv(s3_output);

    // BENCHMARK HERE...!

//    string input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_2003_10.csv";
    string input_pattern = "flights_on_time_performance_2003_10.csv";

    // 2003 test pattern:
    //input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_2003_*.csv";

    // Lambda settings (i.e. 10G and 2 threads?)
    size_t numLambdaThreads = 2;
    size_t lambdaSize = 10000;

    std::string s3_output = "s3://tuplex-leonhard/experiments/flights_hyper";
//
//    // test:
//    input_pattern = "/Users/leonhards/Downloads/flights/flights_on_time_performance_2003_10.csv";
    bool use_lambda = false;
    if(!use_lambda)
        input_pattern = flights_root + "flights_on_time_performance_2003_*.csv";

    // test all files through for issues...!
    // this file has issues => i.e., it triggers fallback ALWAYS for hyper-specialization
    //input_pattern = "/Users/leonhards/Downloads/flights/flights_on_time_performance_2003_08.csv,/Users/leonhards/Downloads/flights/flights_on_time_performance_2003_09.csv";

//    input_pattern = "../resources/hyperspecialization/flights_2003_06.sample.csv";

    // input_pattern = flights_root + "flights_on_time_performance_2003_06.csv";

    std::cout<<"HyperSpecialization Benchmark:\n------------"<<std::endl;
    Timer timer;

    // running first query with hyper specialization on.
    ContextOptions opt = ContextOptions::defaults();
    opt.set("tuplex.executorCount", "0");
    opt.set("tuplex.optimizer.nullValueOptimization", "true"); // this yields exceptions... -> turn off! or perform proper type resampling...
    opt.set("tuplex.resolveWithInterpreterOnly", "true"); // -> this doesn't work with hyper-specialization yet.
    opt.set("tuplex.resolveWithInterpreterOnly", "false"); // -> this doesn't work with hyper-specialization yet.

    // hyperspecialization setting
    if(use_lambda) {
        opt.set("tuplex.backend", "lambda");
        opt.set("tuplex.aws.lambdaMemory", std::to_string(lambdaSize));
        opt.set("tuplex.aws.lambdaThreads", std::to_string(numLambdaThreads));
        opt.set("tuplex.aws.scratchDir", "s3://tuplex-leonhard/scratch/flights-exp");
    }
    opt.set("tuplex.experimental.hyperspecialization", "true");
    Context ctx(opt);

    // could also be interesting to have some sort of figure showing different specializations (i.e. column pushdown)

    // run same query too

    // check for each file in non-lambda mode
    if(!use_lambda) {
        timer.reset();
        auto vfs = VirtualFileSystem::fromURI("file://");
        auto files = vfs.globAll(input_pattern);
        std::sort(files.begin(), files.end(), [](const URI& a, const URI& b){
            return a.toString() < b.toString();
        });
        for(const auto& path : files) {
            std::cout<<"checking for file "<<path<<std::endl;
            auto output_path = "local_hyper" + path.toString().substr(path.toString().rfind('/') + 1) + ".csv";
            ctx.csv(path.toString()).map(UDF(code)).tocsv(output_path);
        }
    } else {
        ctx.csv(input_pattern).map(UDF(code)).tocsv(s3_output +"_hyper");
    }
#ifndef NDEBUG
    displayExceptions(std::cout, true);
#endif
    double hyperQueryTime = timer.time();
    std::cout<<"Hyper query done in "<<hyperQueryTime<<"s"<<std::endl;
    // -----------------------------------------------------------------------------
    std:cout<<"**************************************"<<std::endl;
    std::cout<<"Running query with hyper-opt off"<<std::endl;
    // running query with hyper specialization off.
    timer.reset();
    ContextOptions opt_general = ContextOptions::defaults();
    opt_general.set("tuplex.executorCount", "0");
    opt_general.set("tuplex.optimizer.nullValueOptimization", "true"); // this yields exceptions... -> turn off! or perform proper type resampling...
    opt_general.set("tuplex.resolveWithInterpreterOnly", "true"); // -> this doesn't work with hyper-specialization yet.
    opt_general.set("tuplex.resolveWithInterpreterOnly", "false"); // -> this doesn't work with hyper-specialization yet.
    if(use_lambda) {
        opt_general.set("tuplex.backend", "lambda");
        opt_general.set("tuplex.aws.lambdaMemory", std::to_string(lambdaSize));
        opt_general.set("tuplex.aws.lambdaThreads", std::to_string(numLambdaThreads));
        opt_general.set("tuplex.aws.scratchDir", "s3://tuplex-leonhard/scratch/flights-exp-general");
    }
    opt_general.set("tuplex.experimental.hyperspecialization", "false"); // turn off !!!
    Context ctx_general(opt_general);

    // run same query too
    // check again for each file in general mode.
    if(!use_lambda) {

        // use this to test settings for general context
        //        auto vfs = VirtualFileSystem::fromURI("file://");
        //        auto files = vfs.globAll(input_pattern);
        //        for(const auto& path : files) {
        //            std::cout<<"checking for file "<<path<<std::endl;
        //            auto output_path = "local_general" + path.toString().substr(path.toString().rfind('/') + 1) + ".csv";
        //            ctx.csv(path.toString()).map(UDF(code)).tocsv(output_path);
        //            //ctx_general.csv(path.toString()).map(UDF(code)).tocsv("test_local_general.csv");
        //        }

        std::cout<<"running again for all files"<<std::endl;
        timer.reset();
        ctx_general.csv(input_pattern).map(UDF(code)).tocsv("all_files_general.csv"); // fix: /aws/lambda/tuplex-lambda-runner?

    } else
    ctx_general.csv(input_pattern).map(UDF(code)).tocsv(s3_output +"_general"); // fix: /aws/lambda/tuplex-lambda-runner?
#ifndef NDEBUG
    displayExceptions(std::cout, true);
#endif
    double generalQueryTime = timer.time();
    std::cout<<"General query done in "<<generalQueryTime<<"s"<<std::endl;

    // print out results as json
    std::cout<<"{\"general_total_time\":"<<generalQueryTime<<",\"hyper_total_time\":"<<hyperQueryTime<<"}"<<std::endl;
    //ds.show(5);


    // cmake -DCMAKE_BUILD_TYPE=Release -DPYTHON3_VERSION=3.6 -DLLVM_ROOT_DIR=/usr/lib/llvm-9 .. && make -j 128 testcore && ./dist/bin/testcore --gtest_filter=SamplingTest.FlightsLambdaVersion
}