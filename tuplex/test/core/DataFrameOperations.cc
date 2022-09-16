//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include "TestUtils.h"
#include <fstream>
#include <Context.h>
#include <DataSet.h>

class CSVDataFrameTest : public TuplexTest {
protected:

    std::unique_ptr<tuplex::Context> context;
    PyThreadState *saveState;

    void SetUp() override {
        TuplexTest::SetUp();
        using namespace std;

        // write test file
        ofstream ofs(testName + ".csv");

        ofs<<"firstname,surname,income\n";
        ofs<<"Manuel,Neuer,48000\n";
        ofs<<"Miroslav,Klose,52000\n";
        ofs.close();

        python::initInterpreter();

        python::unlockGIL();
        // release GIL

        context.reset(new tuplex::Context(microTestOptions()));
    }

    void TearDown() override {

        // acquire GIL
        python::lockGIL();

        // important to get GIL for this
        python::closeInterpreter();

        // remove test file
        auto fName = testName + ".csv";
        remove(fName.c_str());

    }

    tuplex::DataSet& csvfile() { return context->csv(testName + ".csv"); }

};

class DataFrameTest : public PyTest {};

TEST_F(DataFrameTest, PrefixNullTest) {
    // create test file containing 0s
    using namespace tuplex;
    using namespace std;

    URI uri(testName + ".txt");
    stringToFile(uri, "0\n000\n0000\n00\n0");
    auto confA = microTestOptions();
    auto confB = microTestOptions();
    // this should also work...
    confB.set("tuplex.optimizer.generateParser", "true");
    for(const auto& conf : vector<ContextOptions>{confA, confB}) {
        Context c(conf);
        auto v = c.csv(uri.toPath(), std::vector<std::string>(),
                       false, ',', '"',
                       {std::string("0")}, {{0, python::Type::makeOptionType(python::Type::STRING)}})
                .collectAsVector();

        vector<string> ref{"(None,)", "('000',)", "('0000',)", "('00',)", "(None,)"};
        ASSERT_EQ(v.size(), ref.size());
        for(int i = 0; i < ref.size(); ++i)
            EXPECT_EQ(ref[i], v[i].toPythonString());
    }
}

TEST_F(DataFrameTest, PushdownWithSpecialization) {
    // use all basic operators in one query to make sure the specialization (rewriting works)
    // create test file containing 0s
    using namespace tuplex;
    using namespace std;

    URI uri(testName + ".txt");
    stringToFile(uri, "A,B,C\n0,a,c\n000,a,c\n0000,a,c\n00,a,c\nN/A,a,c");
    auto conf = microTestOptions();
//    conf.set("tuplex.optimizer.projectionPushdown", "true");
    conf.set("tuplex.csv.selectionPushdown", "true");
    conf.set("tuplex.optimizer.nullValueOptimization", "true");

    Context c(conf);

    // variation of pipeline
    //     auto rows = c.csv(uri.toPath(), {}, true, ',', '"', {std::string("N/A")})
    //                             .withColumn("C", UDF("lambda x: x['A'] + 1")) // this is tricky, because it overrides the column C but doesn't mean this column needs to get parsed!
    //                             .collectAsVector();


    // pipeline I to test
    {
        auto rows = c.csv(uri.toPath(), {}, true, ',', '"', {std::string("N/A")})
                .withColumn("C", UDF("lambda x: x['C'] + '1'")) // this is tricky, because it overrides the column C but doesn't mean this column needs to get parsed!
                .map(UDF("lambda x: {'A': x['A'], 'B': x['B']}"))
                .collectAsVector();
        EXPECT_EQ(rows.size(), 5);
    }

     // pipeline II to test
    {
        auto rows = c.csv(uri.toPath(), {}, true, ',', '"', {std::string("N/A")})
                .map(UDF("lambda x: {'A': x['A'], 'B': x['B']}"))
                .withColumn("C", UDF("lambda x: x['A'] + 1")) // this is tricky, because it overrides the column C but doesn't mean this column needs to get parsed!
                .collectAsVector();
        EXPECT_EQ(rows.size(), 4); // -> type error on the null row.
    }

    // pipeline III to test
    {
         auto rows = c.csv(uri.toPath(), {}, true, ',', '"', {std::string("N/A")})
                                 .withColumn("C", UDF("lambda x: x['A'] + 1")) // this is tricky, because it overrides the column C but doesn't mean this column needs to get parsed!
                                 .selectColumns({"C"})
                                 .collectAsVector();
        EXPECT_EQ(rows.size(), 4); // -> type error on the null row.
    }

    // pipeline IV to test
    {
        auto rows = c.csv(uri.toPath(), {}, true, ',', '"', {std::string("N/A")})
                .mapColumn("A", UDF("lambda x: x + 1"))
                .selectColumns(std::vector<std::string>{"A", "C"})
                .collectAsVector();
        EXPECT_EQ(rows.size(), 4); // -> type error on the null row.
    }

    // pipeline V to test
    {
        auto rows = c.csv(uri.toPath(), {}, true, ',', '"', {std::string("N/A")})
                .mapColumn("A", UDF("lambda x: x + 1"))
                .filter(UDF("lambda r: r['A'] >= 1"))
                .selectColumns(std::vector<std::string>{"A", "C"})
                .collectAsVector();
        EXPECT_EQ(rows.size(), 4); // -> type error on the null row.
    }

    // pipeline VI to test
    {
        auto rows = c.csv(uri.toPath(), {}, true, ',', '"', {std::string("N/A")})
                .mapColumn("A", UDF("lambda x: x + 1"))
                .resolve(ExceptionCode::TYPEERROR, UDF("lambda x: 0"))
                .filter(UDF("lambda r: r['A'] >= 1"))
                .selectColumns(std::vector<std::string>{"A", "C"})
                .collectAsVector();
        EXPECT_EQ(rows.size(), 4); // -> type error on the null row.
    }
}

TEST_F(CSVDataFrameTest, SimpleMapColumnI) {
    using namespace tuplex;

    auto v = csvfile().mapColumn("firstname", UDF("lambda fn: fn[0]"))
                      .mapColumn("income", UDF("lambda x: x / 1000"))
                      .collectAsVector();
    ASSERT_EQ(v.size(), 2);
    printRows(v);
    EXPECT_EQ(v[0].toPythonString(), Row("M", "Neuer", 48.0).toPythonString());
    EXPECT_EQ(v[1].toPythonString(), Row("M", "Klose", 52.0).toPythonString());
}

TEST_F(CSVDataFrameTest, HeaderlessStringFile) {
    using namespace tuplex;
    using namespace std;

    auto conf = testOptions();
    // deactivate optimizers for now
    conf.set("tuplex.optimizer.filterPushdown", "false");
    conf.set("tuplex.csv.selectionPushdown", "false");
    conf.set("tuplex.useLLVMOptimizer", "false");
    conf.set("tuplex.executorCount", "0");
    conf.set("tuplex.optimizer.generateParser", "false");
    Context c(conf);
    auto path = URI(testName + ".txt");
    stringToFile(path, "a\nb");

    vector<string> ref{"a", "b"};
    auto res = c.csv(path.toPath(), std::vector<std::string>{}, false).collectAsVector();
    ASSERT_EQ(res.size(), ref.size());
    for(int i = 0; i < ref.size(); ++i)
        EXPECT_EQ(ref[i], res[i].getString(0));
}

TEST_F(CSVDataFrameTest, TSVFile) {
    using namespace tuplex;
    using namespace std;

    // write file
    auto data = "4\tFAST ETL!\n"
                "7\tFAST ETL!";

    ofstream ofs(testName + ".tsv");
    ofs<<data<<endl;

    Context c(microTestOptions());

    auto v = c.csv(testName + ".tsv").collectAsVector();

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), Row(4, "FAST ETL!").toPythonString());
    EXPECT_EQ(v[1].toPythonString(), Row(7, "FAST ETL!").toPythonString());

    auto fName = testName + ".tsv";
    remove(fName.c_str());
}

TEST_F(CSVDataFrameTest, ReadHeaderLessFile) {
    using namespace tuplex;
    using namespace std;

    auto ref = "+---+\n"
               "| a |\n"
               "+---+\n"
               "| 4 |\n"
               "+---+\n"
               "| 7 |\n"
               "+---+\n";

    // write file
    auto data = "4\tFAST ETL!\n"
                "7\tFAST ETL!";

    ofstream ofs(testName + ".tsv");
    ofs<<data<<endl;

    Context c(microTestOptions());

    auto v = c.csv(testName + ".tsv", vector<string>{"a", "b"}).map(UDF("lambda x: x['a']")).collectAsVector();

    for(const auto& r : v)
        std::cout<<r.toPythonString()<<std::endl;

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), Row(4).toPythonString());
    EXPECT_EQ(v[1].toPythonString(), Row(7).toPythonString());

    // now using show, to make sure column was captured adequately
    std::stringstream ss;
    c.csv(testName + ".tsv", vector<string>{"a", "b"}).selectColumns({"a"}).show(100, ss);

    std::cout<<ss.str()<<std::endl;

    EXPECT_EQ(ss.str(), ref);

    auto fName = testName + ".tsv";
    remove(fName.c_str());
}

// @TODO: tests for header conflicting with columns, etc.
// i..e file a,b,c then give it columns a,b,c and header=False to disable header detection.
TEST_F(DataFrameTest, CSVConflictingColumns) {
    using namespace tuplex;
    using namespace std;

    // write file
    auto data = "a,b,2\n"
                "c,d,3";

    ofstream ofs(testName + ".csv");
    ofs<<data<<endl;

    Context c(microTestOptions());

    auto v = c.csv(testName + ".csv", vector<string>{"a", "b", "c"}, false, ',')
              .map(UDF("lambda x: (x['c'] + 1, x['a'])")).collectAsVector();

    for(auto r : v)
        std::cout<<r.toPythonString()<<std::endl;

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].toPythonString(), Row(3, "a").toPythonString());
    EXPECT_EQ(v[1].toPythonString(), Row(4, "c").toPythonString());

    auto fName = testName + ".csv";
    remove(fName.c_str());
}

// explicit schema test

TEST_F(DataFrameTest, SimpleWithColumnI) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto v = c.parallelize({Row(10), Row(20), Row(3), Row(4)}).withColumn("newcol", UDF("lambda x: 2 * x")).collectAsVector();

    ASSERT_EQ(v.size(), 4);

    printRows(v);

    EXPECT_EQ(v[0], Row(10, 20));
    EXPECT_EQ(v[1], Row(20, 40));
    EXPECT_EQ(v[2], Row(3, 6));
    EXPECT_EQ(v[3], Row(4, 8));
}


// c.parallelize([1, 2, 3], columns=['A']).mapColumn('A', lambda x: x + 1).collect()
TEST_F(DataFrameTest, SimpleMapColumnII) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto v = c.parallelize({Row(1), Row(2), Row(3)}, {"A"}).mapColumn("A", UDF("lambda x: x + 1")).collectAsVector();

    ASSERT_EQ(v.size(), 3);


    printRows(v);

    EXPECT_EQ(v[0], Row(2));
    EXPECT_EQ(v[1], Row(3));
    EXPECT_EQ(v[2], Row(4));
}


TEST_F(DataFrameTest, WithColumnII) {
    using namespace tuplex;

    Context c(microTestOptions());
    auto v = c.parallelize({Row(1, 2), Row(3, 2)})
            .withColumn("newcol", UDF("lambda a, b: (a + b)/10")).collectAsVector();

    ASSERT_EQ(v.size(), 2);

    printRows(v);

    EXPECT_EQ(v[0], Row(1, 2, 3/10.));
    EXPECT_EQ(v[1], Row(3, 2, 5/10.));
}

TEST_F(DataFrameTest, WithColumnIII_singleIndexable) {
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());
    auto v = c.parallelize({Row("abc"), Row("xyz")}, vector<string>{"A"}).withColumn("X", UDF("lambda x: 'ab' + x[2]")).collectAsVector();

    ASSERT_EQ(v.size(), 2);

    printRows(v);

    EXPECT_EQ(v[0].toPythonString(), "('abc','abc')");
    EXPECT_EQ(v[1].toPythonString(), "('xyz','abz')");
}

TEST_F(DataFrameTest, ToCSVFile) {

    using namespace tuplex;

    Context c(microTestOptions());

    c.parallelize({Row(10, 42.34, "hello", "hello \"world!\""),
                   Row(11, 43.34, "hello", "abc")})
                   .tocsv(testName + ".csv");

    // check that file was written locally
    auto fName = testName + ".part0.csv";
    FILE *file = fopen(fName.c_str(), "r");
    ASSERT_TRUE(file);

    fseek(file, 0L, SEEK_END);
    auto size = ftell(file);
    fseek(file, 0L, SEEK_SET);

    char *buf = new char[size+1];
    memset(buf, 0, size + 1);
    fread(buf, size, 1, file);

    std::string stored = buf;

    char ref_buf[2048]; memset(ref_buf, 0, 2048);
    snprintf(ref_buf, 2048, "%d,%.8f,%s,%s\n%d,%.8f,%s,%s\n",
            10,42.34,"hello","\"hello \"\"world!\"\"\"",11,43.34,"hello", "abc");

    std::string ref(ref_buf);
    EXPECT_EQ(stored, ref);

    fclose(file);
}

TEST_F(CSVDataFrameTest, FlightCSV) {
    using namespace tuplex;
    using namespace std;

    // fun error: 9E got detected as float in previous versions, i.e. fix type sniffer there.
    EXPECT_FALSE(isFloatString("9E"));

   // file:
   // "FL_DATE","UNIQUE_CARRIER","AIRLINE_ID","TAIL_NUM","FL_NUM","ORIGIN_AIRPORT_ID",
   // 2019-01-01,"9E",20363,"N676CA","5246",13487,
   // 2019-01-01,"9E",20363,"N391CA","5249",10397,
   // 2019-01-01,"9E",20363,"N391CA","5249",14098,
   // 2019-01-01,"9E",20363,"N326PQ","5250",13487,
   // ==> special thing about this file is, that it has an empty column at the rear

   Context c(microTestOptions());

   auto res = c.csv("../resources/flight_mini.csv").collectAsVector();

   for(auto r : res)
       cout<<r.toPythonString()<<endl;

   ASSERT_EQ(res.size(), 4);
   //EXPECT_EQ(res[0], Row("2019-01-01", "9E", 20363, "N676CA", 5246, 13487, ""));
   EXPECT_EQ(res[0].toPythonString(), "('2019-01-01','9E',20363,'N676CA',5246,13487,None)");
}

TEST_F(CSVDataFrameTest, Nulls) {
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    c.csv("../resources/quotednulls.csv").show();
}

TEST_F(DataFrameTest, Rename) {
    // test renaming of columns!!!

    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());
    stringstream ss;
    c.parallelize({Row(12, "hello"), Row(23, "test")}, {"A", "B"})
     .renameColumn("A", "C").show(-1, ss);

    cout<<ss.str()<<endl;

    // find string!
    EXPECT_NE(std::string::npos, ss.str().find("C"));
}

TEST_F(DataFrameTest, FolderOutput) {
    // test whether output to folder can be forced...
    using namespace tuplex;
    Context c(microTestOptions());

    c.parallelize({Row(10, 20), Row(10, 40)})
     .map(UDF("lambda a, b: {'A' :a +1, 'B' : b}"))
     .tocsv(URI(testName + "_output")); // no extension, so folder assumed.

    // check folder output/ exists!
    auto uri = URI(testName + "_output/");
    ASSERT_TRUE(uri.exists());

    ASSERT_TRUE(URI(testName + "_output/part0.csv").exists());

    // load file from disk
    auto content = fileToString(URI(testName + "_output/part0.csv"));
    EXPECT_EQ(content, "A,B\n11,20\n11,40\n");
}

TEST_F(DataFrameTest, RenameColumns) {
    using namespace tuplex;
    Context c(microTestOptions());

    // rename test, position based:
    auto& ds = c.parallelize({Row(1, 2), Row(3, 4)});
    auto cols_before_rename = ds.columns();

    EXPECT_EQ(cols_before_rename.size(), 0); // no columns defined

    // now rename columns
    auto& ds2 = ds.renameColumn(0, "first");
    ASSERT_EQ(ds2.columns().size(), 2);
    EXPECT_EQ(ds2.columns()[0], "first");
    EXPECT_EQ(ds2.columns()[1], "");
    auto& ds3 = ds2.renameColumn(1, "second");
    ASSERT_EQ(ds3.columns().size(), 2);
    EXPECT_EQ(ds3.columns()[0], "first");
    EXPECT_EQ(ds3.columns()[1], "second");

    // check now fuzzy matching
    auto& err_ds = ds3.renameColumn("secund", "+1");
    EXPECT_TRUE(err_ds.isError());
}

TEST_F(DataFrameTest, IsKeywordAndFilter) {
    // Following causes a bug (https://github.com/tuplex/tuplex/issues/54), this test is to fix it.
    // c = Context()
    // c.parallelize([1, 2, 3]).filter(lambda x: x is 2).collect()

    using namespace tuplex;
    Context c(microTestOptions());

    // rename test, position based:
    auto& ds = c.parallelize({Row(1), Row(2), Row(3)}).filter(UDF("lambda x: x is 2"));
    ASSERT_FALSE(ds.isError());

    // for integers -5 <= x <= 256 python is weird, is acts like equality!
    auto v = ds.collectAsVector();
    ASSERT_EQ(v.size(), 1);
    EXPECT_EQ(v.front().getInt(0), 2);

    // also check here floats to be sure the filter doesn't screw things up.
    ds = c.parallelize({Row(1.0), Row(2.0), Row(3.0)}).filter(UDF("lambda x: x is 2.0"));
    ASSERT_FALSE(ds.isError());
    v = ds.collectAsVector();
    ASSERT_EQ(v.size(), 0);
}

TEST_F(DataFrameTest, FastPreview) {
    using namespace tuplex;
    Context c(microTestOptions());

    // show on a couple rows should be faster by simply using the sample as input...
    // --> note: should also work with fallback!

    // test for CSV, TEXT, ORC
    // TODO.

    auto res = c.csv("../resources/flights_on_time_performance_2019_01.sample.csv")
     .selectColumns({"DAY_OF_MONTH", "MONTH", "YEAR", "ORIGIN", "DEST", "OP_UNIQUE_CARRIER"})
     .takeAsVector(5);
    ASSERT_EQ(res.size(), 5);

    auto textURI = testName + "_test.txt";

    stringToFile(textURI, "A\nB\nC\nD\nE\nF\nG\nH\nI\nJ");
    res = c.text(textURI)
    .map(UDF("lambda x: x.lower()"))
    .takeAsVector(5);
    ASSERT_EQ(res.size(), 5);

    // JITCompiled CSV source
    auto opt_jit = microTestOptions();
    opt_jit.set("tuplex.optimizer.generateParser", "true");
    Context c_jit(opt_jit);

    res = c_jit.csv("../resources/flights_on_time_performance_2019_01.sample.csv")
            .selectColumns({"DAY_OF_MONTH", "MONTH", "YEAR", "ORIGIN", "DEST", "OP_UNIQUE_CARRIER"})
            .takeAsVector(5);
    ASSERT_EQ(res.size(), 5);
}