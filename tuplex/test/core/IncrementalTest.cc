//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/1/2021                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <Context.h>
#include "TestUtils.h"

class IncrementalTest : public PyTest {
protected:

    void SetUp() override {
        PyTest::SetUp();

        using namespace tuplex;
        auto vfs = VirtualFileSystem::fromURI(".");
        vfs.remove(testName);
        auto err = vfs.create_dir(testName);
        ASSERT_TRUE(err == VirtualFileSystemStatus::VFS_OK);
    }

    void TearDown() override {
        PyTest::TearDown();

        using namespace tuplex;
        auto vfs = VirtualFileSystem::fromURI(".");
        vfs.remove(testName);
    }
};

TEST_F(IncrementalTest, TwoJoins) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    Context c(opts);

    auto writeURI = URI(testName + "/" + testName + ".csv");
    auto readURI = URI(testName + "/" + testName + ".*.csv");

    auto ds1 = c.parallelize({Row("A", 1),
                              Row("B", 2),
                              Row("C", 3)},
                             vector<string>{"a", "b"});
    auto ds2 = c.parallelize({Row("A", true),
                              Row("B", false),
                              Row("C", true)},
                             vector<string>{"c", "d"});
    auto ds3 = c.parallelize({Row("A", 1.1),
                              Row("B", 2.2),
                              Row("C", 3.3)},
                             vector<string>{"e", "f"});

    ds3.join(ds1.join(ds2, string("a"), string("c")), string("e"), string("a")).tocsv(writeURI);
    ASSERT_TRUE(true);
}

TEST_F(IncrementalTest, JoinNoExp) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    Context c(opts);

    auto writeURI = URI(testName + "/" + testName + ".csv");
    auto readURI = URI(testName + "/" + testName + ".*.csv");

    auto ds1 = c.parallelize({Row("A", 1),
                              Row("B", 2),
                              Row("C", 3)},
                             vector<string>{"a", "b"});
    auto ds2 = c.parallelize({Row("A", -1),
                              Row("B", -2),
                              Row("C", -3)},
                             vector<string>{"c", "d"});

    ds1.join(ds2, string("a"), string("c"))
        .tocsv(writeURI);

    unordered_multiset<string> expectedOutput({Row(1, "A", -1).toPythonString(), Row(2, "B", -2).toPythonString(), Row(3, "C", -3).toPythonString()});
    auto actualOutput = c.csv(readURI.toPath()).collectAsVector();

    ASSERT_EQ(expectedOutput.size(), actualOutput.size());
    for (const auto &row : actualOutput)
        ASSERT_TRUE(expectedOutput.find(row.toPythonString()) != expectedOutput.end());
}

//TEST_F(IncrementalTest, JoinTimeBenchmark) {
//    using namespace tuplex;
//    using namespace std;
//
//    auto opts = testOptions();
//    opts.set("tuplex.partitionSize", "32MB");
//    opts.set("tuplex.executorCount", "7");
//    opts.set("tuplex.executorMemory", "128MB");
//    opts.set("tuplex.driverMemory", "128MB");
//    Context c(opts);
//
//    auto normalURI = URI(testName + "/" + testName + "normal.csv");
//    auto expURI = URI(testName + "/" + testName + "exp.csv");
//    auto outputURI = URI(testName + "/" + testName + "out.csv");
//
//    stringstream normalFile;
//    normalFile <<"a,b\n";
//    for (int i = 0; i < 9000000; ++i)
//        normalFile << to_string(i) << ",hello\n";
//    stringToFile(normalURI, normalFile.str());
//
//    stringstream expFile;
//    expFile << "a,b\n";
//    for (int i = 0; i < 1000000; ++i)
//        expFile << to_string(i) << ",hello\n";
//    stringToFile(expURI, expFile.str());
//
//    auto exp = c.csv(expURI.toPath());
//    auto norm = c.csv(normalURI.toPath());
//
//    Timer timer;
//    norm.join(norm, string("a"), string("a")).tocsv(outputURI);
//    auto onePassTime = timer.time();
//
//    timer.reset();
//    exp.join(norm, string("a"), string("a")).tocsv(outputURI);
//    norm.join(exp, string("a"), string("a")).tocsv(outputURI);
//    exp.join(exp, string("a"), string("a")).tocsv(outputURI);
//    auto threePassTime = timer.time();
//
//    cout << "One Pass: " << onePassTime << " (s), Three Pass: " << threePassTime << " (s)\n";
//}

TEST_F(IncrementalTest, JoinLeftBeforeExp) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    Context c(opts);

    auto writeURI = URI(testName + "/" + testName + ".csv");
    auto readURI = URI(testName + "/" + testName + ".*.csv");

    auto ds1 = c.parallelize({Row("A", 1),
                              Row("B", 2),
                              Row("C", 3)},
                             vector<string>{"a", "b"});
    auto ds2 = c.parallelize({Row("A", -1),
                              Row("B", -2),
                              Row("C", -3)},
                             vector<string>{"c", "d"});

    ds1.mapColumn("b", UDF("lambda x: 1 // (x - x) if x == 2 else x"))
            .join(ds2, string("a"), string("c"))
            .tocsv(writeURI);

    unordered_multiset<string> expectedOutput1({Row(1, "A", -1).toPythonString(),
                                                Row(3, "C", -3).toPythonString()});
    auto actualOutput1 = c.csv(readURI.toPath()).collectAsVector();
    ASSERT_EQ(expectedOutput1.size(), actualOutput1.size());
    for (const auto &row : actualOutput1)
        ASSERT_TRUE(expectedOutput1.find(row.toPythonString()) != expectedOutput1.end());

    ds1.mapColumn("b", UDF("lambda x: 1 // (x - x) if x == 2 else x"))
        .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"))
        .join(ds2, string("a"), string("c"))
        .tocsv(writeURI);

    unordered_multiset<string> expectedOutput2({Row(1, "A", -1).toPythonString(),
                                                Row(2, "B", -2).toPythonString(),
                                                Row(3, "C", -3).toPythonString()});
    auto actualOutput2 = c.csv(readURI.toPath()).collectAsVector();
    ASSERT_EQ(expectedOutput2.size(), actualOutput2.size());
    for (const auto &row : actualOutput2)
        ASSERT_TRUE(expectedOutput2.find(row.toPythonString()) != expectedOutput2.end());
}

TEST_F(IncrementalTest, JoinRightBeforeExp) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    Context c(opts);

    auto writeURI = URI(testName + "/" + testName + ".csv");
    auto readURI = URI(testName + "/" + testName + ".*.csv");

    auto ds1 = c.parallelize({Row("A", 1),
                              Row("B", 2),
                              Row("C", 3)},
                             vector<string>{"a", "b"});
    auto ds2 = c.parallelize({Row("A", -1),
                              Row("B", -2),
                              Row("C", -3)},
                             vector<string>{"c", "d"});

    ds2 = ds2.mapColumn("d", UDF("lambda x: 1 // (x - x) if x == -2 else x"));

    ds1.join(ds2, string("a"), string("c"))
        .tocsv(writeURI);

    unordered_multiset<string> expectedOutput1({Row(1, "A", -1).toPythonString(),
                                                Row(3, "C", -3).toPythonString()});
    auto actualOutput1 = c.csv(readURI.toPath()).collectAsVector();
    ASSERT_EQ(expectedOutput1.size(), actualOutput1.size());
    for (const auto &row : actualOutput1)
        ASSERT_TRUE(expectedOutput1.find(row.toPythonString()) != expectedOutput1.end());

    ds2 = ds2.resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"));

    ds1.join(ds2, string("a"), string("c"))
        .tocsv(writeURI);

    unordered_multiset<string> expectedOutput2({Row(1, "A", -1).toPythonString(),
                                                Row(2, "B", -2).toPythonString(),
                                                Row(3, "C", -3).toPythonString()});
    auto actualOutput2 = c.csv(readURI.toPath()).collectAsVector();
    ASSERT_EQ(expectedOutput2.size(), actualOutput2.size());
    for (const auto &row : actualOutput2)
        ASSERT_TRUE(expectedOutput2.find(row.toPythonString()) != expectedOutput2.end());
}

TEST_F(IncrementalTest, JoinBothBeforeExp) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    Context c(opts);

    auto writeURI = URI(testName + "/" + testName + ".csv");
    auto readURI = URI(testName + "/" + testName + ".*.csv");

    auto ds1 = c.parallelize({Row("A", 1),
                              Row("B", 2),
                              Row("C", 3)},
                             vector<string>{"a", "b"});
    auto ds2 = c.parallelize({Row("A", -1),
                              Row("B", -2),
                              Row("C", -3)},
                             vector<string>{"c", "d"});

    ds1 = ds1.mapColumn("b", UDF("lambda x: 1 // (x - x) if x == 1 else x"));
    ds2 = ds2.mapColumn("d", UDF("lambda x: 1 // (x - x) if x == -2 else x"));

    ds1.join(ds2, string("a"), string("c"))
        .tocsv(writeURI);

    unordered_multiset<string> expectedOutput1({Row(3, "C", -3).toPythonString()});
    auto actualOutput1 = c.csv(readURI.toPath()).collectAsVector();
    ASSERT_EQ(expectedOutput1.size(), actualOutput1.size());
    for (const auto &row : actualOutput1)
        ASSERT_TRUE(expectedOutput1.find(row.toPythonString()) != expectedOutput1.end());

    ds1 = ds1.resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"));
    ds2 = ds2.resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"));

    ds1.join(ds2, string("a"), string("c"))
            .tocsv(writeURI);

    unordered_multiset<string> expectedOutput2({Row(1, "A", -1).toPythonString(),
                                                Row(2, "B", -2).toPythonString(),
                                                Row(3, "C", -3).toPythonString()});
    auto actualOutput2 = c.csv(readURI.toPath()).collectAsVector();
    ASSERT_EQ(expectedOutput2.size(), actualOutput2.size());
    for (const auto &row : actualOutput2)
        ASSERT_TRUE(expectedOutput2.find(row.toPythonString()) != expectedOutput2.end());
}

TEST_F(IncrementalTest, JoinAfterExp) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    Context c(opts);

    auto writeURI = URI(testName + "/" + testName + ".csv");
    auto readURI = URI(testName + "/" + testName + ".*.csv");

    auto ds1 = c.parallelize({Row("A", 1),
                              Row("B", 2),
                              Row("C", 3)},
                             vector<string>{"a", "b"});
    auto ds2 = c.parallelize({Row("A", -1),
                              Row("B", -2),
                              Row("C", -3)},
                             vector<string>{"c", "d"});

    ds1.join(ds2, string("a"), string("c"))
        .mapColumn("b", UDF("lambda x: 1 // (x - x) if x == 2 else x"))
        .tocsv(writeURI);

    unordered_multiset<string> expectedOutput1({Row(1, "A", -1).toPythonString(),
                                                Row(3, "C", -3).toPythonString()});
    auto actualOutput1 = c.csv(readURI.toPath()).collectAsVector();
    ASSERT_EQ(expectedOutput1.size(), actualOutput1.size());
    for (const auto &row : actualOutput1)
        ASSERT_TRUE(expectedOutput1.find(row.toPythonString()) != expectedOutput1.end());

    ds1.join(ds2, string("a"), string("c"))
        .mapColumn("b", UDF("lambda x: 1 // (x - x) if x == 2 else x"))
        .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"))
        .tocsv(writeURI);

    unordered_multiset<string> expectedOutput2({Row(1, "A", -1).toPythonString(),
                                                Row(2, "B", -2).toPythonString(),
                                                Row(3, "C", -3).toPythonString()});
    auto actualOutput2 = c.csv(readURI.toPath()).collectAsVector();
    ASSERT_EQ(expectedOutput2.size(), actualOutput2.size());
    for (const auto &row : actualOutput2)
        ASSERT_TRUE(expectedOutput2.find(row.toPythonString()) != expectedOutput2.end());
}

TEST_F(IncrementalTest, CommitMode) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    Context c(opts);

    auto outputURI = URI(testName + "/" + testName + ".csv");

    auto csvops = defaultCSVOutputOptions();
    csvops["commit"] = boolToString(false);

    c.parallelize({Row(1), Row(-1), Row(2), Row(-2), Row(3), Row(-3)})
        .map(UDF("lambda x: 1 // (x - x) if x == -1 else x"))
        .map(UDF("lambda x: 1 // (x - x) if x == -2 else x"))
        .map(UDF("lambda x: 1 // (x - x) if x == -3 else x"))
        .tocsv(outputURI, csvops);

    c.parallelize({Row(1), Row(-1), Row(2), Row(-2), Row(3), Row(-3)})
            .map(UDF("lambda x: 1 // (x - x) if x == -1 else x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"))
            .map(UDF("lambda x: 1 // (x - x) if x == -2 else x"))
            .map(UDF("lambda x: 1 // (x - x) if x == -3 else x"))
            .tocsv(outputURI, csvops);

    c.parallelize({Row(1), Row(-1), Row(2), Row(-2), Row(3), Row(-3)})
            .map(UDF("lambda x: 1 // (x - x) if x == -1 else x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"))
            .map(UDF("lambda x: 1 // (x - x) if x == -2 else x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"))
            .map(UDF("lambda x: 1 // (x - x) if x == -3 else x"))
            .tocsv(outputURI, csvops);

    csvops["commit"] = boolToString(true);

    c.parallelize({Row(1), Row(-1), Row(2), Row(-2), Row(3), Row(-3)})
            .map(UDF("lambda x: 1 // (x - x) if x == -1 else x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"))
            .map(UDF("lambda x: 1 // (x - x) if x == -2 else x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"))
            .map(UDF("lambda x: 1 // (x - x) if x == -3 else x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: x"))
            .tocsv(outputURI, csvops);
}

void testIncrementalNoMerge(tuplex::ContextOptions opts, tuplex::URI fileURI, size_t numRows, float general, float fallback, float exception) {
    using namespace tuplex;
    using namespace std;

    opts.set("tuplex.executorCount", "4");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.nullValueOptimization", "true");
    opts.set("tuplex.normalcaseThreshold", "0.6");
    opts.set("tuplex.resolveWithInterpreterOnly", "true");
    opts.set("tuplex.useLLVMOptimizer", "true");
    Context c(opts);

    vector<int> inputRows;
    vector<int> inputRowInds;
    inputRows.reserve(numRows);
    inputRowInds.reserve(numRows);
    for (int i = 0; i < numRows; ++i) {
        inputRows.push_back(i + 1);
        inputRowInds.push_back(i);
    }

    std::random_shuffle(inputRowInds.begin(), inputRowInds.end());
    int counter = 0;
    for (int i = 0; i < (int) (general * numRows); ++i) {
        inputRows[inputRowInds[counter]] = -1;
        counter++;
    }
    for (int i = 0; i < (int) (fallback * numRows); ++i) {
        inputRows[inputRowInds[counter]] = -2;
        counter++;
    }
    for (int i = 0; i < (int) (exception * numRows); ++i) {
        inputRows[inputRowInds[counter]] = -3;
        counter++;
    }

    stringstream ss;
    for (int i = 0; i < numRows; ++i) {
        ss << "1,";
        if (inputRows[i] != -1) {
            ss << to_string(inputRows[i]);
        }
        ss << "\n";
    }
    stringToFile(fileURI.toPath(), ss.str());

    auto udf = "def udf(x, y):\n"
               "    if y == -2:\n"
               "        return y ** 0.5\n"
               "    elif y == -1:\n"
               "        raise ValueError\n"
               "    else:\n"
               "        return float(y)";

    auto &ds_cached = c.csv(fileURI.toPath()).cache().map(UDF(udf)).cache();
}

TEST_F(IncrementalTest, NoMergeFallback) {
    using namespace tuplex;
    testIncrementalNoMerge(microTestOptions(), URI(testName + ".csv"), 100, 0.25, 0.25, 0.25);
}

void executeZillow(tuplex::Context &context, const tuplex::URI& outputURI, int step, bool commit) {
        using namespace tuplex;

        auto extractBd = "def extractBd(x):\n"
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

        auto extractType = "def extractType(x):\n"
                           "    t = x['title'].lower()\n"
                           "    type = 'unknown'\n"
                           "    if 'condo' in t or 'apartment' in t:\n"
                           "        type = 'condo'\n"
                           "    if 'house' in t:\n"
                           "        type = 'house'\n"
                           "    return type";

        auto extractBa = "def extractBa(x):\n"
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

        auto extractSqft = "def extractSqft(x):\n"
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

        auto extractPrice = "def extractPrice(x):\n"
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
        auto extractOffer = "def extractOffer(x):\n"
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
                            "    return offer";

        auto resolveBd = "def resolveBd(x):\n"
                         "    if 'Studio' in x['facts and features']:\n"
                         "        return 1\n"
                         "    raise ValueError\n";

        auto csvops = defaultCSVOutputOptions();
        csvops["commit"] = boolToString(commit);
        std::vector<std::string> columnNames({"url", "zipcode", "address", "city", "state", "bedrooms", "bathrooms", "sqft", "offer", "type", "price"});

        auto &ds = context.csv("../../../../benchmarks/incremental/data/zillow_dirty.csv");
        ds = ds.withColumn("bedrooms", UDF(extractBd));
        if (step > 0)
            ds = ds.resolve(ExceptionCode::VALUEERROR, UDF(resolveBd));
        if (step > 1)
            ds = ds.ignore(ExceptionCode::VALUEERROR);
        ds = ds.filter(UDF("lambda x: x ['bedrooms'] < 10"));
        ds = ds.withColumn("type", UDF(extractType));
        ds = ds.filter(UDF("lambda x: x['type'] == 'condo'"));
        ds = ds.withColumn("zipcode", UDF("lambda x: '%05d' % int(x['postal_code'])"));
        if (step > 2)
            ds = ds.ignore(ExceptionCode::TYPEERROR);
        ds = ds.mapColumn("city", UDF("lambda x: x[0].upper() + x[1:].lower()"));
        ds = ds.withColumn("bathrooms", UDF(extractBa));
        if (step > 3)
            ds = ds.ignore(ExceptionCode::VALUEERROR);
        ds = ds.withColumn("sqft", UDF(extractSqft));
        if (step > 4)
            ds = ds.ignore(ExceptionCode::VALUEERROR);
        ds = ds.withColumn("offer", UDF(extractOffer));
        ds = ds.withColumn("price", UDF(extractPrice));
        if (step > 5)
            ds = ds.resolve(ExceptionCode::VALUEERROR, UDF("lambda x: int(100020)"));
        ds = ds.filter(UDF("lambda x: 100000 < x['price'] < 2e7 and x['offer'] == 'sale'"));
        ds = ds.selectColumns(columnNames);
        ds.tocsv(outputURI, csvops);
}

TEST_F(IncrementalTest, DirtyZilow) {
    using namespace tuplex;
    using namespace std;

    auto opts = testOptions();
    opts.set("tuplex.executorCount", "0");
    opts.set("tuplex.executorMemory", "2G");
    opts.set("tuplex.driverMemory", "2G");
    opts.set("tuplex.partitionSize", "32MB");
    opts.set("tuplex.resolveWithInterpreterOnly", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    Context incrementalContext(opts);
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    Context plainContext(opts);

    for (int step = 0; step < 7; ++step) {
        executeZillow(incrementalContext, testName + "/incremental.csv", step, true);
//        executeZillow(plainContext, testName + "/plain.csv", step);
    }

//    std::vector<std::string> incrementalRows;
//    auto incrementalResult = plainContext.csv(testName + "/incremental.*.csv").collect();
//    while (incrementalResult->hasNextRow())
//        incrementalRows.push_back(incrementalResult->getNextRow().toPythonString());
//
//    std::vector<std::string> plainRows;
//    auto plainResult = plainContext.csv(testName + "/plain.*.csv").collect();
//    while (plainResult->hasNextRow())
//        plainRows.push_back(plainResult->getNextRow().toPythonString());
//
//    ASSERT_EQ(incrementalRows.size(), plainRows.size());
//    for (int i = 0; i < plainRows.size(); ++i)
//        ASSERT_EQ(incrementalRows[i], plainRows[i]);
}

TEST_F(IncrementalTest, FileOutput) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.executorCount", "0");
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    Context c(opts);

    auto numRows = 50;
    auto amountExps = 0.25;
    std::vector<Row> inputRows;
    inputRows.reserve(numRows);
    std::unordered_multiset<std::string> expectedOutput1;
    expectedOutput1.reserve((int) (numRows * amountExps));
    std::unordered_multiset<std::string> expectedOutput2;
    expectedOutput2.reserve(numRows);

    auto inputFileURI = URI(testName + "/in.csv");
    auto fileURI = URI(testName + "/out.csv");
    auto outputFileURI = URI(testName + "/out.*.csv");

    std::stringstream ss;
    for (int i = 0; i < numRows; ++i) {
        if (i % (int) (1 / amountExps) == 0) {
            ss << "0\n";
            expectedOutput2.insert(Row(-1).toPythonString());
        } else {
            ss << to_string(i) << "\n";
            expectedOutput1.insert(Row(i).toPythonString());
            expectedOutput2.insert(Row(i).toPythonString());
        }
    }
    stringToFile(inputFileURI, ss.str());

    c.csv(inputFileURI.toPath()).map(UDF("lambda x: 1 // x if x == 0 else x")).tocsv(fileURI.toPath());
    auto output1 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output1.size(), expectedOutput1.size());
    for (const auto &row : output1) {
        ASSERT_TRUE(expectedOutput1.find(row.toPythonString()) != expectedOutput1.end());
    }

    c.csv(inputFileURI.toPath()).map(UDF("lambda x: 1 // x if x == 0 else x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1")).tocsv(fileURI.toPath());
    auto output2 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output2.size(), expectedOutput2.size());
    for (const auto &row : output2) {
        ASSERT_TRUE(expectedOutput2.find(row.toPythonString()) != expectedOutput2.end());
    }
}

TEST_F(IncrementalTest, FileOutputInOrder) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.executorCount", "0");
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    Context c(opts);

    auto numRows = 50;
    auto amountExps = 0.25;
    std::vector<Row> inputRows;
    inputRows.reserve(numRows);
    std::vector<std::string> expectedOutput1;
    expectedOutput1.reserve((int) (numRows * amountExps));
    std::vector<std::string> expectedOutput2;
    expectedOutput2.reserve(numRows);

    auto inputFileURI = URI(testName + "/in.csv");
    auto fileURI = URI(testName + "/out.csv");
    auto outputFileURI = URI(testName + "/out.*.csv");

    std::stringstream ss;
    for (int i = 0; i < numRows; ++i) {
        if (i % (int) (1 / amountExps) == 0) {
            ss << "0\n";
            expectedOutput2.push_back(Row(-1).toPythonString());
        } else {
            ss << to_string(i) << "\n";
            expectedOutput1.push_back(Row(i).toPythonString());
            expectedOutput2.push_back(Row(i).toPythonString());
        }
    }
    stringToFile(inputFileURI, ss.str());

    c.csv(inputFileURI.toPath()).map(UDF("lambda x: 1 // x if x == 0 else x")).tocsv(fileURI.toPath());
    auto output1 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output1.size(), expectedOutput1.size());
    for (int i = 0; i < expectedOutput1.size(); ++i) {
        ASSERT_EQ(expectedOutput1[i], output1[i].toPythonString());
    }

    c.csv(inputFileURI.toPath()).map(UDF("lambda x: 1 // x if x == 0 else x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1")).tocsv(fileURI.toPath());
    auto output2 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output2.size(), expectedOutput2.size());
    for (int i = 0; i < expectedOutput2.size(); ++i) {
        ASSERT_EQ(expectedOutput2[i], output2[i].toPythonString());
    }
}

TEST_F(IncrementalTest, DebugResolver) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    Context c(opts);

//    c.parallelize({Row(1), Row(0), Row(3)})
//        .map(UDF("lambda x: 1 // x if x == 0 else x"))
//        .tocsv(testName + "/out.csv");
//
    c.parallelize({Row(1), Row(0), Row(3)})
        .map(UDF("lambda x: 1 // x if x == 0 else x"))
        .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 1 // x"))
        .tocsv(testName + "/out.csv");

    c.parallelize({Row(1), Row(0), Row(3)})
            .map(UDF("lambda x: 1 // x if x == 0 else x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 1 // x"))
            .ignore(ExceptionCode::ZERODIVISIONERROR)
            .tocsv(testName + "/out.csv");
}

TEST_F(IncrementalTest, Filter) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.executorCount", "2");
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    opts.set("tuplex.resolveWithInterpreterOnly", "false");
    Context c(opts);

    auto inputFileURI = URI(testName + "/in.csv");
    auto fileURI = URI(testName + "/out.csv");
    auto outputFileURI = URI(testName + "/out.*.csv");

    std::vector<Row> expectedOutput1;
    std::vector<Row> expectedOutput2;
    std::stringstream ss;
    for (int i = 0; i < 1000; ++i) {
        auto num = rand()%4;
        switch (num) {
            case 0: {
                ss << to_string(i) << "\n";
                expectedOutput1.push_back(Row(i));
                expectedOutput2.push_back(Row(i));
                break;
            }
            case 1: {
                ss << "-1\n";
                break;
            }
            case 2: {
                ss << "-2\n";
                expectedOutput2.push_back(Row(-2));
                break;
            }
            case 3: {
                ss << "0\n";
                break;
            }
        }
    }
    stringToFile(inputFileURI, ss.str());

    c.csv(inputFileURI.toPath()).map(UDF("lambda x: 1 // (x - x) if x < 0 else x")).filter(UDF("lambda x: x != 0")).tocsv(fileURI.toPath());
    auto output1 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output1.size(), expectedOutput1.size());
    for (int i = 0; i < expectedOutput1.size(); ++i) {
        ASSERT_EQ(expectedOutput1[i].toPythonString(), output1[i].toPythonString());
    }

    c.csv(inputFileURI.toPath()).map(UDF("lambda x: 1 // (x - x) if x < 0 else x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 1 // (x - x) if x == -1 else x")).filter(UDF("lambda x: x != 0")).tocsv(fileURI.toPath());
    auto output2 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output2.size(), expectedOutput2.size());
    for (int i = 0; i < expectedOutput2.size(); ++i) {
        ASSERT_EQ(expectedOutput2[i].toPythonString(), output2[i].toPythonString());
    }
}

TEST_F(IncrementalTest, FileOutput2) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.resolverWithInterpreterOnly", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    Context c(opts);

    auto numRows = 10000;
    auto amountExps = 0.25;
    std::vector<Row> inputRows;
    inputRows.reserve(numRows);
    std::unordered_multiset<std::string> expectedOutput1;
    expectedOutput1.reserve((int) (numRows * amountExps));
    std::unordered_multiset<std::string> expectedOutput2;
    expectedOutput2.reserve(numRows);

    auto inputFileURI = URI(testName + "/in.csv");
    auto fileURI = URI(testName + "/out.csv");
    auto outputFileURI = URI(testName + "/out.*.csv");

    std::stringstream ss;
    for (int i = 0; i < numRows; ++i) {
        if (i % (int) (1 / amountExps) == 0) {
            ss << "0\n";
            expectedOutput2.insert(Row(-1).toPythonString());
        } else {
            ss << to_string(i) << "\n";
            expectedOutput1.insert(Row(i).toPythonString());
            expectedOutput2.insert(Row(i).toPythonString());
        }
    }
    stringToFile(inputFileURI, ss.str());

    c.csv(inputFileURI.toPath()).map(UDF("lambda x: 1 // x if x == 0 else x"))
        .map(UDF("lambda x: 1 // x if x == 0 else x"))
        .tocsv(fileURI.toPath());
    auto output1 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output1.size(), expectedOutput1.size());
    for (const auto &row : output1) {
        ASSERT_TRUE(expectedOutput1.find(row.toPythonString()) != expectedOutput1.end());
    }

    c.csv(inputFileURI.toPath()).map(UDF("lambda x: 1 // x if x == 0 else x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 0"))
            .map(UDF("lambda x: 1 // x if x == 0 else x"))
            .tocsv(fileURI.toPath());
    auto output2 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output2.size(), expectedOutput1.size());
    for (const auto &row : output2) {
        ASSERT_TRUE(expectedOutput1.find(row.toPythonString()) != expectedOutput1.end());
    }

    c.csv(inputFileURI.toPath())
        .map(UDF("lambda x: 1 // x if x == 0 else x"))
        .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 0"))
        .map(UDF("lambda x: 1 // x if x == 0 else x"))
        .ignore(ExceptionCode::ZERODIVISIONERROR)
        .tocsv(fileURI.toPath());
    auto output3 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output3.size(), expectedOutput1.size());
    for (const auto &row : output3) {
        ASSERT_TRUE(expectedOutput2.find(row.toPythonString()) != expectedOutput1.end());
    }
}

TEST_F(IncrementalTest, FlightsPipeline) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.resolverWithInterpreterOnly", "false");
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    Context c(opts);

    // create flights pipeline with aggregation
    auto& ds = c.csv("../resources/flights_on_time_performance_2019_01.sample.csv");
    auto v = ds.collectAsVector();
    EXPECT_FALSE(v.empty());
}