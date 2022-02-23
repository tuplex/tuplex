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

void executeZillow(tuplex::Context &context, const tuplex::URI& outputURI, int step) {
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

        std::vector<std::string> columnNames({"url", "zipcode", "address", "city", "state", "bedrooms", "bathrooms", "sqft", "offer", "type", "price"});

        auto &ds = context.csv("../resources/zillow_dirty.csv");
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
        ds.tocsv(outputURI);
}

TEST_F(IncrementalTest, DirtyZilow) {
    using namespace tuplex;
    using namespace std;

    auto opts = testOptions();
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    Context incrementalContext(opts);
    opts.set("tuplex.optimizer.incrementalResolution", "false");
    Context plainContext(opts);

    for (int step = 0; step < 7; ++step) {
        executeZillow(incrementalContext, testName + "/incremental.csv", step);
        executeZillow(plainContext, testName + "/plain.csv", step);

        std::multiset<std::string> incrementalRows;
        auto incrementalResult = plainContext.csv(testName + "/incremental.*.csv").collect();
        while (incrementalResult->hasNextRow())
            incrementalRows.insert(incrementalResult->getNextRow().toPythonString());

        auto plainResult = plainContext.csv(testName + "/plain.*.csv").collect();

        auto numPlainRows = 0;
        while (plainResult->hasNextRow()) {
            numPlainRows++;
            ASSERT_TRUE(incrementalRows.find(plainResult->getNextRow().toPythonString()) != incrementalRows.end());
        }
        ASSERT_EQ(numPlainRows, incrementalRows.size());
    }
}

TEST_F(IncrementalTest, FileOutput) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.driverMemory", "300B");
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

TEST_F(IncrementalTest, FileOutput2) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
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