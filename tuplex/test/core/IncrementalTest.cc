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

TEST_F(IncrementalTest, DirtyZilow) {
    using namespace tuplex;
    using namespace std;

    auto opts = testOptions();
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    Context c(opts);

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

    std::vector<double> times;
    Timer timer;
    for (int step = 0; step < 7; ++step) {
        auto tstart = timer.time();
        auto &ds = c.csv("../resources/zillow_dirty.csv");
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
            ds = ds.resolve(ExceptionCode::VALUEERROR, UDF("lambda x: 100002"));
        ds = ds.filter(UDF("lambda x: 100000 < x['price'] < 2e7 and x['offer'] == 'sale'"));
        ds = ds.selectColumns({"url", "zipcode", "address", "city", "state",
                               "bedrooms", "bathrooms", "sqft", "offer", "type", "price"});
        ds.tocsv(testName + "/zillow_output.csv");
        times.push_back(timer.time() - tstart);
    }

    for (auto time : times) {
        std::cout << "Iteration took: " << to_string(time) << "(s)\n";
    }
}

TEST_F(IncrementalTest, FileOutput) {
    using namespace tuplex;
    using namespace std;

    auto opts = testOptions();
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "false");
    Context c(opts);

    auto numRows = 100;
    auto amountExps = 0.25;
    std::vector<Row> inputRows;
    inputRows.reserve(numRows);
    std::unordered_multiset<std::string> expectedOutput1;
    expectedOutput1.reserve((int) (numRows * amountExps));
    std::unordered_multiset<std::string> expectedOutput2;
    expectedOutput2.reserve(numRows);

    for (int i = 0; i < numRows; ++i) {
        if (i % (int) (1 / amountExps) == 0) {
            inputRows.push_back(Row(0));
            expectedOutput2.insert(Row(-1).toPythonString());
        } else {
            inputRows.push_back(Row(i));
            expectedOutput1.insert(Row(i).toPythonString());
            expectedOutput2.insert(Row(i).toPythonString());
        }
    }

    auto fileURI = URI(testName + "/out.csv");
    auto outputFileURI = URI(testName + "/out.*.csv");

    auto &ds_cached = c.parallelize(inputRows).cache();

    ds_cached.map(UDF("lambda x: 1 // x if x == 0 else x")).tocsv(fileURI.toPath());
    auto output1 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output1.size(), expectedOutput1.size());
    for (const auto &row : output1) {
        ASSERT_TRUE(expectedOutput1.find(row.toPythonString()) != expectedOutput1.end());
    }

    ds_cached.map(UDF("lambda x: 1 // x if x == 0 else x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1")).tocsv(fileURI.toPath());
    auto output2 = c.csv(outputFileURI.toPath()).collectAsVector();
    ASSERT_EQ(output2.size(), expectedOutput2.size());
    for (const auto &row : output2) {
        ASSERT_TRUE(expectedOutput2.find(row.toPythonString()) != expectedOutput2.end());
    }
}