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
#include <AnnotatedAST.h>
#include "Helper.h"
#include <graphviz/GraphVizGraph.h>
#include "../../codegen/include/parser/Parser.h"

// @Todo: lambda x: x : 10 will lead to infinity loop in parser...
// give up on that...

using namespace tuplex;

bool saveToPDF(ASTNode* root, const std::string& path) {
    assert(root);
    GraphVizGraph graph;
    graph.createFromAST(root);
    return graph.saveAsPDF(path);
}

// works
TEST(ComplexUDF, extractZipcode) {
    // this is the nice code, however exchange with simpler formatting code...
    // auto code = "lambda x: '{:05}'.format(int(x['postal_code']))";
    auto code = "lambda x: '%05d' % int(x['postal_code'])";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);

    saveToPDF(node.get(), "extractZipcode.pdf");
}


// slice leads to infinity loop
TEST(ComplexUDF, cleanCity) {
    auto code = "lambda x: x['city'][0].upper() + x['city'][1:].lower()";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);

    saveToPDF(node.get(), "cleanCity.pdf");
}

// works
TEST(ComplexUDF, dictionary) {
    auto code = "lambda x: {'a': 1, 'b': 2})";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);

    saveToPDF(node.get(), "dictionary_test.pdf");
}


TEST(ComplexUDF, list) {
    auto code = "lambda x: ['abc', 'def', 'ghi'])";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);

    saveToPDF(node.get(), "list_test.pdf");
}


TEST(ComplexUDF, testIF) {

//    auto code = "def extractBd(x):\n"
//                "   if x > 10:\n"
//                "       return 20\n"
//                "   else:\n"
//                "       return 10\n";

    auto code = "if True:\n"
                "    20\n"
                "else:\n"
                "    10\n";

    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
    saveToPDF(node.get(), "testIF.pdf");
}

TEST(ComplexUDF, extractBd) {
    auto code = "def extractBd(x):\n"
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
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
}

TEST(ComplexUDF, extractBa) {
    auto code = "def extractBa(x):\n"
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
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
}

TEST(ComplexUDF, extractSqft) {
    auto code = "def extractSqft(x):\n"
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
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
}

TEST(ComplexUDF, extractOffer) {
    auto code = "def extractOffer(x):\n"
                "    offer = x['title']\n"
                "    assert 'Sale' in offer or 'Rent' in offer or 'SOLD' in offer\n"
                "\n"
                "    if 'Sale' in offer:\n"
                "        offer = 'sale'\n"
                "    if 'Rent' in offer:\n"
                "        offer = 'rent'\n"
                "    if 'SOLD' in offer:\n"
                "        offer = 'sold'\n"
                "    if 'foreclos' in offer.lower():\n"
                "        offer = 'foreclosed'\n"
                "\n"
                "    return offer";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
}

TEST(ComplexUDF, extractType) {
    auto code = "def extractType(x):\n"
                "    t = x['title'].lower()\n"
                "    type = 'unknown'\n"
                "    if 'condo' in t or 'appartment' in t:\n"
                "        type = 'condo'\n"
                "    if 'house' in t:\n"
                "        type = 'house'\n"
                "    return type";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
}

TEST(ComplexUDF, extractPrice) {
    auto code = "def extractPrice(x):\n"
                "    price = x['price']\n"
                "\n"
                "    if x['offer'] == 'sold':\n"
                "        # price is to be calculated using price/sqft * sqft\n"
                "        val = x['facts and features']\n"
                "        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]\n"
                "        r = s[s.find('$')+1:s.find(', ') - 1]\n"
                "        price_per_sqft = int(r)\n"
                "        price = price_per_sqft * x['sqft']\n"
                "    elif x['offer'] == 'rent':\n"
                "        max_idx = price.rfind('/')\n"
                "        price = int(price[1:max_idx].replace(',', ''))\n"
                "    else:\n"
                "        # take price from price column\n"
                "        price = int(price[1:].replace(',', ''))\n"
                "\n"
                "    return price";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
}

TEST(ComplexUDF, filterPrice) {
    auto code = "lambda x: 100000 < x['price'] <= 2e7";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
}

TEST(ComplexUDF, filterType) {
    auto code = "lambda x: x['type'] == 'house'";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
}

TEST(ComplexUDF, filterBd) {
    auto code = "x['bedrooms'] < 10";
    auto node = std::unique_ptr<ASTNode>(tuplex::parseToAST(code));
    ASSERT_TRUE(node);
}