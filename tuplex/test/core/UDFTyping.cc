//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <UDF.h>
#include <visitors/TypeAnnotatorVisitor.h>
#include <gtest/gtest.h>
#include <parser/Parser.h>
#include <visitors/ColumnRewriteVisitor.h>
#include <graphviz/GraphVizGraph.h>

//TEST(UDFTyping, IfStatementsReassign) {
//    using namespace tuplex;
//    using namespace std;
//
//    // this is a simple symbol table test, i.e. can happen that symbol table doesn't catch this properly...
////    auto extractPrice_c = "def extractPrice(x):\n"
////                          "    price = x['price']\n"
////                          "\n"
////                          "    if x['offer'] == 'sold':\n"
////                          "        price = 20\n"
////                          "    else:\n"
////                          "        # take price from price column\n"
////                          "        price = 7\n"
////                          "\n"
////                          "    return price";
//
//    // this works...
//    // use it for now, to make pipeline work...
//    auto extractPrice_c = "def extractPrice(x):\n"
//                          "    price = x['price']\n"
//                          "    p = 0\n"
//                          "    if x['offer'] == 'sold':\n"
//                          "        # price is to be calculated using price/sqft * sqft\n"
//                          "        val = x['facts and features']\n"
//                          "        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]\n"
//                          "        r = s[s.find('$')+1:s.find(', ') - 1]\n"
//                          "        price_per_sqft = int(r)\n"
//                          "        p = price_per_sqft * x['sqft']\n"
//                          "    elif x['offer'] == 'rent':\n"
//                          "        max_idx = price.rfind('/')\n"
//                          "        p = int(price[1:max_idx].replace(',', ''))\n"
//                          "    else:\n"
//                          "        # take price from price column\n"
//                          "        p = int(price[1:].replace(',', ''))\n"
//                          "\n"
//                          "    return p";
//
//
//    // first, rewrite node
//    auto node = std::unique_ptr<ASTNode>(parseToAST(extractPrice_c));
//    ASSERT_TRUE(node);
//
//    // then, rewrite columns
//    vector<string> columnNames{"price", "offer", "facts and features", "sqft"};
//    ColumnRewriteVisitor crv(columnNames, "x");
//
//    node->accept(crv);
//
//    // now, run typing!
//    python::Type inputType = python::Type::makeTupleType({python::Type::STRING,
//                                                          python::Type::STRING,
//                                                          python::Type::STRING,
//                                                          python::Type::I64});
//
//    // create symbol table
//    SymbolTableBuilder stb;
//    stb.createTable(node.get());
//    auto table = stb.getTable();
//    //table->setTypeForFunctionParameter("x", inputType);
//    table->addSymbol("x", inputType); // ??
//
//    TypeAnnotatorVisitor tav(*table);
//    table->enterScope();
//    node->accept(tav);
//    table->exitScope();
//
//    // print type annotated ast
//    GraphVizGraph graph;
//    graph.createFromAST(node.get(), true);
//    graph.saveAsPDF("typed_ast.pdf");
//
//    cout<<"return type of function is: "<<node->getInferredType().getReturnType().desc()<<endl;
//
//    ASSERT_EQ(node->getInferredType().getReturnType(), python::Type::I64);
//
//}