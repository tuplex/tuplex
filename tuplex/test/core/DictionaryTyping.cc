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
#include <TypeAnnotatorVisitor.h>
#include <SymbolTable.h>
#include <gtest/gtest.h>
#include <parser/Parser.h>
#include <graphviz/GraphVizGraph.h>
#include <CodegenHelper.h>
#include <AnnotatedAST.h>

// classes to work with:
// type annotator visitor
// trace visitor

TEST(DictionaryTyping, Simple) {
    using namespace tuplex;
    using namespace std;

    // test simple UDF
    auto simple_c = "def f(L):\n"
                    "    d = {}\n"
                    "    k = L[0]\n"
                    "    d[k] = 0\n"
                    "    d[k] += 1\n"
                    "    return d";

    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(simple_c);

    // make typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("typed_ast.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    // ASSERT_EQ(ast->getInferredType().getReturnType(), python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));

}

TEST(DictionaryTyping, Count) {
    using namespace tuplex;
    using namespace std;

    // test count UDF
    auto count_c = "def count(L):\n"
                    "    d = {}\n"
                    "    for x in L:\n"
                    "        if x not in d.keys():\n"
                    "            d[x] = 0\n"
                    "        d[x] += 1\n"
                    "    return d";

    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(count_c);

    // make typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("typed_ast.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    // ASSERT_EQ(ast->getInferredType().getReturnType(), python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));

}