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
    python::Type inputType = python::Type::makeListType(python::Type::F64);

    // create symbol table
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/typed_ast.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    ASSERT_EQ(ast.getReturnType(), python::Type::makeDictionaryType(python::Type::F64, python::Type::I64));

    // TODO: case where d = {0: {}, 1: {0: 10, 1: 15}, 2: None} --> Dict[i64, Option[Dict[i64, i64]]]
}

TEST(DictionaryTyping, IndexExpression) {
    using namespace tuplex;
    using namespace std;

    // a[2 * k + 1] = n
    auto code = "def f(L):\n"
                "    d = {}\n"
                "    k = L[0]\n"
                "    d[2 * k + 1] = 0\n"
                "    return d";

    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/typed_ast_1.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    ASSERT_EQ(ast.getReturnType(), python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));
}

TEST(DictionaryTyping, NestedSubscripts) {
    using namespace tuplex;
    using namespace std;

    // Q: what should I do about the case where dictionaries don't have the same number of entries (is this supported?)

    // a[x][y] = n
    auto code_1 = "def f(L):\n"
                  "    d = {0: {0: 10, 1: 100}, 1: {0: 15, 1: 500}}\n"
                  "    w = L[0]\n"
                  "    x = L[1]\n"
                  "    d[w][x] = 15\n"
                  "    return d";

    // parse code to AST
    auto ast_1 = tuplex::codegen::AnnotatedAST();
    ast_1.parseString(code_1);

    // make typing
    python::Type inputType_1 = python::Type::makeListType(python::Type::I64);

    // create symbol table
    ast_1.addTypeHint("L", inputType_1);
    ast_1.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph_1;
    graph_1.createFromAST(ast_1.getFunctionAST(), true);
    graph_1.saveAsPDF("typed_ast.pdf");

    cout<<"return type of function is: "<<ast_1.getReturnType().desc()<<endl;

    python::Type expected_ret_1 = python::Type::makeDictionaryType(python::Type::I64, python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));
    ASSERT_EQ(ast_1.getReturnType(), expected_ret_1);

    // a[x][y] = n, with case where empty dictionary type should be upcasted to Dict[i64, i64]
    auto code_2 = "def f(L):\n"
                  "    d = {0: {0: 10, 1: 100}, 1: {}}\n"
                  "    w = L[0]\n"
                  "    x = L[1]\n"
                  "    d[w][x] = 15\n"
                  "    return d";

    // parse code to AST
    auto ast_2 = tuplex::codegen::AnnotatedAST();
    ast_2.parseString(code_2);

    // make typing
    python::Type inputType_2 = python::Type::makeListType(python::Type::I64);

    // create symbol table
    ast_2.addTypeHint("L", inputType_2);
    ast_2.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph_2;
    graph_2.createFromAST(ast_2.getFunctionAST(), true);
    graph_2.saveAsPDF("typed_ast.pdf");

    cout<<"return type of function is: "<<ast_2.getReturnType().desc()<<endl;

    python::Type expected_ret_2 = python::Type::makeDictionaryType(python::Type::I64, python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));
    ASSERT_EQ(ast_2.getReturnType(), expected_ret_2);

    // a[x][y][z][w] = n
    // auto code = "def f(L):\n"
    //             "    d = {0: {0: {0: {0: 10}, 1: {0: 1}}, 1: {0: {0: 15}, 1: {0: 2}}}, 1: {0: {0: {0: 20}, 1: {0: 0}}, 1: {0: {0: 19}, 1: {0: 4}}}}\n"
    //             "    w = L[0]\n"
    //             "    x = L[1]\n"
    //             "    y = L[2]\n"
    //             "    z = L[3]\n"
    //             "    d[w][x][y][z] = 60\n"
    //             "    return d";

    // // parse code to AST
    // auto ast = tuplex::codegen::AnnotatedAST();
    // ast.parseString(code);

    // // make typing
    // python::Type inputType = python::Type::makeListType(python::Type::I64);

    // // create symbol table
    // ast.addTypeHint("L", inputType);
    // ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // // print type annotated ast
    // GraphVizGraph graph;
    // graph.createFromAST(ast.getFunctionAST(), true);
    // graph.saveAsPDF("typed_ast.pdf");

    // cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    // ASSERT_EQ(ast->getInferredType().getReturnType(), python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));
}

TEST(DictionaryTyping, AttributeSubscripts) {
    using namespace tuplex;
    using namespace std;

    // how should I write tests that use classes + class attributes?

    // a.b[x] = n
    auto code_1 = "def f(L):\n"
                  "    d = {0: {0: 10, 1: 100}, 1: {0: 15, 1: 500}}\n"
                  "    w = L[0]\n"
                  "    x = L[1]\n"
                  "    d[w][x] = 15\n"
                  "    return d";

    // parse code to AST
    auto ast_1 = tuplex::codegen::AnnotatedAST();
    ast_1.parseString(code_1);

    // make typing
    python::Type inputType_1 = python::Type::makeListType(python::Type::I64);

    // create symbol table
    ast_1.addTypeHint("L", inputType_1);
    ast_1.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph_1;
    graph_1.createFromAST(ast_1.getFunctionAST(), true);
    graph_1.saveAsPDF("typed_ast.pdf");

    cout<<"return type of function is: "<<ast_1.getReturnType().desc()<<endl;

    python::Type expected_ret_1 = python::Type::makeDictionaryType(python::Type::I64, python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));
    ASSERT_EQ(ast_1.getReturnType(), expected_ret_1);

    // a.b.c[x] = n
    auto code_2 = "def f(L):\n"
                  "    d = {0: {0: 10, 1: 100}, 1: {}}\n"
                  "    w = L[0]\n"
                  "    x = L[1]\n"
                  "    d[w][x] = 15\n"
                  "    return d";

    // parse code to AST
    auto ast_2 = tuplex::codegen::AnnotatedAST();
    ast_2.parseString(code_2);

    // make typing
    python::Type inputType_2 = python::Type::makeListType(python::Type::I64);

    // create symbol table
    ast_2.addTypeHint("L", inputType_2);
    ast_2.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph_2;
    graph_2.createFromAST(ast_2.getFunctionAST(), true);
    graph_2.saveAsPDF("typed_ast.pdf");

    cout<<"return type of function is: "<<ast_2.getReturnType().desc()<<endl;

    python::Type expected_ret_2 = python::Type::makeDictionaryType(python::Type::I64, python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));
    ASSERT_EQ(ast_2.getReturnType(), expected_ret_2);
    
    // a[x].b[y] = n
    // a.b[x][y][z] = n
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

    ASSERT_EQ(ast.getReturnType(), python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));
}