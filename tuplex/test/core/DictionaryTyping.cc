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

// TEST(DictionaryTyping, Template) {
//     using namespace tuplex;
//     using namespace std;

//     auto code = "";
    
//     // parse code to AST
//     auto ast = tuplex::codegen::AnnotatedAST();
//     ast.parseString(code);

//     // make input typing
//     python::Type inputType = python::Type::PYOBJECT;

//     // create symbol table (add parameters and types)
//     ast.addTypeHint("L", inputType);
//     ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

//     // print type annotated ast
//     GraphVizGraph graph;
//     graph.createFromAST(ast.getFunctionAST(), true);
//     graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/<test_name>.pdf");

//     cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

//     python::Type expected_ret = python::Type::PYOBJECT;

//     // check return type
//     ASSERT_EQ(ast.getReturnType(), expected_ret);
// }

TEST(DictionaryTyping, Simple) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {}\n"
                "    k = L[0]\n"
                "    d[k] = 0\n"
                "    d[k] += 1\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make typing
    python::Type inputType = python::Type::makeListType(python::Type::F64);

    // create symbol table
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/simple_ast.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    ASSERT_EQ(ast.getReturnType(), python::Type::makeDictionaryType(python::Type::F64, python::Type::I64));
}

TEST(DictionaryTyping, KeyTypeChange) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {}\n"
                "    d['a'] = L[0]\n"
                "    d[2] = L[1]\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table (add parameters and types)
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/key_type_change.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::PYOBJECT, python::Type::I64);

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, ValueTypeChange) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {}\n"
                "    d[0] = L[0]\n"
                "    d[1] = L[1]\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::GENERICLIST;

    // create symbol table (add parameters and types)
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/value_type_change.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::I64, python::Type::PYOBJECT);

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, DictTypeChange) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {}\n"
                "    d[0] = L[0]\n"
                "    d['one'] = L[1]\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::GENERICLIST;

    // create symbol table (add parameters and types)
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/dict_type_change.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::PYOBJECT, python::Type::PYOBJECT);

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, IndexExpression) {
    using namespace tuplex;
    using namespace std;

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
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/index_exp_ast.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    ASSERT_EQ(ast.getReturnType(), python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));
}

TEST(DictionaryTyping, NestedSubscriptSimple) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {0: {0: 10, 1: 100}, 1: {0: 15, 1: 500}}\n"
                "    w = L[0]\n"
                "    x = L[1]\n"
                "    d[w][x] = 15\n"
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
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/nested_sub_simple.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::I64, python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, NestedSubscriptUpcast) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {0: {0: 10, 1: 100}, 1: {}}\n"
                "    w = L[0]\n"
                "    x = L[1]\n"
                "    d[w][x] = 60\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table (add parameters and types)
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/nested_sub_upcast.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::I64, python::Type::makeDictionaryType(python::Type::I64, python::Type::I64));

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, NestedSubscriptOption) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {0: {}, 1: {0: 10, 1: 15}, 2: None}\n"
                "    w = L[0]\n"
                "    x = L[1]\n"
                "    d[w][x] = 60\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table (add parameters and types)
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/nested_sub_option.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::I64, python::Type::makeOptionType(python::Type::makeDictionaryType(python::Type::I64, python::Type::I64)));

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, NestedSubscriptMultiple) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {0: {0: {0: {0: 10}, 1: {0: 1}}, 1: {0: {0: 15}, 1: {0: 2}}}, 1: {0: {0: {0: 20}, 1: {0: 0}}, 1: {0: {0: 19}, 1: {0: 4}}}}\n"
                "    w = L[0]\n"
                "    x = L[1]\n"
                "    y = L[2]\n"
                "    z = L[3]\n"
                "    d[w][x][y][z] = 60\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table (add parameters and types)
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/nested_sub_multiple.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = 
        python::Type::makeDictionaryType(
            python::Type::I64, 
            python::Type::makeDictionaryType(
                python::Type::I64, 
                python::Type::makeDictionaryType(
                    python::Type::I64, 
                    python::Type::makeDictionaryType(
                        python::Type::I64, 
                        python::Type::I64))));

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, ControlFlowSimple) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {}\n"
                "    d[0] = 0\n"
                "    d[1] = 0\n"
                "    if L[0] <= 5:\n"
                "        d[0] += 1\n"
                "    else:\n"
                "        d[1] += 1\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table (add parameters and types)
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/control_flow_simple.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::I64, python::Type::I64);

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, ControlFlowLoop) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {}\n"
                "    d[0] = 0\n"
                "    d[1] = 0\n"
                "    for i in L:\n"
                "        if i <= 5:\n"
                "            d[0] += 1\n"
                "        else:\n"
                "            d[1] += 1\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table (add parameters and types)
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/control_flow_loop.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::I64, python::Type::I64);

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

// TODO: need case where value being assigned to the same key is of a different type in each if/else branch

TEST(DictionaryTyping, ControlFlowKeyAssignment) {
    // currently fails; need to add support for dict_keys
    using namespace tuplex;
    using namespace std;

    auto code = "def f(L):\n"
                "    d = {}\n"
                "    for i in L:\n"
                "        if (i % 2) not in d.keys():\n"
                "            d[i % 2] = 0\n"
                "        if i > 5:\n"
                "            d[i % 2] += 5\n"
                "        else:\n"
                "            d[i % 2] += i\n"
                "    return d";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeListType(python::Type::I64);

    // create symbol table (add parameters and types)
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/control_flow_key_assign.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::I64, python::Type::I64);

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, DictionaryInputSimple) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(D):\n"
                "    D[0.0] += 1\n"
                "    D[1.0] += 2\n"
                "    D[2.0] = D[0] + D[1]\n"
                "    return D";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeDictionaryType(python::Type::F64, python::Type::F64);

    // create symbol table (add parameters and types)
    ast.addTypeHint("D", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/dict_input_simple.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::F64, python::Type::F64);

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, DictionaryInputControlFlow) {
    using namespace tuplex;
    using namespace std;

    auto code = "def f(D):\n"
                "    if D[0] < D[1]:\n"
                "        D[0] = D[1]\n"
                "        return D[1]\n"
                "    else:\n"
                "        D[0] = D[2]\n"
                "        return D[2]";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeDictionaryType(python::Type::I64, python::Type::F64);

    // create symbol table (add parameters and types)
    ast.addTypeHint("D", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/dict_input_control_flow.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::F64;

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, Everything) {
    // expected to fail; need to add support for dict_keys and dict_values
    using namespace tuplex;
    using namespace std;

    // example: we have a dictionary mapping countries to the continent they are in
    auto code = "def f(D):\n"
                "    continents = {\n"
                "        'Africa': 0,\n"
                "        'Asia': 0,\n"
                "        'Europe': 0,\n"
                "        'Other': 0\n"
                "    }\n"
                "    for continent in D.values():\n"
                "        if continent not in continents.keys():\n"
                "            continents['Other'] += 1\n"
                "        else:\n"
                "            continents[continent] += 1\n"
                "    return continents";
    
    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(code);

    // make input typing
    python::Type inputType = python::Type::makeDictionaryType(python::Type::STRING, python::Type::STRING);

    // create symbol table (add parameters and types)
    ast.addTypeHint("D", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("/home/rgoyal6/tuplex/tuplex/build/dictionary_asts/everything.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    python::Type expected_ret = python::Type::makeDictionaryType(python::Type::STRING, python::Type::I64);

    // check return type
    ASSERT_EQ(ast.getReturnType(), expected_ret);
}

TEST(DictionaryTyping, Count) {
    // expected to fail; need to add support for dict_keys
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
    python::Type inputType = python::Type::makeListType(python::Type::STRING);

    // create symbol table
    ast.addTypeHint("L", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("dict_count.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    ASSERT_EQ(ast.getReturnType(), python::Type::makeDictionaryType(python::Type::STRING, python::Type::I64));
}

TEST(DictionaryTyping, KeyView) {
    // expected to fail; need to add support for dict_keys
    using namespace tuplex;
    using namespace std;

    // could also use list((10, 20, 30)) e.g., or tuple(list(...)) -> needs speculation.

    // test count UDF
//    auto count_c = "def count_keys(x):\n"
//                   "    d = {'A':10, 'B': 10, x: 20}\n"
//                   "    return list(d.keys())";
    auto count_c = "def count_keys(x):\n"
                   "    d = {'A':10, 'B': 10, x: 20}\n"
                   "    return d.keys()";

    // parse code to AST
    auto ast = tuplex::codegen::AnnotatedAST();
    ast.parseString(count_c);

    // make typing
    python::Type inputType = python::Type::STRING;

    // create symbol table
    ast.addTypeHint("x", inputType);
    ast.defineTypes(codegen::DEFAULT_COMPILE_POLICY);

    // print type annotated ast
    GraphVizGraph graph;
    graph.createFromAST(ast.getFunctionAST(), true);
    graph.saveAsPDF("dict_count_keys.pdf");

    cout<<"return type of function is: "<<ast.getReturnType().desc()<<endl;

    ASSERT_EQ(ast.getReturnType(), python::Type::makeDictionaryType(python::Type::STRING, python::Type::I64));
}