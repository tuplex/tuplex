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
#include "DataSet.h"
#include <UDF.h>
#include <vector>
#include <Row.h>
#include <PythonHelpers.h>
#include <graphviz/GraphVizGraph.h>

using namespace tuplex;
using namespace std;

TEST(UDF, inputParams) {
    UDF udf("lambda a, b, c, d, e, f: a + f");
    auto params = udf.getInputParameters();

    vector<std::string> ref{"a", "b", "c", "d", "e", "f"};

    EXPECT_EQ(params.size(), ref.size());
    for(int i = 0; i < ref.size(); ++i) {
        EXPECT_EQ(ref[i], std::get<0>(params[i]));
    }
}

TEST(UDF, dictModeI) {
    // test some udfs and whether dict mode is correctly determined
    using namespace std;

    vector<tuple<string, vector<string>, bool>> test_udfs{
            make_tuple("lambda a: a * 20", vector<string>{}, false),
            make_tuple("lambda a, b, c: a + b + c", vector<string>{}, false),
            make_tuple("lambda x: x['test']", vector<string>{"test", "columnB"}, true),
            make_tuple("def f(a):\n"
            "\treturn a + 1\n", vector<string>{}, false),
            make_tuple("def g(a, b):\n"
            "\treturn a * b + 20", vector<string>{"colA", "colB"}, false),
            make_tuple("def f(x):\n"
            "\treturn x['a'] != x['b']", vector<string>{"a", "c", "b"}, true)};

    for(auto val : test_udfs) {
        std::cout<<"testing udf:\n"<<std::get<0>(val)<<std::endl;
        UDF udf(std::get<0>(val));
        EXPECT_TRUE(udf.rewriteDictAccessInAST(std::get<1>(val)));
        EXPECT_EQ(udf.dictMode(), std::get<2>(val));
    }

#warning "following UDF requires (but there are a couple more) runtime sampling to deduce certain characteristics"
    // make_tuple("def f(x):\n"
    //            "\ty = x\n"
    //            "\treturn y['test']\n", vector<string>{"a", "b", "test"}, true)
}

TEST(UDF, ComplexDictModeDetection) {
    using namespace std;
    using namespace tuplex;

    // this test checks whether with renaming etc., dict access is performed.
    auto codeI = "def f(x):\n"
                "  return x[x['col']]\n";
    UDF udfI(codeI);
    udfI.rewriteDictAccessInAST(vector<string>{"col"});
    // can't decide whether dictmode or not => i.e., could be both!
    cout<<"dict mode (I): "<<udfI.dictMode()<<endl;

    // reassigning var, should be able to rewrite to eliminate dict mode
    auto codeII = "def f(x):\n"
                  "  y = x\n"
                  "  return y['col']\n";
    UDF udfII(codeII);
    udfII.rewriteDictAccessInAST(vector<string>{"col"});
    // should not be dictmode, b.c. can rewrite
    cout<<"dict mode (II): "<<udfII.dictMode()<<endl;

}

TEST(UDF, NoneReturn) {
    UDF udf("lambda x: int(x) if len(x) > 0 else None");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::STRING)));

    EXPECT_EQ(udf.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::I64)));

    UDF udf2("lambda x: 'null' if x == None else x");
    udf2.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));
    EXPECT_EQ(udf2.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING)));


    // Note: the optimizer should determine the normal case!

    // todo: special check: x ==> this is difficult to compile!
    UDF udf3("lambda x: 'null' if x else x");
    udf3.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));
    EXPECT_EQ(udf3.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING)));

    UDF udf4("lambda x: None if x else x");
    udf4.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));
    EXPECT_EQ(udf4.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING)));

    // Now, type inference with functions. They expect real types -> type partially, i.e. in codegen exception should be generated!
    // TODO: could also type all of this via tracing!
    UDF udf5("lambda x: len(x) if x else 0");
    udf5.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));
    EXPECT_EQ(udf5.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::I64));
}

TEST(UDF, NoneTyping) {

    // special case: subscript on string option is string.
    UDF udf("lambda x: x[5]");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::makeOptionType(python::Type::STRING))));

    EXPECT_EQ(udf.getOutputSchema().getRowType(), python::Type::propagateToTupleType(python::Type::STRING));


    // @TODO
    // edge cases with None for slicing
    // lambda x: x[None:None:None] for x? --> None is ignored for indexing...
}


// deprecated, the null-value opt mechanism is now incorporated after cleaning
//TEST(UDF, DoNotTypeDeadIfBranches) {
////    auto fillInTimes_C = "def fillInTimesUDF(row):\n"
////                         "    ACTUAL_ELAPSED_TIME = row['ActualElapsedTime']\n"
////                         "    if row['DivReachedDest']:\n"
////                         "        if int(row['DivReachedDest']) > 0:\n"
////                         "            return float(row['DivActualElapsedTime'])\n"
////                         "        else:\n"
////                         "            return ACTUAL_ELAPSED_TIME\n"
////                         "    else:\n"
////                         "        return ACTUAL_ELAPSED_TIME";
//
//    using namespace std;
//    using namespace tuplex;
//
//    auto test_C = "def test(a, b, c):\n"
//                  "\td = a\n"
//                  "\tif b:\n"
//                  "\t\tif int(b) > 10:\n"
//                  "\t\t\treturn float(c)\n"
//                  "\t\telse:\n"
//                  "\t\t\treturn d\n"
//                  "\telse:\n"
//                  "\t\treturn d\n";
//
//    UDF udf(test_C);
//
//    // @TODO: do not remove nodes from AST but instead use annotations...
//    // ==> more efficient for the copying going on
//
//    // This here sho
//    // Typing is done in 3 attempts:
//    // 1.) attempt static typing, might fail
//    // 2.) attempt typing using sample, remove from AST nodes which do not comply
//
//    auto res = udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW,
//                                          python::Type::makeTupleType({
//                                                                              python::Type::F64,
//                                                                              python::Type::NULLVALUE,
//                                                                              python::Type::makeOptionType(python::Type::F64)
//                                                                      })), false);
//    ASSERT_EQ(res, false);
//
//    // now hint with annotating branches statically
//    // Note: this changes the internal AST!
//    res = udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW,
//                                     python::Type::makeTupleType({
//                                                                         python::Type::F64,
//                                                                         python::Type::NULLVALUE,
//                                                                         python::Type::makeOptionType(python::Type::F64)
//                                                                 })), true);
//    ASSERT_EQ(res, true);
//    EXPECT_EQ(udf.getOutputSchema().getRowType().desc(), "(f64)");
//
//    GraphVizGraph graph;
//    graph.createFromAST(udf.getAnnotatedAST().getFunctionAST(), true);
//    graph.saveAsPDF("udftest_ast.pdf");
//
//    // now trace with original UDF
//    // type using sample! => this enriches AST with information so it can be compiled...
//    udf = UDF(test_C);
//    python::initInterpreter();
//    res = udf.hintSchemaWithSample({python::rowToPython(Row(3.4, "22", 8.9)),
//                                    python::rowToPython(Row(3.4, Field::null(), 8.9)),
//                                    python::rowToPython(Row(Field::null()))},
//                                   python::Type::makeTupleType({python::Type::F64, python::Type::NULLVALUE, python::Type::F64}));
//    python::closeInterpreter();
//
//    auto env = make_shared<codegen::LLVMEnvironment>();
//    auto cf = udf.compile(*env.get(), true, true);
//    EXPECT_TRUE(cf.good());
//
//    ASSERT_EQ(res, true);
//    EXPECT_EQ(udf.getOutputSchema().getRowType().desc(), "(f64)");
//
//#ifndef NDEBUG
//    std::cout<<env->getIR()<<std::endl;
//#endif
//}


TEST(UDF, Rewrite) {
    // rewrite for projection pushdown, look into weird edge cases here
    using namespace tuplex;
    using namespace std;

    // auto unpacking works for weird case when
    // Row(Tuple("hello", "hel"))
    auto rA = Row(Tuple("hello", "hel"));
    auto typeA = rA.getRowType();

    UDF udf1("lambda a, b: b"); // can remove param a from list, because only b is accessed!
    UDF udf2("lambda a, b: a.startswith(b)"); // needs both parameters

    udf1.hintInputSchema(Schema(Schema::MemoryLayout::ROW, typeA));
    udf2.hintInputSchema(Schema(Schema::MemoryLayout::ROW, typeA));

    EXPECT_EQ(udf1.getOutputSchema().getRowType().desc(), "(str)"); // single string output
    EXPECT_EQ(udf2.getOutputSchema().getRowType().desc(), "(boolean)"); // bool

    udf1.rewriteParametersInAST({{1, 0}});
    EXPECT_EQ(udf1.getInputSchema().getRowType().desc(), "(str)");



    // let's assume we have dict access, and only one col survies. How should this be rewritten?
    UDF udf3("lambda x: x['colA']");
    vector<string> colsA{"colZ", "colY", "colA", "colX"};
    udf3.rewriteDictAccessInAST(colsA);
    udf3.hintInputSchema(Schema(Schema::MemoryLayout::ROW, Row(12, 13, "test", 15).getRowType()));

    cout<<udf3.getInputSchema().getRowType().desc()<<endl;
    udf3.rewriteParametersInAST({{2, 0}}); // only colA survives!
    cout<<udf3.getInputSchema().getRowType().desc()<<endl;
    cout<<udf3.getOutputSchema().getRowType().desc()<<endl;


    // how is the following typed & compiled?
    UDF udf4("def f(x):\n\treturn x[0]");
    udf4.hintInputSchema(Schema(Schema::MemoryLayout::ROW, Row(Tuple(10)).getRowType()));

    cout<<udf4.getInputSchema().getRowType().desc()<<endl;
    cout<<udf4.getOutputSchema().getRowType().desc()<<endl;


    cout<<Row(Tuple(-10, 20, -30, 25, -50)).toPythonString()<<endl; // ((-10,20,-30,25,-50),)

    // let's think about some rewrite examples

    // [1, 2, 3, 4] .map(lambda x: x * x) => rewriteMap: 0->0      (if source supports it!) => (i64) as result input type
    // [(1, 1), (2, 2)] .map(lambda x: x[1])  => rewriteMap 1 -> 0 (if source supports it!) => (i64) as result input type
    // [((1, 1),), ((2, 2),)]  .map(lambda x: x[1])  => rewriteMap: 0 -> 0 (no change here, i.e. first  ((i64, i64)) as result input type


    UDF udf6("lambda a, b: a.index(b)");
    udf6.hintInputSchema(Schema(Schema::MemoryLayout::ROW,Row(Tuple("hello", "llo")).getRowType()));
    auto v = udf6.getAccessedColumns();
    ASSERT_EQ(v.size(), 2);
}

TEST(UDF, RewriteSpecialCases) {
    // rewrite for projection pushdown, look into weird edge cases here
    using namespace tuplex;
    using namespace std;

    UDF udf("lambda x: x['a']");
    udf.rewriteDictAccessInAST({"c", "b", "a"}); // convert dict access...
    // hint schema ( tuple of 3!)
    auto type = python::Type::makeTupleType({python::Type::I64, python::Type::NULLVALUE, python::Type::F64});
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, type));
    EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "f64");
    EXPECT_EQ(udf.getOutputSchema().getRowType().desc(), "(f64)");

    // now perform projection pushdown, i.e. rewrite 2->0
    udf.rewriteParametersInAST({{2, 0}});
    EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "f64");
    EXPECT_EQ(udf.getOutputSchema().getRowType().desc(), "(f64)");

}

TEST(UDF, RetypeTest) {
    // check that multiple retypes using projections work easily

    using namespace tuplex;
    using namespace std;

    UDF udf("lambda x: x['a']");
    udf.rewriteDictAccessInAST({"c", "b", "a"}); // convert dict access...
    // hint schema ( tuple of 3!)
    auto type = python::Type::makeTupleType({python::Type::I64, python::Type::NULLVALUE, python::Type::F64});
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, type));
    EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "f64");
    EXPECT_EQ(udf.getOutputSchema().getRowType().desc(), "(f64)");

    std::cout<<udf.desc()<<std::endl;

    // now perform projection pushdown, i.e. rewrite 2->0
    udf.rewriteParametersInAST({{2, 0}});
    EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "f64");
    EXPECT_EQ(udf.getOutputSchema().getRowType().desc(), "(f64)");

    // now retype using string
    auto new_type = python::Type::makeTupleType({python::Type::I64, python::Type::I64, python::Type::I64});

    bool rc = false;

    // retype using original
    rc = udf.retype(new_type);
    EXPECT_TRUE(rc);
    //udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, new_type));
    EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "i64");
    EXPECT_EQ(udf.getOutputSchema().getRowType().desc(), "(i64)");

    // now check when using retyping using a tinier type.
    auto new_type_proj = python::Type::makeTupleType({python::Type::NULLVALUE});
    rc = udf.retype(new_type_proj);
    EXPECT_TRUE(rc);
    EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "null");
    EXPECT_EQ(udf.getOutputSchema().getRowType().desc(), "(null)");
}

TEST(UDF, RetypeWithChangedColumnOrder) {
    // perform some more complex retype scenarios (i.e., column names missing, order changed etc.)
    // --> need to do this to avoid issues with retyping later on!

    auto code = "lambda x: (x['A'], x['B'], x['C'], x['D'], x['E'])";

    // type first with right row type
    // note that rewrite with colunm names changes order? -> should it?
    // -> there should be a stable rewrite when it comes to column names!!!
    // maybe introduce proper Row type!
    // -> that would solve a bunch of issues...
    // yeah, that's a good idea...
    UDF udf(code);
    udf.rewriteDictAccessInAST({"A", "B", "C", "D", "E"});
    auto row_type = python::Type::makeTupleType({python::Type::I64, python::Type::NULLVALUE, python::Type::F64, python::Type::STRING, python::Type::BOOLEAN});
    udf.retype(row_type);


}

TEST(UDF, InputColumnCount) {
    using namespace tuplex;
    using namespace std;

    // single param
    {
        UDF udf("lambda x: x");
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::I64));
        EXPECT_EQ(udf.inputColumnCount(), 1);
        EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "i64");
        udf.removeTypes();
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::I64})));
        EXPECT_EQ(udf.inputColumnCount(), 1);
        EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "i64");

        udf.removeTypes();
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::makeTupleType({python::Type::makeTupleType({python::Type::I64})})));
        EXPECT_EQ(udf.inputColumnCount(), 1);
        EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "(i64)");
    }

    // multi param
    {
        auto tuple_type = python::Type::makeTupleType({python::Type::I64, python::Type::BOOLEAN, python::Type::F64, python::Type::STRING});
        UDF udf("lambda x: (x[0], x[2])");
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, tuple_type));
        EXPECT_EQ(udf.inputColumnCount(), 4);
        EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "(i64,f64)");
        udf = UDF("lambda a, b, c, d: (a, c)");
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, tuple_type));
        EXPECT_EQ(udf.inputColumnCount(), 4);
        EXPECT_EQ(udf.getAnnotatedAST().getReturnType().desc(), "(i64,f64)");
    }
}

TEST(UDF, SymbolTableIfLogic) {

    // check here symbol table can handle type reassignments properly
    // # if/else changes type of variable
    // def f(x):
    //     # can unify types of x here...
    //     if x > 10:
    //         x = 'hello'
    //     else:
    //         x = 'test'
    //     return x
    //
    //
    // print(f(5)) # 'test'
    // print(f(11)) # 'hello'

    // and
    // # if/else changes type of variable
    // def f(x):
    //     # can't unify types => use speculation.
    //     if x > 10:
    //         x = 'hello'
    //     else:
    //         x = x * x
    //     return x
    //
    //
    // print(f(5)) # 25
    // print(f(11)) # 'hello'

    // are the two test cases.

    // => add one more nesting level, also partial

    using namespace tuplex;

    // i.e. same branch behavior overrides value of integer!
    auto code = "def f(x):\n"
                "   if x > 10:\n"
                "       x = 'two digits'\n"  // => this should yield a scope. => overlap with higher scopes?
                "   else:\n"
                "       x = 'one digit'\n"   // => this should yield a scope. => overlap with higher scopes?
                "   return x";

    // hinting this with int should lead to str return!
    UDF udf(code);
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::I64)));
    auto desc = udf.getOutputSchema().getRowType().desc();

    EXPECT_EQ(desc, "(str)");

    auto codeBad = "def f(x):\n"
                   "    # can't unify types => use speculation.\n"
                   "    if x > 10:\n"
                   "        x = 'hello'\n"
                   "    else:\n"
                   "        x = x * x\n"
                   "    return x";

    try {
        UDF udf(codeBad);
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::I64)));
    } catch(const std::exception& e) {
        EXPECT_EQ(std::string(e.what()), "type conflict, not implemented yet. Need to speculate here and add guards!");
    }
}

TEST(UDF, ModuleCall) {
    // re.search

    // i.e. if we have something like

    // re.search the we search re for member search

    // for os.path.join
    // it's first os.path (i.e. find member path of os)
    // then: find member join of whatever is previously returned i.e. `os.path`

    // i.e. when using re.search
    // => re should return a type of re_module => that sounds right
    // then there are subtypes registered under re.search, i.e. the function type of that function!


    // one symbol table per module
    // i.e. cf.

    // https://courses.cs.washington.edu/courses/cse401/13wi/lectures/lect15.pdf

    // https://docs.python.org/3/reference/expressions.html#attribute-references

    using namespace tuplex;
    ClosureEnvironment ce;
    ce.importModuleAs("re", "re");
    UDF udf("lambda x: re.search('\\d+', x)", "", ce);
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::STRING)));
    auto desc = udf.getOutputSchema().getRowType().desc();
    EXPECT_EQ(desc, "(Option[matchobject])");
}

TEST(UDF, RewriteRowType) {
    // rewrite map: 11 -> 7,10 -> 6,9 -> 5,8 -> 4,6 -> 3,4 -> 2,3 -> 1,0 -> 0,
    //udf input schema:  Row['type'->Option[str],'created_at'->Option[str],'payload'->Option[Struct[(str,'action'=>str),(str,'before'=>str),(str,'comment'=>Struct[(str,'author_association'=>str),(str,'body'=>str),(str,'commit_id'=>str),(str,'created_at'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'issue_url'=>str),(str,'line'=>Option[i64]),(str,'node_id'=>str),(str,'path'=>Option[str]),(str,'performed_via_github_app'=>null),(str,'position'=>Option[i64]),(str,'reactions'=>Struct[(str,'url'=>str),(str,'total_count'=>i64),(str,'+1'=>i64),(str,'-1'=>i64),(str,'laugh'=>i64),(str,'hooray'=>i64),(str,'confused'=>i64),(str,'heart'=>i64),(str,'rocket'=>i64),(str,'eyes'=>i64)]),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'commits'=>List[Struct[(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),(str,'distinct'=>bool),(str,'message'->str),(str,'sha'->str),(str,'url'->str)]]),(str,'description'=>Option[str]),(str,'distinct_size'=>i64),(str,'forkee'=>Struct[(str,'allow_forking'=>bool),(str,'archive_url'=>str),(str,'archived'=>bool),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>str),(str,'disabled'=>bool),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>bool),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>bool),(str,'has_issues'=>bool),(str,'has_pages'=>bool),(str,'has_projects'=>bool),(str,'has_wiki'=>bool),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>bool),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>Option[str]),(str,'languages_url'=>str),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'spdx_id'=>str),(str,'url'=>str),(str,'node_id'=>str)]),(str,'master_branch'=>Option[str]),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>bool),(str,'public'=>bool),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'gist'=>Struct[(str,'comments'=>i64),(str,'created_at'=>str),(str,'description'=>str),(str,'files'=>{}),(str,'git_pull_url'=>str),(str,'git_push_url'=>str),(str,'html_url'=>str),(str,'id'=>str),(str,'public'=>bool),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)])]),(str,'head'=>str),(str,'issue'=>Struct[(str,'active_lock_reason'=>null),(str,'assignee'=>Option[Struct[(str,'avatar_url'->str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'->str),(str,'html_url'=>str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'->str)]]),(str,'assignees'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'author_association'=>str),(str,'body'=>Option[str]),(str,'closed_at'=>Option[str]),(str,'comments'=>i64),(str,'comments_url'=>str),(str,'created_at'=>str),(str,'events_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'labels'=>List[Struct[(str,'color'->str),(str,'default'=>bool),(str,'description'=>str),(str,'id'=>i64),(str,'name'->str),(str,'node_id'=>str),(str,'url'->str)]]),(str,'labels_url'=>str),(str,'locked'=>bool),(str,'milestone'=>Option[Struct[(str,'closed_at'=>null),(str,'closed_issues'->i64),(str,'created_at'->str),(str,'creator'->Struct[(str,'avatar_url'->str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'->str),(str,'html_url'=>str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'->str)]),(str,'description'->str),(str,'due_on'->Option[str]),(str,'html_url'=>str),(str,'id'=>i64),(str,'labels_url'=>str),(str,'node_id'=>str),(str,'number'->i64),(str,'open_issues'->i64),(str,'state'->str),(str,'title'->str),(str,'updated_at'=>str),(str,'url'->str)]]),(str,'node_id'=>str),(str,'number'=>i64),(str,'performed_via_github_app'=>null),(str,'pull_request'=>Struct[(str,'diff_url'=>Option[str]),(str,'html_url'=>Option[str]),(str,'patch_url'=>Option[str]),(str,'url'=>str)]),(str,'reactions'=>Struct[(str,'url'=>str),(str,'total_count'=>i64),(str,'+1'=>i64),(str,'-1'=>i64),(str,'laugh'=>i64),(str,'hooray'=>i64),(str,'confused'=>i64),(str,'heart'=>i64),(str,'rocket'=>i64),(str,'eyes'=>i64)]),(str,'repository_url'=>str),(str,'state'=>str),(str,'timeline_url'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'legacy'=>Struct[(str,'action'=>str),(str,'comment_id'=>i64),(str,'commit'=>str),(str,'desc'=>str),(str,'head'=>str),(str,'id'=>i64),(str,'issue'=>i64),(str,'issue_id'=>i64),(str,'name'=>str),(str,'number'=>i64),(str,'push_id'=>i64),(str,'ref'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'url'=>str)]),(str,'master_branch'=>str),(str,'member'=>Struct[(str,'gravatar_id'=>str),(str,'avatar_url'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'login'=>str)]),(str,'number'=>i64),(str,'pages'=>List[Struct[(str,'sha'->str),(str,'title'->str),(str,'action'->str),(str,'page_name'->str),(str,'summary'->null)]]),(str,'pull_request'=>Struct[(str,'_links'=>Struct[(str,'self'=>Struct[(str,'href'=>str)]),(str,'html'=>Struct[(str,'href'=>str)]),(str,'issue'=>Struct[(str,'href'=>str)]),(str,'comments'=>Struct[(str,'href'=>str)]),(str,'review_comments'=>Struct[(str,'href'=>str)]),(str,'review_comment'=>Struct[(str,'href'=>str)]),(str,'commits'=>Struct[(str,'href'=>str)]),(str,'statuses'=>Struct[(str,'href'=>str)])]),(str,'active_lock_reason'=>null),(str,'additions'=>i64),(str,'assignee'=>Option[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'assignees'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'author_association'=>str),(str,'auto_merge'=>null),(str,'base'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'repo'=>Struct[(str,'allow_forking'=>bool),(str,'archive_url'=>str),(str,'archived'=>bool),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>Option[str]),(str,'disabled'=>bool),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>bool),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>bool),(str,'has_issues'=>bool),(str,'has_pages'=>bool),(str,'has_projects'=>bool),(str,'has_wiki'=>bool),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>bool),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>str),(str,'languages_url'=>str),(str,'license'=>Option[Struct[(str,'key'->str),(str,'name'->str),(str,'node_id'->str),(str,'spdx_id'->str),(str,'url'->Option[str])]]),(str,'master_branch'=>null),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>bool),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'sha'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'body'=>Option[str]),(str,'changed_files'=>i64),(str,'closed_at'=>Option[str]),(str,'comments'=>i64),(str,'comments_url'=>str),(str,'commits'=>i64),(str,'commits_url'=>str),(str,'created_at'=>str),(str,'deletions'=>i64),(str,'diff_url'=>str),(str,'draft'=>bool),(str,'head'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'repo'=>Struct[(str,'allow_forking'=>bool),(str,'archive_url'=>str),(str,'archived'=>bool),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>Option[str]),(str,'disabled'=>bool),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>bool),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>bool),(str,'has_issues'=>bool),(str,'has_pages'=>bool),(str,'has_projects'=>bool),(str,'has_wiki'=>bool),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>bool),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>str),(str,'languages_url'=>str),(str,'license'=>Option[Struct[(str,'key'->str),(str,'name'->str),(str,'node_id'->str),(str,'spdx_id'->str),(str,'url'->Option[str])]]),(str,'master_branch'=>null),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>bool),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'sha'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'html_url'=>str),(str,'id'=>i64),(str,'issue_url'=>str),(str,'labels'=>List[Struct[(str,'id'->i64),(str,'node_id'->str),(str,'url'->str),(str,'name'->str),(str,'color'->str),(str,'default'->bool),(str,'description'->str)]]),(str,'locked'=>bool),(str,'maintainer_can_modify'=>bool),(str,'merge_commit_sha'=>Option[str]),(str,'mergeable'=>Option[bool]),(str,'mergeable_state'=>str),(str,'merged'=>bool),(str,'merged_at'=>Option[str]),(str,'merged_by'=>Option[Struct[(str,'avatar_url'->str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'->str),(str,'html_url'=>str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'->str)]]),(str,'milestone'=>null),(str,'node_id'=>str),(str,'number'=>i64),(str,'patch_url'=>str),(str,'rebaseable'=>Option[bool]),(str,'requested_reviewers'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'requested_teams'=>[]),(str,'review_comment_url'=>str),(str,'review_comments'=>i64),(str,'review_comments_url'=>str),(str,'state'=>str),(str,'statuses_url'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'push_id'=>i64),(str,'pusher_type'=>str),(str,'ref'=>Option[str]),(str,'ref_type'=>str),(str,'review'=>Struct[(str,'id'=>i64),(str,'node_id'=>str),(str,'user'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'body'=>str),(str,'commit_id'=>str),(str,'submitted_at'=>str),(str,'state'=>str),(str,'html_url'=>str),(str,'pull_request_url'=>str),(str,'author_association'=>str),(str,'_links'=>Struct[(str,'html'=>Struct[(str,'href'=>str)]),(str,'pull_request'=>Struct[(str,'href'=>str)])])]),(str,'size'=>i64)]],'repo'->Option[Struct[(str,'id'=>i64),(str,'name'=>str),(str,'url'=>str)]],'year'->i64,'repo_id'->i64,'commits'->List[Struct[(str,'sha'->str),(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),(str,'url'->str),(str,'message'->str)]],'number_of_commits'->i64]
    //udf output schema: (Option[str],i64,i64,i64)
    using namespace tuplex;

    // create UDF with row type and perform rewrite then
    auto code = generate_python_code_for_select_columns_udf({11, 10, 9, 8, 6, 4, 3, 0});

    auto encoded_row_type_str = "Row['type'->Option[str],'created_at'->Option[str],'payload'->Option[Struct[(str,'action'=>str),(str,'before'=>str),(str,'comment'=>Struct[(str,'author_association'=>str),(str,'body'=>str),(str,'commit_id'=>str),(str,'created_at'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'issue_url'=>str),(str,'line'=>Option[i64]),(str,'node_id'=>str),(str,'path'=>Option[str]),(str,'performed_via_github_app'=>null),(str,'position'=>Option[i64]),(str,'reactions'=>Struct[(str,'url'=>str),(str,'total_count'=>i64),(str,'+1'=>i64),(str,'-1'=>i64),(str,'laugh'=>i64),(str,'hooray'=>i64),(str,'confused'=>i64),(str,'heart'=>i64),(str,'rocket'=>i64),(str,'eyes'=>i64)]),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'commits'=>List[Struct[(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),(str,'distinct'=>bool),(str,'message'->str),(str,'sha'->str),(str,'url'->str)]]),(str,'description'=>Option[str]),(str,'distinct_size'=>i64),(str,'forkee'=>Struct[(str,'allow_forking'=>bool),(str,'archive_url'=>str),(str,'archived'=>bool),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>str),(str,'disabled'=>bool),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>bool),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>bool),(str,'has_issues'=>bool),(str,'has_pages'=>bool),(str,'has_projects'=>bool),(str,'has_wiki'=>bool),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>bool),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>Option[str]),(str,'languages_url'=>str),(str,'license'=>Struct[(str,'key'=>str),(str,'name'=>str),(str,'spdx_id'=>str),(str,'url'=>str),(str,'node_id'=>str)]),(str,'master_branch'=>Option[str]),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>bool),(str,'public'=>bool),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'gist'=>Struct[(str,'comments'=>i64),(str,'created_at'=>str),(str,'description'=>str),(str,'files'=>{}),(str,'git_pull_url'=>str),(str,'git_push_url'=>str),(str,'html_url'=>str),(str,'id'=>str),(str,'public'=>bool),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'url'=>str)])]),(str,'head'=>str),(str,'issue'=>Struct[(str,'active_lock_reason'=>null),(str,'assignee'=>Option[Struct[(str,'avatar_url'->str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'->str),(str,'html_url'=>str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'->str)]]),(str,'assignees'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'author_association'=>str),(str,'body'=>Option[str]),(str,'closed_at'=>Option[str]),(str,'comments'=>i64),(str,'comments_url'=>str),(str,'created_at'=>str),(str,'events_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'labels'=>List[Struct[(str,'color'->str),(str,'default'=>bool),(str,'description'=>str),(str,'id'=>i64),(str,'name'->str),(str,'node_id'=>str),(str,'url'->str)]]),(str,'labels_url'=>str),(str,'locked'=>bool),(str,'milestone'=>Option[Struct[(str,'closed_at'=>null),(str,'closed_issues'->i64),(str,'created_at'->str),(str,'creator'->Struct[(str,'avatar_url'->str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'->str),(str,'html_url'=>str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'->str)]),(str,'description'->str),(str,'due_on'->Option[str]),(str,'html_url'=>str),(str,'id'=>i64),(str,'labels_url'=>str),(str,'node_id'=>str),(str,'number'->i64),(str,'open_issues'->i64),(str,'state'->str),(str,'title'->str),(str,'updated_at'=>str),(str,'url'->str)]]),(str,'node_id'=>str),(str,'number'=>i64),(str,'performed_via_github_app'=>null),(str,'pull_request'=>Struct[(str,'diff_url'=>Option[str]),(str,'html_url'=>Option[str]),(str,'patch_url'=>Option[str]),(str,'url'=>str)]),(str,'reactions'=>Struct[(str,'url'=>str),(str,'total_count'=>i64),(str,'+1'=>i64),(str,'-1'=>i64),(str,'laugh'=>i64),(str,'hooray'=>i64),(str,'confused'=>i64),(str,'heart'=>i64),(str,'rocket'=>i64),(str,'eyes'=>i64)]),(str,'repository_url'=>str),(str,'state'=>str),(str,'timeline_url'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'legacy'=>Struct[(str,'action'=>str),(str,'comment_id'=>i64),(str,'commit'=>str),(str,'desc'=>str),(str,'head'=>str),(str,'id'=>i64),(str,'issue'=>i64),(str,'issue_id'=>i64),(str,'name'=>str),(str,'number'=>i64),(str,'push_id'=>i64),(str,'ref'=>str),(str,'shas'=>List[List[str]]),(str,'size'=>i64),(str,'url'=>str)]),(str,'master_branch'=>str),(str,'member'=>Struct[(str,'gravatar_id'=>str),(str,'avatar_url'=>str),(str,'url'=>str),(str,'id'=>i64),(str,'login'=>str)]),(str,'number'=>i64),(str,'pages'=>List[Struct[(str,'sha'->str),(str,'title'->str),(str,'action'->str),(str,'page_name'->str),(str,'summary'->null)]]),(str,'pull_request'=>Struct[(str,'_links'=>Struct[(str,'self'=>Struct[(str,'href'=>str)]),(str,'html'=>Struct[(str,'href'=>str)]),(str,'issue'=>Struct[(str,'href'=>str)]),(str,'comments'=>Struct[(str,'href'=>str)]),(str,'review_comments'=>Struct[(str,'href'=>str)]),(str,'review_comment'=>Struct[(str,'href'=>str)]),(str,'commits'=>Struct[(str,'href'=>str)]),(str,'statuses'=>Struct[(str,'href'=>str)])]),(str,'active_lock_reason'=>null),(str,'additions'=>i64),(str,'assignee'=>Option[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'assignees'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'author_association'=>str),(str,'auto_merge'=>null),(str,'base'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'repo'=>Struct[(str,'allow_forking'=>bool),(str,'archive_url'=>str),(str,'archived'=>bool),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>Option[str]),(str,'disabled'=>bool),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>bool),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>bool),(str,'has_issues'=>bool),(str,'has_pages'=>bool),(str,'has_projects'=>bool),(str,'has_wiki'=>bool),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>bool),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>str),(str,'languages_url'=>str),(str,'license'=>Option[Struct[(str,'key'->str),(str,'name'->str),(str,'node_id'->str),(str,'spdx_id'->str),(str,'url'->Option[str])]]),(str,'master_branch'=>null),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>bool),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'sha'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'body'=>Option[str]),(str,'changed_files'=>i64),(str,'closed_at'=>Option[str]),(str,'comments'=>i64),(str,'comments_url'=>str),(str,'commits'=>i64),(str,'commits_url'=>str),(str,'created_at'=>str),(str,'deletions'=>i64),(str,'diff_url'=>str),(str,'draft'=>bool),(str,'head'=>Struct[(str,'label'=>str),(str,'ref'=>str),(str,'repo'=>Struct[(str,'allow_forking'=>bool),(str,'archive_url'=>str),(str,'archived'=>bool),(str,'assignees_url'=>str),(str,'blobs_url'=>str),(str,'branches_url'=>str),(str,'clone_url'=>str),(str,'collaborators_url'=>str),(str,'comments_url'=>str),(str,'commits_url'=>str),(str,'compare_url'=>str),(str,'contents_url'=>str),(str,'contributors_url'=>str),(str,'created_at'=>str),(str,'default_branch'=>str),(str,'deployments_url'=>str),(str,'description'=>Option[str]),(str,'disabled'=>bool),(str,'downloads_url'=>str),(str,'events_url'=>str),(str,'fork'=>bool),(str,'forks'=>i64),(str,'forks_count'=>i64),(str,'forks_url'=>str),(str,'full_name'=>str),(str,'git_commits_url'=>str),(str,'git_refs_url'=>str),(str,'git_tags_url'=>str),(str,'git_url'=>str),(str,'has_downloads'=>bool),(str,'has_issues'=>bool),(str,'has_pages'=>bool),(str,'has_projects'=>bool),(str,'has_wiki'=>bool),(str,'homepage'=>Option[str]),(str,'hooks_url'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'is_template'=>bool),(str,'issue_comment_url'=>str),(str,'issue_events_url'=>str),(str,'issues_url'=>str),(str,'keys_url'=>str),(str,'labels_url'=>str),(str,'language'=>str),(str,'languages_url'=>str),(str,'license'=>Option[Struct[(str,'key'->str),(str,'name'->str),(str,'node_id'->str),(str,'spdx_id'->str),(str,'url'->Option[str])]]),(str,'master_branch'=>null),(str,'merges_url'=>str),(str,'milestones_url'=>str),(str,'mirror_url'=>null),(str,'name'=>str),(str,'node_id'=>str),(str,'notifications_url'=>str),(str,'open_issues'=>i64),(str,'open_issues_count'=>i64),(str,'owner'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)]),(str,'private'=>bool),(str,'pulls_url'=>str),(str,'pushed_at'=>str),(str,'releases_url'=>str),(str,'size'=>i64),(str,'ssh_url'=>str),(str,'stargazers_count'=>i64),(str,'stargazers_url'=>str),(str,'statuses_url'=>str),(str,'subscribers_url'=>str),(str,'subscription_url'=>str),(str,'svn_url'=>str),(str,'tags_url'=>str),(str,'teams_url'=>str),(str,'topics'=>[]),(str,'trees_url'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'visibility'=>str),(str,'watchers'=>i64),(str,'watchers_count'=>i64)]),(str,'sha'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'html_url'=>str),(str,'id'=>i64),(str,'issue_url'=>str),(str,'labels'=>List[Struct[(str,'id'->i64),(str,'node_id'->str),(str,'url'->str),(str,'name'->str),(str,'color'->str),(str,'default'->bool),(str,'description'->str)]]),(str,'locked'=>bool),(str,'maintainer_can_modify'=>bool),(str,'merge_commit_sha'=>Option[str]),(str,'mergeable'=>Option[bool]),(str,'mergeable_state'=>str),(str,'merged'=>bool),(str,'merged_at'=>Option[str]),(str,'merged_by'=>Option[Struct[(str,'avatar_url'->str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'->str),(str,'html_url'=>str),(str,'id'->i64),(str,'login'->str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'->str)]]),(str,'milestone'=>null),(str,'node_id'=>str),(str,'number'=>i64),(str,'patch_url'=>str),(str,'rebaseable'=>Option[bool]),(str,'requested_reviewers'=>List[Struct[(str,'login'->str),(str,'id'->i64),(str,'node_id'->str),(str,'avatar_url'->str),(str,'gravatar_id'->str),(str,'url'->str),(str,'html_url'->str),(str,'followers_url'->str),(str,'following_url'->str),(str,'gists_url'->str),(str,'starred_url'->str),(str,'subscriptions_url'->str),(str,'organizations_url'->str),(str,'repos_url'->str),(str,'events_url'->str),(str,'received_events_url'->str),(str,'type'->str),(str,'site_admin'->bool)]]),(str,'requested_teams'=>[]),(str,'review_comment_url'=>str),(str,'review_comments'=>i64),(str,'review_comments_url'=>str),(str,'state'=>str),(str,'statuses_url'=>str),(str,'title'=>str),(str,'updated_at'=>str),(str,'url'=>str),(str,'user'=>Struct[(str,'avatar_url'=>str),(str,'events_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'gravatar_id'=>str),(str,'html_url'=>str),(str,'id'=>i64),(str,'login'=>str),(str,'node_id'=>str),(str,'organizations_url'=>str),(str,'received_events_url'=>str),(str,'repos_url'=>str),(str,'site_admin'=>bool),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'type'=>str),(str,'url'=>str)])]),(str,'push_id'=>i64),(str,'pusher_type'=>str),(str,'ref'=>Option[str]),(str,'ref_type'=>str),(str,'review'=>Struct[(str,'id'=>i64),(str,'node_id'=>str),(str,'user'=>Struct[(str,'login'=>str),(str,'id'=>i64),(str,'node_id'=>str),(str,'avatar_url'=>str),(str,'gravatar_id'=>str),(str,'url'=>str),(str,'html_url'=>str),(str,'followers_url'=>str),(str,'following_url'=>str),(str,'gists_url'=>str),(str,'starred_url'=>str),(str,'subscriptions_url'=>str),(str,'organizations_url'=>str),(str,'repos_url'=>str),(str,'events_url'=>str),(str,'received_events_url'=>str),(str,'type'=>str),(str,'site_admin'=>bool)]),(str,'body'=>str),(str,'commit_id'=>str),(str,'submitted_at'=>str),(str,'state'=>str),(str,'html_url'=>str),(str,'pull_request_url'=>str),(str,'author_association'=>str),(str,'_links'=>Struct[(str,'html'=>Struct[(str,'href'=>str)]),(str,'pull_request'=>Struct[(str,'href'=>str)])])]),(str,'size'=>i64)]],'repo'->Option[Struct[(str,'id'=>i64),(str,'name'=>str),(str,'url'=>str)]],'year'->i64,'repo_id'->i64,'commits'->List[Struct[(str,'sha'->str),(str,'author'->Struct[(str,'name'->str),(str,'email'->str)]),(str,'url'->str),(str,'message'->str)]],'number_of_commits'->i64]";
    auto decoded_row_type = python::Type::decode(encoded_row_type_str);

    ASSERT_TRUE(decoded_row_type.isRowType());
    EXPECT_GE(decoded_row_type.get_column_count(), 12);
    UDF udf(code);

    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, decoded_row_type));
}

TEST(UDF, ConstantFolding) {
    using namespace tuplex;

    auto udf_code = "def fill_in_delays(row):\n"
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
                    "            return {'year' : year,"
                    "                    'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'arr_delay': None,"
                    "                    'carrier_delay' : None}\n"
                    "        elif arr_delay < 0.:\n"
                    "            # stays None, because flight arrived early\n"
                    "            # if diverted though, need to add everything to div_arr_delay\n"
                    "            return {'year' : year,"
                    "                    'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'arr_delay': row['ARR_DELAY'],\n"
                    "                    'carrier_delay' : None}\n"
                    "        elif arr_delay < 5.:\n"
                    "            # it's an ontime flight, just attribute any delay to the carrier\n"
                    "            carrier_delay = arr_delay\n"
                    "            # set the rest to 0\n"
                    "            # ....\n"
                    "            return {'year' : year,"
                    "                    'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'arr_delay': row['ARR_DELAY'],\n"
                    "                    'carrier_delay' : carrier_delay}\n"
                    "        else:\n"
                    "            # use model to determine everything and set into (join with weather data?)\n"
                    "            # i.e., extract here a couple additional columns & use them for features etc.!\n"
                    "            crs_dep_time = float(row['CRS_DEP_TIME'])\n"
                    "            crs_arr_time = float(row['CRS_ARR_TIME'])\n"
                    "            crs_elapsed_time = float(row['CRS_ELAPSED_TIME'])\n"
                    "            carrier_delay = 1024 + 2.7 * crs_dep_time - 0.2 * crs_elapsed_time\n"
                    "            return {'year' : year,"
                    "                    'month' : month,\n"
                    "                    'day' : row['DAY_OF_MONTH'],\n"
                    "                    'arr_delay': row['ARR_DELAY'],\n"
                    "                    'carrier_delay' : carrier_delay}\n"
                    "    else:\n"
                    "        # just return it as is\n"
                    "        return {'year' : year,"
                    "                'month' : month,\n"
                    "                'day' : row['DAY_OF_MONTH'],\n"
                    "                'arr_delay': row['ARR_DELAY'],\n"
                    "                'carrier_delay' : row['CARRIER_DELAY']}";

    // @TODO: fix on this simplified version the constant folding...
}


// this test fails, postponed for now. There're more important things todo.
//TEST(UDF, RewriteSlice) {
//    UDF udf5("lambda x: x[1:3:0]");
//    udf5.hintInputSchema(Schema(Schema::MemoryLayout::ROW, Row(Tuple(-10, 20, -30, 25, -50)).getRowType()));
//
//    EXPECT_EQ("((i64,i64,i64,i64,i64))", udf5.getInputSchema().getRowType().desc());
//    // rewrite using rewriteMap 0:0
//    udf5.rewriteParametersInAST({{0, 0}});
//
//    EXPECT_EQ("((i64,i64,i64,i64,i64))", udf5.getInputSchema().getRowType().desc());
//
//    cout<<udf5.getInputSchema().getRowType().desc()<<endl;
//    cout<<udf5.getOutputSchema().getRowType().desc()<<endl;
//}