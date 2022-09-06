//
// Created by Leonhard Spiegelberg on 9/5/22.
//

#include "TestUtils.h"
#include "JsonStatistic.h"

#include <AccessPathVisitor.h>

class JSONPathSelection : public PyTest {};

TEST_F(JSONPathSelection, BasicPathSelection) {
    using namespace tuplex;
    using namespace std;

    // fetch the type of some github rows
    std::string gh_path = "../resources/ndjson/github.json";
    std::string data = fileToString(gh_path);
    std::vector<std::vector<std::string>> column_names;
    auto rows = parseRowsFromJSON(data, &column_names, false);
    ASSERT_FALSE(rows.empty());

    cout<<"type of first row: \n"<<rows.front().getRowType().desc()<<endl;

    auto input_row_type = rows.front().getRowType();

    UDF udf("lambda x: x['repo']['url']");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, input_row_type));
    auto output_row_type = udf.getOutputSchema().getRowType();
    cout<<"output type of UDF: "<<output_row_type.desc()<<endl;

    // detect paths
    AccessPathVisitor apv;
    auto root = udf.getAnnotatedAST().getFunctionAST();
    root->accept(apv);
    auto indices = apv.getAccessedIndices();
    cout<<"visited indices: "<<indices<<endl;

    // need to detect path access using identifier (single + multiple columns)
    // e.g.,
    // x['repo']['url']

    // need to detect path access using multiple variables, i.e. more complex paths?
    // y = x['repo']
    // return y['url']
    // => i.e., can this be restricted?
    // what about redefinitions?d
    // not really needed, use simple method for now...
    // --> maybe track through assignments?
    // if/else control flow is critical. Yet, that can be solved by simply following
    // only valid control flow.
    // also specialize according to normal case!
    // i.e., maintain nametable pointing to access paths (?)
    // def f(x):
    //    y = x['repo']
    //    return y['url']
    // => this should make clear that only path 'repo'.'url' is needed.
    // for literals/constants from global enclosure allow tracking as well!
    // KEY = 'repo'
    // def f(x):
    //   return x[KEY]
    // should work as well.

}