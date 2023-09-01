//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 3/29/2023                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "TestUtils.h"
#include <Context.h>
#include <PythonHelpers.h>
#include <logical/FilterOperator.h>
#include <visitors/FilterBreakdownVisitor.h>
#include <parser/Parser.h>
#include <visitors/TypeAnnotatorVisitor.h>

// need for these tests a running python interpreter, so spin it up
class LogicalOperatorTest : public PyTest {};

// check whether removing operators works appropriately
TEST_F(LogicalOperatorTest, RemoveOperator) {
    using namespace tuplex;
    using namespace std;
    std::string test_path = "../resources/hyperspecialization/github_daily/*.json.sample";
    std::string test_output_path = "./local-exp/json";
    ContextOptions co = microTestOptions();
    SamplingMode sm = DEFAULT_SAMPLING_MODE;
    auto jsonop = std::shared_ptr<FileInputOperator>(FileInputOperator::fromJSON(test_path,
                                                                                 true, true, co, sm));

    auto repo_id_code = "def extract_repo_id(row):\n"
                        "\tif 2012 <= row['year'] <= 2014:\n"
                        "\t\treturn row['repository']['id']\n"
                        "\telse:\n"
                        "\t\treturn row['repo']['id']\n";

    auto wop1 = std::shared_ptr<LogicalOperator>(new WithColumnOperator(jsonop, jsonop->columns(), "year", UDF("lambda x: int(x['created_at'].split('-')[0])")));
    auto wop2 = std::shared_ptr<LogicalOperator>(new WithColumnOperator(wop1, wop1->columns(), "repo_id", UDF(repo_id_code)));
    auto filter_op = std::shared_ptr<LogicalOperator>(new FilterOperator(wop2, UDF("lambda x: x['type'] == 'ForkEvent'"), wop2->columns()));

    // parent of mop is filter
    auto mop = new MapOperator(filter_op, UDF("lambda t: (t['type'],t['repo_id'],t['year'])"), filter_op->columns());
    mop->setOutputColumns(std::vector<std::string>{"type", "repo_id", "year"});
    auto sop = std::shared_ptr<LogicalOperator>(mop);
    auto fop = std::make_shared<FileOutputOperator>(sop, test_output_path, UDF(""), "csv", FileFormat::OUTFMT_CSV, defaultCSVOutputOptions());


    // check now removal...
    filter_op->remove();
    ASSERT_EQ(wop2->children().size(), 1);
    EXPECT_EQ(wop2->children().front()->name(), "map");
    ASSERT_EQ(mop->parents().size(), 1);
    EXPECT_EQ(mop->parent()->name(), "withColumn");
}