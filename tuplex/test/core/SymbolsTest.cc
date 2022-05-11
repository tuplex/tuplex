//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 4/22/2022                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include <Context.h>
#include "../../utils/include/Utils.h"
#include "TestUtils.h"

class SymbolProcessTest : public PyTest {};

TEST_F(SymbolProcessTest, MissingSymbol) {
    // make sure code doesn't crash when symbols are missing
    using namespace tuplex;
    using namespace std;
    // c.csv(...).mapColumn('title', lambda x: split(x)[0]) --> split is a non-defined symbol!
    auto path = "../resources/zillow_data.csv";
    Context c(microTestOptions());

    auto v = c.csv(path).mapColumn("title", UDF("lambda x: split(x)[0]")).collectAsVector();
}