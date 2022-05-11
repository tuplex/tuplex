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
    
    // cf. https://stackoverflow.com/questions/23270078/test-a-specific-exception-type-is-thrown-and-the-exception-has-the-right-propert
    // this tests _that_ the expected exception is thrown
    EXPECT_THROW({
                     try
                     {
                         auto v = c.csv(path).mapColumn("title", UDF("lambda x: split(x)[0]")).collectAsVector();

                         // make sure v is not getting optmized away...
                         EXPECT_FALSE(v.empty());
                     }
                     catch( const std::runtime_error& e )
                     {
                         // and this tests that it has the correct message
                         //EXPECT_STREQ( "Cucumber overflow", e.what() );
                         EXPECT_TRUE(strlen(e.what()) > 0);
                         throw;
                     }
                 }, std::runtime_error);
}
