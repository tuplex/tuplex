//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <VirtualFileSystem.h>
#include <URI.h>

TEST(URI, parent) {
    // test parent function for various URIs
    using namespace tuplex;

    // inspired from Pathlib path.
    EXPECT_EQ(URI("s3://tuplex/test").parent().toPath(), URI("s3://tuplex").toPath());
    EXPECT_EQ(URI("s3://tuplex/test/").parent().toPath(), URI("s3://tuplex").toPath());
    EXPECT_EQ(URI("s3://tuplex").parent().toPath(), URI("s3://.").toPath());
    EXPECT_EQ(URI("/tuplex/test").parent().toPath(), URI("/tuplex").toPath());
    EXPECT_EQ(URI("/tuplex/test/").parent().toPath(), URI("/tuplex").toPath());
    EXPECT_EQ(URI("/tuplex/test////////////").parent().toPath(), URI("/tuplex").toPath());
    EXPECT_EQ(URI("/tuplex").parent().toPath(), URI("/").toPath());
    EXPECT_EQ(URI("tuplex/test").parent().toPath(), URI("tuplex").toPath());
    EXPECT_EQ(URI("tuplex/test/").parent().toPath(), URI("tuplex").toPath());
    EXPECT_EQ(URI("tuplex").parent().toPath(), URI(".").toPath());
}