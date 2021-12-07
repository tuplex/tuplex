//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 10/18/2021                                                                        //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <Context.h>
#include "../core/TestUtils.h"
#include "FileSystemUtils.h"

TEST(FileOutput, NewFolder) {
    using namespace tuplex;

    auto opts = microTestOptions();
    Context c(opts);

    auto newFolder = uniqueFileName();

    std::vector<Row> rows({Row(1), Row(2), Row(3)});
    c.parallelize(rows).tocsv(newFolder);

    auto outputRows = c.csv(newFolder + "/part0.csv").collectAsVector();
    ASSERT_EQ(rows.size(), outputRows.size());
    for (int i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows.at(i).toPythonString(), outputRows.at(i).toPythonString());
    }
}

TEST(FileOutput, EmptyFolder) {
    using namespace tuplex;

    auto opts = microTestOptions();
    Context c(opts);

    auto emptyFolder = uniqueFileName();

    auto vfs = VirtualFileSystem::fromURI(URI("."));
    vfs.create_dir(URI(emptyFolder));

    std::vector<Row> rows({Row(1), Row(2), Row(3)});
    c.parallelize(rows).tocsv(emptyFolder);

    auto outputRows = c.csv(emptyFolder + "/part0.csv").collectAsVector();
    ASSERT_EQ(rows.size(), outputRows.size());
    for (int i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows.at(i).toPythonString(), outputRows.at(i).toPythonString());
    }
}

TEST(FileOutput, NonEmptyFolder) {
    using namespace tuplex;

    auto opts = microTestOptions();
    Context c(opts);

    auto nonEmptyFolder = uniqueFileName();

    auto vfs = VirtualFileSystem::fromURI(URI("."));
    vfs.create_dir(URI(nonEmptyFolder));
    vfs.create_dir(URI(nonEmptyFolder + "/subfolder"));

    std::vector<Row> rows({Row(1), Row(2), Row(3)});
    EXPECT_ANY_THROW(c.parallelize(rows).tocsv(nonEmptyFolder));
}
