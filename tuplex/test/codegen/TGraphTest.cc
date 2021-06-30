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
#include <TGraph.h>
#include <string>

TEST(Graph, BasicConnectivity) {
    TGraph<int> g;
    g.addNode(10);
    g.addEdge(10, 20);
    g.addEdge(10, 30);
    g.addEdge(30, 50);
    g.addEdge(25, 10);

    EXPECT_TRUE(g.contains(50));
    EXPECT_FALSE(g.contains(100));
    EXPECT_EQ(g.numNodes(), 5);
    EXPECT_FALSE(g.isReachable(10, 25));
    EXPECT_TRUE(g.isReachable(10, 50));
    EXPECT_FALSE(g.isReachable(20, 50));
}

TEST(Graph, PDFExport) {
    TGraph<int> g;
    g.addNode(10);
    g.addEdge(10, 20);
    g.addEdge(10, 30);
    g.addEdge(30, 50);
    g.addEdge(25, 10);
    GraphVizBuilder builder;
    g.buildViz(builder, [](const int& i){ return std::to_string(i); });

    // can run this
    EXPECT_TRUE(builder.saveToPDF("test.pdf"));
}