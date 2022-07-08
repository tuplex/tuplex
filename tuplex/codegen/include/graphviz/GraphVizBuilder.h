//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_GRAPHVIZBUILDER_H
#define TUPLEX_GRAPHVIZBUILDER_H

#include <iostream>
#include <string>
#include <vector>

namespace tuplex {
    // this class can be used to generate a graphviz dot file
// useful for display of graphs
    class GraphVizBuilder {
    private:
        int _id;
        std::vector<std::string> _nodes;
        std::vector<std::string> _edges;

        static const std::string nodePrefix;

        bool writeToStream(std::ostream& os);
    public:
        GraphVizBuilder() :_id(0)   {}

        int addNode(const std::string& label);
        int addHTMLNode(const std::string& label);

        void addEdge(const int iFrom, const int iTo, const std::string& subfieldFrom="", const std::string& subfieldTo="");

        bool saveToDotFile(const std::string& path);

        bool saveToPDF(const std::string& path);
    };
}


#endif //TUPLEX_GRAPHVIZBUILDER_H