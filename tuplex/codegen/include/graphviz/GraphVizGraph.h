//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_GRAPHVIZGRAPH_H
#define TUPLEX_GRAPHVIZGRAPH_H

#include <memory>
#include <graphviz/GraphVizBuilder.h>
#include "IVisitor.h"
#include <stack>
#include <visitors/GraphVizVisitor.h>

namespace tuplex {
    class GraphVizGraph {
    private:
        GraphVizVisitor _astVisitor;
        bool            _astGraphCreated;
    public:

        GraphVizGraph() : _astGraphCreated(false)   {}
        ~GraphVizGraph() {}

        void createFromAST(ASTNode *root, bool withTypes = false);

        bool saveAsDot(const std::string& path);
        bool saveAsPDF(const std::string& path);

    };
}

#endif //TUPLEX_GRAPHVIZGRAPH_H