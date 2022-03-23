//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <graphviz/GraphVizGraph.h>
#include <regex>
#include <TSet.h>

namespace tuplex {
    // helper function
    std::string setToString(const TSet<python::Type>& S) {
        std::string s = "\\{";

        for(int i = 0; i < S.size(); i++) {
            s += S[i].desc();
            if(i != S.size() - 1)
                s += ", ";
        }

        return s + "\\}";
    }

// glue code logic
    void GraphVizGraph::createFromAST(ASTNode *root, bool withTypes) {
        _astVisitor.showTypes(withTypes);
        root->accept(_astVisitor);
        _astGraphCreated = true;
    }

    bool GraphVizGraph::saveAsDot(const std::string &path) {
        if(_astGraphCreated) {
            return _astVisitor.getBuilder()->saveToDotFile(path);
        }
            // log here...
        else return false;
    }

    bool GraphVizGraph::saveAsPDF(const std::string &path) {
        if(_astGraphCreated) {
            return _astVisitor.getBuilder()->saveToPDF(path);
        }
            // log here...
        else return false;
    }
}