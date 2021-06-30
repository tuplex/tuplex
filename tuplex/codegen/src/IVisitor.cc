//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <IVisitor.h>
#include <tuple>
#include <memory>

namespace tuplex {
    std::tuple<bool, bool, bool> whichBranchToVisit(NIfElse* ifelse) {
        assert(ifelse);

        using namespace std;

        // are there annotations for this node?
        if(ifelse->hasAnnotation() && ifelse->annotation().numTimesVisited > 0) {
            // yes, so decide based on tracing where to visit.
            // check for annotations & visit according to them
            bool visit_ifelse = !ifelse->hasAnnotation() || ifelse->annotation().numTimesVisited > 0;
            bool visit_if = false;
            if(ifelse->_then)
                visit_if = !ifelse->_then->hasAnnotation() || ifelse->_then->annotation().numTimesVisited > 0;
            bool visit_else = false;
            if(ifelse->_else)
                visit_else =
                        !(ifelse->_else && ifelse->_else->hasAnnotation()) || ifelse->_else->annotation().numTimesVisited > 0;

            // check for if + else annotation case
            if(ifelse->_else && (ifelse->_else->hasAnnotation() && ifelse->_then->hasAnnotation())) {
                auto weight_if = ifelse->_then->annotation().numTimesVisited;
                auto weight_else = ifelse->_else->annotation().numTimesVisited;
                if(weight_if > weight_else) {
                    visit_if = true;
                    visit_else = false;
                } else {
                    visit_if = weight_if >= weight_else;
                    visit_else = true;
                }
            }

            // if ifelse statement should not be visited, set both to false
            if(!visit_ifelse) {
                visit_if = false;
                visit_else = false;
            }

            return std::make_tuple(visit_ifelse, visit_if, visit_else);
        } else {
            // visit everything (check whether else exists)
            return make_tuple(true, true, ifelse->_else);
        }
    }
}