//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IREPLACEVISITOR_H
#define TUPLEX_IREPLACEVISITOR_H

#include <visitors/IVisitor.h>

namespace tuplex {
    /*!
 * helper class to perform node replacement by recursively visiting nodes
 */
    class IReplaceVisitor : public IVisitor {
    protected:

        /*!
         * overwrite this function to replace node with the return value.
         * make sure the returned variable is not owned by another node!
         * @param parent parent of the node
         * @param node node to be replaced by the return value of this function
         * @return result of calling the replace function will replace node. Can be also a nullptr.
         */
        virtual ASTNode* replace(ASTNode* parent, ASTNode* node) = 0;

        // helper function, that makes sure result of replace is called recursively!
        inline ASTNode* replaceh(ASTNode* parent, ASTNode* node) {
            if(node)
                node->accept(*this);

            auto res = replace(parent, node);

            // visit children of the possible newly attached node
            // if it differs from the original node.
            if(res && res != node)
                res->accept(*this);
            return res;
        }

        template<class Node>
        inline Node* replaceh(ASTNode* parent, Node* node) {
            auto ret_ptr = replaceh(parent, (ASTNode*)node); // replace
            assert(ret_ptr->type() == Node::type_);
            return (Node*)ret_ptr;
        }

    public:
        virtual void visit(NFunction*);
        virtual void visit(NBinaryOp*);
        virtual void visit(NUnaryOp*);
        virtual void visit(NSuite*);
        virtual void visit(NModule*);
        virtual void visit(NLambda*);
        virtual void visit(NCompare*);
        virtual void visit(NParameterList*);
        virtual void visit(NStarExpression*);
        virtual void visit(NParameter*);
        virtual void visit(NAwait*);
        virtual void visit(NTuple*);
        virtual void visit(NDictionary*);
        virtual void visit(NList*);
        virtual void visit(NSubscription*);
        virtual void visit(NReturn*);
        virtual void visit(NAssign*);
        virtual void visit(NCall*);
        virtual void visit(NAttribute*);
        virtual void visit(NSlice*);
        virtual void visit(NSliceItem*);
        virtual void visit(NIfElse*);
        virtual void visit(NRange*);
        virtual void visit(NComprehension*);
        virtual void visit(NListComprehension*);
        virtual void visit(NAssert*);
        virtual void visit(NRaise*);

        virtual void visit(NWhile*);
        virtual void visit(NFor*);


        // there is no point in visiting these Literals or endnodes since they won't be pruned in any way
        // hence, just define them as empty functions.
        virtual void visit(NNone*)          {}
        virtual void visit(NNumber*)        {}
        virtual void visit(NIdentifier*)    {}
        virtual void visit(NBoolean*)       {}
        virtual void visit(NEllipsis*)      {}
        virtual void visit(NString*)        {}
        virtual void visit(NBreak*) {}
        virtual void visit(NContinue*) {}

    };
}

#endif //TUPLEX_IREPLACEVISITOR_H