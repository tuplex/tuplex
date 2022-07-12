//
// Created by leonhards on 3/23/22.
//

#ifndef TUPLEX_GRAPHVIZVISITOR_H
#define TUPLEX_GRAPHVIZVISITOR_H

#include <memory>
#include <stack>
#include <vector>
#include <regex>
#include "IVisitor.h"
#include <graphviz/GraphVizBuilder.h>

namespace tuplex {
    class GraphVizVisitor : public IVisitor {
    private:
        std::unique_ptr<GraphVizBuilder> _builder;
        std::stack<int> _ids;
        int _lastId;

        // getParent gets the current parent (pops stack, i.e. mimicking recursive calls)
        inline int getParent() { int iParent =_ids.top(); _ids.pop(); return iParent;}
        bool _showTypes;

        /*!
         * creates a string describing the typing of an AST node
         * @param node
         * @param showCandidates
         * @return
         */
        std::string typeStr(ASTNode *node);

        /*!
         * escape characters to valid HTML codes.
         * @param s
         * @return html string
         */
        std::string escapeHTML(const std::string& s);
    public:
        GraphVizVisitor(): _builder(new GraphVizBuilder()) {
            _ids.push(-1);
            _lastId = -1;
            _showTypes = false;
        }

        auto getBuilder() -> decltype(_builder.get()) {
            return _builder.get();
        }

        void visit(NNone*) override;
        void visit(NParameter*) override;
        void visit(NParameterList*) override;
        void visit(NFunction*) override;
        void visit(NNumber*) override;
        void visit(NIdentifier*) override;
        void visit(NBoolean*) override;
        void visit(NEllipsis*) override;
        void visit(NString*) override;
        void visit(NBinaryOp*) override;
        void visit(NUnaryOp*) override;
        void visit(NSuite*) override;
        void visit(NModule*) override;
        void visit(NLambda*) override;
        void visit(NAwait*) override;
        void visit(NStarExpression*) override;
        void visit(NCompare*) override;
        void visit(NIfElse*) override;
        void visit(NTuple*) override;
        void visit(NDictionary*) override;
        void visit(NList*) override;
        void visit(NSubscription*) override;
        void visit(NReturn*) override;
        void visit(NAssign*) override;
        void visit(NCall*) override;
        void visit(NAttribute*) override;
        void visit(NSlice*) override;
        void visit(NSliceItem*) override;
        void visit(NRange*) override;
        void visit(NComprehension*) override;
        void visit(NListComprehension*) override;

        void visit(NAssert*) override;
        void visit(NRaise*) override;

        void visit(NWhile*) override;
        void visit(NFor*) override;
        void visit(NBreak*) override;
        void visit(NContinue*) override;

        void showTypes(bool show=true) { _showTypes = show; }
    };
}

#endif //TUPLEX_GRAPHVIZVISITOR_H
