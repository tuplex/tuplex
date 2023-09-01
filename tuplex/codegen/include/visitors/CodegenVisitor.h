////
//// Created by leonhards on 4/16/23.
////
//
//#ifndef TUPLEX_CODEGENVISITOR_H
//#define TUPLEX_CODEGENVISITOR_H
//
//#include "ApatheticVisitor.h"
//
//namespace tuplex {
//    // helper class to generate Python Code from AST
//    class CodegenVisitor : public ApatheticVisitor {
//    public:
//        std::string python_code() const;
//    private:
//        std::vector<std::string> _stack;
//
//        // leaf nodes
//        void visit(NNone*) override;
//        void visit(NNumber*) override;
//        void visit(NIdentifier*) override;
//        void visit(NBoolean*) override;
//        void visit(NEllipsis*) override;
//        void visit(NString*) override;
//
//        // non-leaf nodes, recursive calls are carried out for these
//        void visit(NParameter*) override;
//        void visit(NParameterList*) override;
//        void visit(NFunction*) override;
//
//        void visit(NBinaryOp*) override;
//        void visit(NUnaryOp*) override;
//        void visit(NSuite*) override;
//        void visit(NModule*) override;
//        void visit(NLambda*) override;
//        void visit(NAwait*) override;
//        void visit(NStarExpression*) override;
//        void visit(NCompare*) override;
//        void visit(NIfElse*) override;
//        void visit(NTuple*) override;
//        void visit(NDictionary*) override;
//        void visit(NList*) override;
//
//        void visit(NSubscription*) override;
//        void visit(NReturn*) override;
//        void visit(NAssign*) override;
//        void visit(NCall*) override;
//        void visit(NAttribute*) override;
//        void visit(NSlice*) override;
//        void visit(NSliceItem*) override;
//
//        void visit(NRange*) override;
//        void visit(NComprehension*) override;
//        void visit(NListComprehension*) override;
//
//        void visit(NAssert*) override;
//        void visit(NRaise*) override;
//
//        void visit(NWhile*) override;
//        void visit(NFor*) override;
//        void visit(NBreak*) override;
//        void visit(NContinue*) override;
//    };
//}
//
//#endif //TUPLEX_CODEGENVISITOR_H
