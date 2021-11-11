//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TRACEVISITOR_H
#define TUPLEX_TRACEVISITOR_H

#include <ApatheticVisitor.h>
#include <Python.h>
#include <vector>
#include <PythonHelpers.h>
#include <IFailable.h>
#include <csetjmp>
#include <ClosureEnvironment.h>
#include <ASTHelpers.h>

// a tracing visitor to determine optimizations within functions!
namespace tuplex {
    class TraceVisitor : public ApatheticVisitor, public IFailable {
    private:
        PyObject *_args;
        bool _functionSeen;
        size_t _numSamplesProcessed;
        struct TraceItem {
            PyObject* value;
            std::string name;

            TraceItem(PyObject* obj) : value(obj)   {}
            TraceItem(PyObject* obj, const std::string n) : value(obj), name(n) {}
        };

        // evaluation stack
        std::vector<TraceItem> _evalStack;
        // use the helper function to add annotations to ASTs!
        void addTraceResult(ASTNode* node, TraceItem item);

        // symbols
        std::vector<TraceItem> _symbols;

        MessageHandler& logger() { return Logger::instance().logger("tracer"); }

        void unpackFunctionParameters(const std::vector<ASTNode*> &astArgs);

        /*!
         * types of traced input arguments
         */
        std::vector<std::vector<python::Type>> _colTypes;
        std::vector<std::vector<python::Type>> _retColTypes;

        /*!
         * any exceptions that might have occurred while processing the sample
         */
         std::vector<std::string> _exceptions; // for now simple strings => aggregate

         // store tracebacks for clean display
         // TODO: could avoid storing duplicates so index by type & line number & column number, yet this not supported yet.

        // was a break statement executed in the ongoing loop?
        std::vector<bool> _loopBreakStack;

        // each element vector corresponds to {{symbols created before loop}, symbolTypeChange} for an ongoing loop
        // whenever the type of a symbol that is in {symbols created before loop} changes, set symbolTypeChange to true
        std::vector<std::pair<std::vector<std::string>, bool>> _symbolsTypeChangeStack;

        TraceItem _retValue;

        python::Type _inputRowType; // optional schema to filter out bad input rows (will save as exception)

        void fetchAndStoreError();

        void errCheck();

        /*!
         * internal class thrown when errors occur to leave control flow
         */
        class TraceException : public std::exception {
        };
    public:
        TraceVisitor(const python::Type& inputRowType=python::Type::UNKNOWN) : _args(nullptr),
                    _functionSeen(false),
                    _retValue(nullptr), _inputRowType(inputRowType), _numSamplesProcessed(0) {
        }

        /*!
         * trace input over AST and record within tree what happened
         * @param node
         * @param args PyObject to trace
         */
        void recordTrace(ASTNode* node, PyObject* args);

        python::Type majorityInputType() const;
        python::Type majorityOutputType() const;


        /*!
         * set global constants, variables, imports etc. from closure environment
         * @param ce
         */
        void setClosure(const ClosureEnvironment& ce, bool acquireGIL);

        /*!
         * retrieve last result of function...
         * @return
         */
        PyObject* lastResult() const { return _retValue.value; }

        // leaf nodes
        void visit(NNone *) override;

        void visit(NNumber *) override;

        void visit(NIdentifier *) override;

        void visit(NBoolean *) override;

        void visit(NEllipsis *) override {}

        void visit(NString *) override;

        // non-leaf nodes, recursive calls are carried out for these
        void visit(NParameter *) override;

        void visit(NParameterList *) override;

        void visit(NFunction *) override;

        void visit(NBinaryOp *) override;

        void visit(NUnaryOp *) override;

        void visit(NSuite *) override;

        void visit(NModule *) override;

        void visit(NLambda *) override;

        void visit(NAwait *) override;

        void visit(NStarExpression *) override;

        void visit(NCompare *) override;

        void visit(NIfElse *) override;

        void visit(NTuple *) override;

        void visit(NDictionary *) override;

        void visit(NSubscription *) override;

        void visit(NReturn *) override;

        void visit(NAssign *) override;

        void visit(NCall *) override;

        void visit(NAttribute *) override;

        void visit(NSlice *) override;

        void visit(NSliceItem *) override;

        void visit(NFor *) override;

        void visit(NWhile *) override;

        void visit(NRange *) override;

        void visit(NList *) override;
    };
}

#endif //TUPLEX_TRACEVISITOR_H