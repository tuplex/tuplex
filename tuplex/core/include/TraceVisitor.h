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
#include <utility>
#include <vector>
#include <PythonHelpers.h>
#include <IFailable.h>
#include <csetjmp>
#include <ClosureEnvironment.h>

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

            explicit TraceItem(PyObject* obj) : value(obj)   {}
            TraceItem(PyObject* obj, std::string n) : value(obj), name(std::move(n)) {}
        };

        // evaluation stack
        std::vector<TraceItem> _evalStack;
        // use the helper function to add annotations to ASTs!
        void addTraceResult(ASTNode* node, TraceItem item);

        // symbols
        std::vector<TraceItem> _symbols;

        static MessageHandler& logger() { return Logger::instance().logger("tracer"); }

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
        explicit TraceVisitor(const python::Type& inputRowType=python::Type::UNKNOWN) : _args(nullptr),
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
    };
}

#endif //TUPLEX_TRACEVISITOR_H