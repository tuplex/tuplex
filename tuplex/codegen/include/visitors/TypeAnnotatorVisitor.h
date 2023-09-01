//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TYPEANNOTATORVISITOR_H
#define TUPLEX_TYPEANNOTATORVISITOR_H

#include "visitors/ApatheticVisitor.h"
#include "symbols/SymbolTable.h"
#include "codegen/CodegenHelper.h"
#include <codegen/IFailable.h>
#include <tuple>
#include <ast/ASTHelpers.h>
#include "type_deopt.h"

namespace tuplex {

    class TypeAnnotatorVisitor : public ApatheticVisitor, public IFailable {
    private:
        SymbolTable& _symbolTable; // global symbol table for everything.
        const codegen::CompilePolicy& _policy;
        TypeUnificationPolicy _typeUnificationPolicy;
        std::unordered_map<std::string, python::Type> _nameTable; // i.e. mini symbol table for assignments.
        std::unordered_map<std::string, std::shared_ptr<IteratorInfo>> _iteratorInfoTable; // i.e. name table for storing iteratorInfo of variables.

        // deoptimizes all types in the current nametable/iteratorinfo table.
        // required for for/while loops b.c. no opt type support there yet.
        void deopt_tables();

        void resolveNameConflicts(const std::unordered_map<std::string, python::Type>& table);
        void resolveNamesForIfStatement(std::unordered_map<std::string, python::Type>& if_table,
                                        std::unordered_map<std::string, python::Type>& else_table);

        inline python::Type lookupType(const std::string& name ) {
            // check first name table
            if(_nameTable.find(name) != _nameTable.end())
                return _nameTable[name];
            return _symbolTable.lookupType(name);
        }

        TSet<std::string> _missingIdentifiers;
        std::map<std::string, python::Type> _annotationLookup;

        std::vector<python::Type>   _funcReturnTypes;
        std::vector<size_t>         _returnTypeCounts; //! when annotations are present, count here.

        ///! helper variable to store the last NCAll* node's inferred params type.
        ///!  required to specialize attributes
        std::stack<python::Type> _lastCallParameterType;

        void init();
        python::Type binaryOpInference(ASTNode* left, const python::Type& a,
                                       const TokenType tt, ASTNode* right,
                                       const python::Type& b);
        void assignHelper(NIdentifier *id, const python::Type &type);

        bool is_nested_subscript_target(ASTNode* target);
        void recursive_set_subscript_types(NSubscription* target, python::Type value_type);

        void checkRetType(python::Type t);
        /*!
         * Annotate iterator-related NCall with iterator-specific info
         * @param funcName
         * @param call
         */
        void annotateIteratorRelatedCalls(const std::string &funcName, NCall* call);

        // total number of samples processed in TraceVisitor
        size_t _totalSampleCount;
        // set to true once type change during loop occurs
        bool _loopTypeChange;
        // indices of samples that will raise normal case violation
        std::set<size_t> _normalCaseViolationSampleIndices;
        // each vector contains symbols that need to be tracked for type stability for the current loop
        size_t _ongoingLoopCount;
        bool _annotateWithConstantValues; // optimization to create even more reduced types. If set to true, then literals will emit constantValued types!


        void typeStructuredDictSubscription(NSubscription* sub, const python::Type& type);

        /*!
         * checks whether contained is statements are valid re types, return false if not.
         * @param cmp compare node
         * @return true if check passed, false else.
         */
        bool checkForValidIsComparisons(NCompare* cmp);
    public:

        void reset() {
            _missingIdentifiers = TSet<std::string>();
            _annotationLookup.clear();
            _funcReturnTypes.clear();
            IFailable::reset();
            _normalCaseViolationSampleIndices.clear();
            _loopTypeChange = false;
            _totalSampleCount = 0;
            _ongoingLoopCount = 0;
        }

        explicit TypeAnnotatorVisitor(SymbolTable& symbolTable,
                                      const codegen::CompilePolicy& policy): _symbolTable(symbolTable),
                                                                         _policy(policy),
                                                                         _loopTypeChange(false),
                                                                         _totalSampleCount(0),
                                                                         _ongoingLoopCount(0),
                                                                         _annotateWithConstantValues(true) {
            init();
        }

        void visit(NIdentifier*) override;
        void visit(NFunction*) override;
        void visit(NBinaryOp*) override;
        void visit(NUnaryOp*) override;
        void visit(NSuite*) override;
        void visit(NModule*) override;
        void visit(NLambda*) override;
        void visit(NCompare*) override;
        void visit(NParameterList*) override;
        void visit(NStarExpression*) override;
        void visit(NParameter*) override;
        void visit(NAwait*) override;
        void visit(NTuple*) override;
        void visit(NDictionary*) override;
        void visit(NList*) override;
        void visit(NSubscription*) override;
        void visit(NSlice*) override;

        void visit(NReturn*) override;

        void visit(NCall*) override;
        void visit(NAttribute*) override;

        void visit(NAssign*) override;


        void visit(NIfElse*) override;

        void visit(NRange*) override;
        void visit(NComprehension*) override;
        void visit(NListComprehension*) override;

        void visit(NFor*) override;
        void visit(NWhile*) override;

        // literal nodes
        void visit(NNumber*) override;
        void visit(NString*) override;
        void visit(NBoolean*) override;
        // None is already a constant -> do not bother!
        // for compound types, check whether there's an option to annotate using constants!

        TSet<std::string> getMissingIdentifiers() { return _missingIdentifiers; }
    };

    /*!
     * see whether ast node can be turned into a static key usable for structured dictionaries.
     * @param node the ast node, if nullptr unknown is returned.
     * @return key and type of key. unknown if not possible.
     */
    extern std::tuple<std::string, python::Type> extractKeyFromASTNode(ASTNode* node);
}

#endif //TUPLEX_TYPEANNOTATORVISITOR_H