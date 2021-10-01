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

#include "ApatheticVisitor.h"
#include "SymbolTable.h"
#include <IFailable.h>
#include <tuple>
#include <ASTHelpers.h>

namespace tuplex {
    inline python::Type unifyTypes(const python::Type& a, const python::Type& b, bool allowNumbers) {
        using namespace std;
        if(a == b)
            return a;

        if(!allowNumbers) {
            // only NULL to any or element and option type allowed
            if (a == python::Type::NULLVALUE)
                return python::Type::makeOptionType(b);
            if (b == python::Type::NULLVALUE)
                return python::Type::makeOptionType(a);

            // one is option type, the other not but the elementtype of the option type!
            if (a.isOptionType() && !b.isOptionType() && a.elementType() == b)
                return a;
            if (b.isOptionType() && !a.isOptionType() && b.elementType() == a)
                return b;
        } else {
            auto t = python::Type::superType(a, b);
            if(t != python::Type::UNKNOWN)
                return t;
        }

        // tuples, lists, dicts...
        if(a.isTupleType() && b.isTupleType() && a.parameters().size() == b.parameters().size()) {
            vector<python::Type> v;
            for(unsigned i = 0; i < a.parameters().size(); ++i) {
                v.push_back(unifyTypes(a.parameters()[i], b.parameters()[i], allowNumbers));
                if(v.back() == python::Type::UNKNOWN)
                    return python::Type::UNKNOWN;
            }
            return python::Type::makeTupleType(v);
        }

        if(a.isListType() && b.isListType()) {
            auto el = unifyTypes(a.elementType(), b.elementType(), allowNumbers);
            if(el == python::Type::UNKNOWN)
                return python::Type::UNKNOWN;
            return python::Type::makeListType(el);
        }

        if(a.isDictionaryType() && b.isDictionaryType()) {
            auto key_t = unifyTypes(a.keyType(), b.keyType(), allowNumbers);
            auto val_t = unifyTypes(a.valueType(), b.valueType(), allowNumbers);
            if(key_t != python::Type::UNKNOWN && val_t != python::Type::UNKNOWN)
                return python::Type::makeDictionaryType(key_t, val_t);
        }
        return python::Type::UNKNOWN;
    }

    class TypeAnnotatorVisitor : public ApatheticVisitor, public IFailable {
    private:
        SymbolTable& _symbolTable; // global symbol table for everything.
        bool _allowNumericTypeUnification; // whether bool/i64 get autoupcasted and merged when type conflicts exist within if-branches.
        std::unordered_map<std::string, python::Type> _nameTable; // i.e. mini symbol table for assignments.
        std::unordered_map<std::string, std::shared_ptr<IteratorInfo>> _iteratorInfoTable; // i.e. name table for storing iteratorInfo of variables.

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
        void assignHelper(NIdentifier *id, python::Type type);
        void checkRetType(python::Type t);
        /*!
         * Annotate iterator-related NCall with iterator-specific info
         * @param funcName
         * @param call
         */
        void annotateIteratorRelatedCalls(const std::string &funcName, NCall* call);

    public:

        void reset() {
            _missingIdentifiers = TSet<std::string>();
            _annotationLookup.clear();
            _funcReturnTypes.clear();
            IFailable::reset();
        }

        explicit TypeAnnotatorVisitor(SymbolTable& symbolTable,
                                      bool allowNumericTypeUnification): _symbolTable(symbolTable),
                                                                         _allowNumericTypeUnification(allowNumericTypeUnification) {
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

        TSet<std::string> getMissingIdentifiers() { return _missingIdentifiers; }
    };
}

#endif //TUPLEX_TYPEANNOTATORVISITOR_H