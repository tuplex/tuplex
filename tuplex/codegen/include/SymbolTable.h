//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_SYMBOLTABLE_H
#define TUPLEX_SYMBOLTABLE_H

#include <string>
#include <list>
#include <map>
#include <stack>
#include <TypeSystem.h>
#include <cassert>
#include <iostream>
#include <Logger.h>
#include <ASTAnnotation.h>
#include <ClosureEnvironment.h>
#include <IFailable.h>

namespace tuplex {

    class SymbolTable;
    class ClosureEnvironment;

    // symbol table holding python builtins, global variables obtained from function closure etc.
    class SymbolTable : public IFailable {
    public:
        enum class ScopeType {
            UNKNOWN,
            BUILTIN,
            GLOBAL,
            LOCAL
        };

        struct Scope {
            int level;
            ScopeType type;
            std::string name; // optional name (i.e. function name)
            Scope* parent;

            // names associated with certain objects & types
            std::unordered_map<std::string, std::shared_ptr<Symbol>> symbols;

            Scope() : parent(nullptr)   {}

            virtual ~Scope() {
                parent = nullptr;
                symbols.clear();
            }
        };

        SymbolTable() : _visitIndex(0) {}

        ~SymbolTable() {
            if(!_scopes.empty()) {
                for(auto scope : _scopes) {
                    delete scope;
                    scope = nullptr;
                }
            }
        }

        /*!
         * adds a new symbol to the table. Will throw an exception if symbol is already defined.
         * @param symbolName identifier of the symbol
         * @param type type of the symbol (if known)
         * @param stype Type of scope this variable is placed in
         * @param scopeID an ID for the scope this variable belongs too.
         */
        void addSymbol(const std::string& symbolName,
                       const python::Type& type,
                       const ScopeType& stype);

        Symbol* addSymbol(std::shared_ptr<Symbol> sym);

        inline ScopeType currentScopeType() {
            if(_scopeStack.empty()) {
                Logger::instance().defaultLogger().warn("requesting empty scope type");
                return ScopeType::UNKNOWN;
            } else {
                return _scopeStack.back()->type;
            }
        }

        inline int currentScopeLevel() {
            if(_scopeStack.empty()) {
                Logger::instance().defaultLogger().warn("there are no scopes on the stack. ERROR.");
                return -1;
            } else {
                assert(_scopeStack.size() == _scopeStack.back()->level);
                return _scopeStack.size();
            }
        }

        /*!
         * starts a new nested scope
         */
        void beginScope(const ScopeType& st, const std::string& functionName="")   {
            // push to stack
            auto scope = new Scope;
            scope->type = st;
            scope->level = _scopeStack.size();
            scope->parent = _scopeStack.empty() ? nullptr : currentScope();
            scope->name = functionName;
            _scopeStack.push_back(scope);
            _scopes.emplace_back(scope);
            _lastScope = scope;
        }


        /*!
         * creates a new SymbolTable instance from optionally a closure environment
         * @param globals can be nullptr, then only builtins will be added.
         * @return SymbolTable instance or nullptr if creation failed.
         */
        static std::shared_ptr<SymbolTable> createFromEnvironment(const ClosureEnvironment* globals=nullptr);

        /*!
         * leaves current scope
         */
        void endScope() {
            assert(!_scopeStack.empty());
            _lastScope = _scopeStack.back();
            _scopeStack.pop_back(); // remove top scope...
        }

        void resetScope() {
            // visiting mode, reset scope!
            while(!_scopeStack.empty())
                _scopeStack.pop_back();
            _visitIndex = 0;
            _lastScope = nullptr;
        }

        // visit functions
        void enterScope() {
            // push current to stack
            // @Todo: maybe some checks that this is legal
            assert(_visitIndex < _scopes.size());
            _scopeStack.push_back(_scopes[_visitIndex++]);
            // todo update scope with all info from stack, i.e. replace unknown with type hints...
            assert(!_scopeStack.empty());
            _lastScope = _scopeStack.back();
        }

        void exitScope() {
            if(!_scopeStack.empty()) {
                _lastScope = _scopeStack.back();
                _scopeStack.pop_back();
            } else {
                _lastScope = nullptr;
            }
        }

        /*!
         * add a general symbol entry with a specific type
         * @param name
         * @param type
         * @return pointer to the symbol or nullptr if adding failed
         */
        Symbol* addSymbol(const std::string& name, const python::Type& type);


        /*!
         * add an attribute to a builtin type, e.g. str.lower
         * @param builtinType
         * @param name
         * @param type
         */
        void addBuiltinTypeAttribute(const python::Type& builtinType, const std::string& name, const python::Type& type);

        /*!
         * checks whether a symbol can be looked up or not
         * @param symbol
         * @return
         */
        inline bool lookup(const std::string& name) { return findSymbol(name) != nullptr; }

        /*!
         * returns the type for the symbol, UNKNOWN if not found in table
         * @param symbol
         * @return type for symbol
         */
        python::Type lookupType(const std::string& symbol);

        /*!
         * prints a nice textual representation of the current symboltable
         * @param os
         */
        void print(std::ostream& os=std::cout);

        /*!
         * retrieve type for a function invoked with parameterType.
         * @param symbol
         * @param parameterType type of the parameters used to invoke the function. Must be tuple type!
         * @param withNullAsAny because we allow in the null optimization arbitrary options to be replaced by null,
         *        in BlockGeneratorVisitor.cc nullchecks for that case are explicitly generated. However, we still need
         *        to type such functions. Hence, make any function NULL compatible with that option. Disabled per default.
         * @return type if found, else unknown
         */
        python::Type findFunctionType(const std::string& symbol, const python::Type& parameterType, bool withNullAsAny=false);

        /*!
         * returns type of attribute for a given object type invoked with parameterType
         * @param object_type
         * @param attribute
         * @param parameterType type of the parameters used to invoke the function. Must be tuple type!
         * @return type of attribute
         */
        python::Type findAttributeType(const python::Type& object_type,
                                       const std::string& attribute,
                                       const python::Type& parameterType);

        Scope* lastScope() const { return _lastScope; }

        /*!
         * general function to lookup a symbol based on name
         * @param name
         * @return symbol or nullptr if not found
         */
        std::shared_ptr<Symbol> findSymbol(const std::string& name);

        /*!
         * find symbol by fully qualified name, i.e. something like math.inf
         * @param fullyQualifiedName
         * @return symbol or nullptr if not found
         */
        std::shared_ptr<Symbol> findFullyQualifiedSymbol(const std::string& fullyQualifiedName);

    private:
        std::vector<Scope*> _scopeStack;
        std::vector<Scope*> _scopes;
        size_t _visitIndex;

        Scope* _lastScope = nullptr;

        Scope* currentScope() {
            assert(!_scopeStack.empty());
            return _scopeStack.back();
        }

        /*!
         * helper function to find closest function when nullable types are involved.
         * @param symbol
         * @param parameterType
         * @return
         */
        python::Type findClosestFunction(const std::string& symbol, const python::Type& parameterType);

        /*!
         * add python builtin symbols to current table.
         */
        void addBuiltins();

        /*!
         * add all the exception types to both the table and the TypeSystem.cc
         */
        void addBuiltinExceptionHierarchy();
    };
}


#endif //TUPLEX_SYMBOLTABLE_H