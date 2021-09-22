//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CODEGENERATOR_H
#define TUPLEX_CODEGENERATOR_H

#include <TypeSystem.h>
#include <Logger.h>
#include <SymbolTable.h>
#include <ClosureEnvironment.h>
#include <IFailable.h>

#include <llvm/ADT/APFloat.h>
#include <llvm/ADT/STLExtras.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Verifier.h>
#include <LLVMEnvironment.h>

namespace tuplex {
    namespace codegen {
        // support structure to deal with arguments
        struct FunctionArguments {
            std::vector<python::Type> argTypes;
        };

        // class holding an abstract syntax tree
        class AnnotatedAST : public IFailable {
        private:

            // name of the function/last statement within the IR module
            std::string _irFuncName;
            std::map<std::string, python::Type> _typeHints;

            bool _allowNumericTypeUnification;

            std::vector<std::string> _typingErrMessages; // error messages produced by type annotator.

            // holds the AST tree after successful parsing
            ASTNode *_root;
            bool _typesDefined; // lazy check variable whether types are already defined or not

            ClosureEnvironment _globals; // global variables + modules

            void release();

            // in this function the AST is (pre)processed
            // 1) cleaning/pruning the AST tree
            // 2) reducing expressions/rewriting if possible
            void processAST();

            ASTNode *findFunction(ASTNode *root) const;

            void hintFunctionParameters(ASTNode *node);
            void assignParameterType(ASTNode* arg);

            // deep copy
            void cloneFrom(const AnnotatedAST& other);

            // updates function ast with type & also updates the param nodes...
            void setFunctionType(ASTNode* node, const python::Type& type);
        public:
            AnnotatedAST(): _root(nullptr), _typesDefined(false), _allowNumericTypeUnification(false) {}

            AnnotatedAST(const AnnotatedAST& other) : _root(nullptr), _typesDefined(other._typesDefined), _globals(other._globals), _allowNumericTypeUnification(other._allowNumericTypeUnification) {
                cloneFrom(other);
            }

            ~AnnotatedAST() {
                release();
            }

            AnnotatedAST& operator = (const AnnotatedAST& other) {
                // self assignment?
                if(&other == this)
                    return *this;

                cloneFrom(other);
                return *this;
            }

            /*!
             * constructs annotated ast from string.
             * @param s
             * @return false if string could not be parsed.
             */
            bool parseString(const std::string& s, bool allowNumericTypeUnification);

            void allowNumericTypeUnification(bool allow) { _allowNumericTypeUnification = allow; }
            inline bool allowNumericTypeUnification() const { return _allowNumericTypeUnification; }
            void setGlobals(const ClosureEnvironment& globals) { _globals = globals; }
            const ClosureEnvironment& globals() const { return _globals; }

            bool writeGraphVizFile(const std::string& path);
            bool writeGraphToPDF(const std::string &path);

            /*!
             * specify for the identifier a possible type. This may enable / simplify code generation.
             * Note that an indentifier can have multiple type hints
             * @param identifier
             * @param type
             */
            void addTypeHint(const std::string& identifier, const python::Type& type);


            /*!
             * generates code for a python UDF function
             * @param allowUndefinedBehavior whether generated code allows for undefined behavior, i.e. division by zero, ...
             * @param sharedObjectPropagation whether to share read-only objects across rows
             * @return bool if code can be generated, false if not
             */
            bool generateCode(LLVMEnvironment *env, bool allowUndefinedBehavior, bool sharedObjectPropagation);

            /*!
             * function name to call this udf in LLVM IR
             * @return function name
             */
            std::string getFunctionName() const { return _irFuncName; }

            /*!
             * return type of the generated LLVM IR function
             * @return
             */
            python::Type getReturnType() const;

            /*!
             * returns how the data would be represented as row type
             * @return
             */
            inline python::Type getRowType() const {
                auto t = getReturnType();
                if(t == python::Type::EMPTYTUPLE)
                    return python::Type::makeTupleType({python::Type::EMPTYTUPLE}); // ((),) as special case
                // propagate to tuple type!
                return python::Type::propagateToTupleType(t);
            }

            /*!
             * expected input types for the IR function. E.g. if the function is lambda x, y: x * y
             * this will return (type(x), type(y)). type(x) is the type of x, i.e. i64, f64, string, (i64, f64), ...
             * @return
             */
            FunctionArguments getParameterTypes() const;

            /*!
             * sets types of all parameters to unknown
             * @return itself
             */
            AnnotatedAST& removeParameterTypes();

            /*!
             * checks _compileErrors in IFailable and throws an exception if return type is not supported and not resolved through fallback mode.
             * currently returning list of lists/tuples/dicts/multi-types will raise exception.
             * TODO: Add support for returning list of tuples/dicts and use fallback mode for other cases
             */
            void checkReturnError();

            /*!
             * set/upcast return type to target type
             * @param targetType type to return as. Note: must be compatible, else throws exception
             */
            void setReturnType(const python::Type& targetType);

            /*!
             * returns the names of the parameters of the top level function. May be also empty if it is a void function
             * @return parameter names, may be used to place type hints
             */
            std::vector<std::string> getParameterNames() const;

            /*!
             * annotates the tree with final types. If this is not possible, returns false
             * @param silentMode determines whether the type inference should log out problems or not
             * @param removeBranches whether to use RemoveDeadBranchesVisitor to prune AST
             * @return whether types could be successfully annotated/defined for all AST nodes
             */
            bool defineTypes(bool silentMode=false, bool removeBranches=false);


            /*!
             * for easier handling, function arguments may be auto unpacked. This annotates the (first) found function
             * to be ready for unpacking
             * @param unpack
             */
            void setUnpacking(bool unpack=false);

            ASTNode* getFunctionAST() const { return findFunction(_root); }

            /*!
             * returns all stored typing err messages
             * @return
             */
            std::vector<std::string> typingErrMessages() const { return _typingErrMessages; }
        };
    }
}

#endif //TUPLEX_IRGENERATOR_H