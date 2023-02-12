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
#include <symbols/SymbolTable.h>
#include <symbols/ClosureEnvironment.h>
#include <codegen/IFailable.h>

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
#include <codegen/LLVMEnvironment.h>

#ifdef BUILD_WITH_CEREAL
// @TODO: maybe make this cleaner (one header? precompiled-headers?)
#include "cereal/access.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/map.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/common.hpp"
#include "cereal/archives/binary.hpp"
#endif

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

            std::vector<std::string> _typingErrMessages; // error messages produced by type annotator.

            // holds the AST tree after successful parsing
            std::unique_ptr<ASTNode> _root;
            bool _typesDefined; // lazy check variable whether types are already defined or not

            ClosureEnvironment _globals; // global variables + modules


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
            AnnotatedAST(): _root(nullptr), _typesDefined(false) {}

            AnnotatedAST(const AnnotatedAST& other) : _root(nullptr), _typesDefined(other._typesDefined), _globals(other._globals) {
                cloneFrom(other);
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
             * @param s python source code to parse
             * @return false if string could not be parsed.
             */
            bool parseString(const std::string& s);
            void setGlobals(const ClosureEnvironment& globals) { _globals = globals; }
            const ClosureEnvironment& globals() const { return _globals; }

            bool writeGraphVizFile(const std::string& path);

            /*!
             * invokes graphviz/dot process to create PDF from tree. Per default outputs only nodes, set with_types=true to add types as well.
             * @param path where to store (locally) the pdf file
             * @param with_types whether to include type information or not
             * @return true if success, false else (i.e. when dot is not installed)
             */
            bool writeGraphToPDF(const std::string &path, bool with_types=false);

            /*!
             * specify for the identifier a possible type. This may enable / simplify code generation.
             * Note that an indentifier can have multiple type hints
             * @param identifier
             * @param type
             */
            void addTypeHint(const std::string& identifier, const python::Type& type);


            /*!
             * generates code for a python UDF function
             * @param env LLVM module where to generate code into
             * @param policy UDF compiler policy to use
             * @return bool if code can be generated, false if not
             */
            bool generateCode(LLVMEnvironment *env, const codegen::CompilePolicy& policy);

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
             * @param policy compiler policy
             * @param silentMode determines whether the type inference should log out problems or not
             * @param removeBranches whether to use RemoveDeadBranchesVisitor to prune AST
             * @return whether types could be successfully annotated/defined for all AST nodes
             */
            bool defineTypes(const codegen::CompilePolicy& policy, bool silentMode=false, bool removeBranches=false);

            /*!
             * rerun TypeAnnotatorVisitor on AST, use partially available information already available for edge caes.
             * @param policy
             * @param silentMode
             * @param removeBranches
             * @return
             */
            bool redefineTypes(const codegen::CompilePolicy& policy, bool silentMode=false, bool removeBranches=false);

            /*!
             * for easier handling, function arguments may be auto unpacked. This annotates the (first) found function
             * to be ready for unpacking
             * @param unpack
             */
            void setUnpacking(bool unpack=false);

            ASTNode* getFunctionAST() const { return findFunction(_root.get()); }

            /*!
             * returns all stored typing err messages
             * @return
             */
            std::vector<std::string> typingErrMessages() const { return _typingErrMessages; }

            void reduceConstantTypes();

            // cereal serialization functions
            template<class Archive> void serialize(Archive &ar) {
                ar(_irFuncName, _typeHints, _typingErrMessages, _root, _typesDefined, _globals);
            }
        };
    }
}

#endif //TUPLEX_IRGENERATOR_H