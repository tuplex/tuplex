//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <AnnotatedAST.h>
#include <graphviz/GraphVizGraph.h>
#include <ASTNodes.h>
#include <BlockGeneratorVisitor.h>
#include <SymbolTable.h>
#include "CleanAstVisitor.h"
#include <UnrollLoopsVisitor.h>
#include <TypeAnnotatorVisitor.h>
#include <Logger.h>
#include <ReducableExpressionsVisitor.h>
#include <ReduceExpressionsVisitor.h>
#include <RemoveDeadBranchesVisitor.h>
#include <TypeSystem.h>
#include <Pipe.h>
#include <parser/Parser.h>

#ifndef NDEBUG
static int g_func_counter = 0;
#endif

namespace tuplex {
    namespace codegen {
        void AnnotatedAST::release() {
            if(_root) {
                delete _root;
                _root = nullptr;
            }
        }

        ASTNode* AnnotatedAST::findFunction(ASTNode *root) const {
            if(!root)
                return nullptr;

            // called to retrieve the last function of a statement object
            if(root->type() == ASTNodeType::Module)
                return findFunction(static_cast<NModule*>(root)->_suite);
            else if(root->type() == ASTNodeType::Suite) {
                return findFunction(static_cast<NSuite*>(root)->_statements.back());
            }
            else if(root->type() == ASTNodeType::Function)
                return root;
            else if(root->type() == ASTNodeType::Lambda)
                return root;
            else {
                Logger::instance().logger("codegen").error("illegal AST node type found while searching for function");
                return nullptr;
            }
        }

        bool AnnotatedAST::generateCode(LLVMEnvironment *env, bool allowUndefinedBehavior, bool sharedObjectPropagation) {
            assert(env);

            auto& logger = Logger::instance().logger("codegen");

            if(!_root)
                return false;

            // fix types in the graph. I.e. no more sets,
            // each node gets a type assigned.
            // needed by the block generator
            if(!defineTypes(false))
                return false;

            // note: this may screw up things!
            ASTNode *func = findFunction(_root);
            assert(func->type() == ASTNodeType::Lambda || func->type() == ASTNodeType::Function);

            // actual code generation
            tuplex::codegen::BlockGeneratorVisitor bgv(env, _typeHints, allowUndefinedBehavior, sharedObjectPropagation);
            bgv.addGlobals(func, _globals);

            try {
                func->accept(bgv);
            } catch(const std::exception& e) {
                // remove bad function from llvm module
                _irFuncName = bgv.getLastFuncName();

                std::string errMessage = "code generation for Python UDF failed.\n\nDetails: " + std::string(e.what());

                // remove from llvm module
                if(!_irFuncName.empty()) {
                    auto func = env->getModule()->getFunction(_irFuncName);

                    // print bad function in debug mode & remove from module
                    if(func) {
#ifndef NDEBUG
                        auto funcCode = printFunction(func, true);
                        trim(funcCode);
                        errMessage += "\n\n" + funcCode + "\n";
#endif
                        func->eraseFromParent();
                    }
                }

                logger.error(errMessage);
                return false;
            }

            // name of function
            _irFuncName = bgv.getLastFuncName();

            // if function is not compilable, return false
            return true;
        }

        python::Type AnnotatedAST::getReturnType() const {
            ASTNode *node = findFunction(_root);

            if(!_typesDefined) {
                Logger::instance().logger("codegen").error("types were not defined for UDF. Can't return returntype.");
                return python::Type::UNKNOWN;
            }

            if(!node)
                return python::Type::UNKNOWN;

            if(ASTNodeType::Lambda == node->type() || ASTNodeType::Function == node->type()) {
                auto retType = node->getInferredType();
                assert(retType.isFunctionType());
                return retType.getReturnType();
            } else {
                Logger::instance().logger("codegen").error("could not find function AST Node");
                return python::Type::UNKNOWN;
            }
        }

        FunctionArguments AnnotatedAST::getParameterTypes() const {
            ASTNode *node = findFunction(_root);

            FunctionArguments fa;

            if(!node)
                return fa;

            if(ASTNodeType::Lambda == node->type()) {
                NLambda *lambda = dynamic_cast<NLambda*>(node);
                if(lambda->_arguments)
                    for(auto& arg : lambda->_arguments->_args) {
                        fa.argTypes.push_back(arg->getInferredType());
                    }
                return fa;
            } else if(ASTNodeType::Function == node->type()) {
                NFunction *func = dynamic_cast<NFunction*>(node);
                for(auto& arg : func->_parameters->_args) {
                    fa.argTypes.push_back(arg->getInferredType());
                }
                return fa;
            } else {
                Logger::instance().logger("codegen").error("could not find function AST Node");
                return fa;
            }
        }

        AnnotatedAST& AnnotatedAST::removeParameterTypes() {
            ASTNode *node = findFunction(_root);

            if(!node)
                return *this;

            if(ASTNodeType::Lambda == node->type()) {
                NLambda* lam = dynamic_cast<NLambda*>(node); assert(lam);

                for(auto& arg : lam->_arguments->_args)
                    arg->setInferredType(python::Type::UNKNOWN);
            }

            if(ASTNodeType::Function == node->type()) {
                NFunction* func = dynamic_cast<NFunction*>(node); assert(node);

                for(auto& arg : func->_parameters->_args)
                    arg->setInferredType(python::Type::UNKNOWN);
            }

            // also reset types defined + hints!
            _typesDefined = false;
            _typeHints.clear();
            _typingErrMessages.clear();

            return *this;
        }


        bool AnnotatedAST::parseString(const std::string &s, bool allowNumericTypeUnification) {
            // for a clean restart, later cache in memory!
            release();

            _allowNumericTypeUnification = allowNumericTypeUnification;

            // using ANTLR4 parser
            assert(!_root);
            _root = tuplex::parseToAST(s);

            // successful parse if _root is not nullptr
            if(_root != nullptr) {
                try {
                    processAST();
                }
                catch(Exception& e) {
                    Logger::instance().logger("Python parser").error("exception encountered while processing AST: "+e.getMessage());
                    if(_root)
                        delete _root;
                    _root = nullptr;
                }
            }
            else {
                Logger::instance().logger("Python parser").error("could not parse provided code.");
                _root = nullptr;
            }

            return _root != nullptr;
        }

        void AnnotatedAST::cloneFrom(const AnnotatedAST &other) {
            // as of now, deep copy only the relevant parts.
            release();
            assert(!_root);
            _root = other._root ? other._root->clone() : nullptr;
            _irFuncName = other._irFuncName;
            _typeHints = other._typeHints;
            _typesDefined = other._typesDefined;
            _globals = other._globals;
            _allowNumericTypeUnification = other._allowNumericTypeUnification;
        }

        bool AnnotatedAST::writeGraphVizFile(const std::string &path) {
            if(!_root)return false;

            GraphVizGraph graph;
            graph.createFromAST(_root);
            return graph.saveAsDot(path);
        }

        bool AnnotatedAST::writeGraphToPDF(const std::string &path) {
            if(!_root)return false;

            GraphVizGraph graph;
            graph.createFromAST(_root);
            return graph.saveAsPDF(path);
        }

        void AnnotatedAST::processAST() {
            auto& logger = Logger::instance().defaultLogger();

            if(!_root)
                return;

            assert(_root);

#ifndef NDEBUG
#ifdef GENERATE_PDFS
            writeGraphToPDF(std::to_string(g_func_counter++) + "_01_ast.pdf");
#else
            logger.debug("writing Python AST to PDF skipped.");
#endif
#endif

            // call clean visitor
            CleanAstVisitor cv;
            _root->accept(cv);

            // next visitor transforms AST to convert any unknown names into
            // NameErrors!
            // i.e. for a function lambda x: math.cos(x) where math is not imported,
            // this would result in NameError: name 'math' is not defined
            // when math is imported and i.e. an unknown attribute is accessed, this would result in
            // AttributeError: module 'math' has no attribute 'jdhkgjk'
            // => clean this upfront!
            // also clean raise 20 into TypeError!
            // @TODO:

            // also include check whether UDF always throws...


            // in addition call dead-branch removal visitor (helpful especially when using null-valu optimization!)
            RemoveDeadBranchesVisitor rdb;
            _root->accept(rdb);
#ifndef NDEBUG
#ifdef GENERATE_PDFS
            writeGraphToPDF(std::to_string(g_func_counter++) + "_02_cleaned_ast.pdf");
#else
            logger.debug("writing cleaned Python AST to PDF skipped.");
#endif
#endif

            // Steps to be carried out before type inference (for cleaning up everything!)
            // 1. fold expressions => i.e. if we have 2 + 3 reduce this to 5!
            // => needs to be done in bottom-up (i.e. postorder) manner
            // ==> necessary for the next step which is the type inference
            // this means, in each expression at least some unknown type occurs!
            ReduceExpressionsVisitor rev(_globals);
            _root->accept(rev);
#ifndef NDEBUG
#ifdef GENERATE_PDFS
            writeGraphToPDF(std::to_string(g_func_counter++) + "_03_reduced_expr_ast.pdf");
#else
            logger.debug("writing constant-folded Python AST to PDF skipped.");
#endif
#endif

            // unroll for node if expression is a tuple
            UnrollLoopsVisitor ulv;
            _root->accept(ulv);
#ifndef NDEBUG
#ifdef GENERATE_PDFS
            writeGraphToPDF(std::to_string(g_func_counter++) + "_04_unrolled_for_loops_ast.pdf");
#else
            logger.debug("writing for loops unrolled Python AST to PDF skipped.");
#endif
#endif
            // in debug mode, make sure that the tree is fully reduced!
            assert(!containsReducableExpressions(_root));
        }

        void AnnotatedAST::addTypeHint(const std::string &identifier, const python::Type &type) {
            auto it = _typeHints.find(identifier);
            bool newHint = false;
            if(it != _typeHints.end())
                newHint = it->second != type;

            _typeHints[identifier] = type;

            // if different from old, reset
            if(newHint)
                _typesDefined = false;
        }

        std::vector<std::string> AnnotatedAST::getParameterNames() const {

            if(!_root)
                return std::vector<std::string>();

            assert(_root);

            ASTNode *node = findFunction(_root);

            std::vector<std::string> names;

            if(ASTNodeType::Lambda == node->type()) {
                NLambda *lambda = static_cast<NLambda*>(node);
                if(!lambda->_arguments)
                    return std::vector<std::string>();

                for(const auto& arg: lambda->_arguments->_args) {
                    assert(arg->type() == ASTNodeType::Parameter);
                    NParameter *param = static_cast<NParameter*>(arg);
                    assert(param->_identifier);
                    names.push_back(param->_identifier->_name);
                }
            } else if(ASTNodeType::Function == node->type()) {
                NFunction *function = static_cast<NFunction*>(node);

                for(const auto& arg: function->_parameters->_args) {
                    assert(arg->type() == ASTNodeType::Parameter);
                    NParameter *param = static_cast<NParameter*>(arg);
                    assert(param->_identifier);
                    names.push_back(param->_identifier->_name);
                }

            } else {
                Logger::instance().logger("codegen").error("could not find callable udf");
            }

            return names;
        }

        void AnnotatedAST::assignParameterType(ASTNode *arg) {
            assert(arg);
            assert(arg->type() == ASTNodeType::Parameter);
            auto param = dynamic_cast<NParameter*>(arg);
            assert(param->_identifier);
            auto identifier = param->_identifier->_name;

            if(param->_default)
                throw std::runtime_error("parameter " + identifier + " is a keyword argument. Not allowed for Tuplex functions.");

            // find type in hints, SHOULD exist!
            auto it = _typeHints.find(identifier);
            if(it == _typeHints.end())
                throw std::runtime_error("could not find any type hint for parameter " + identifier);
            auto type = it->second;

            if(param->_annotation) {
                // translate, compatible with hin?
                // => upquery result in graph?
                // => add additional check in generated code?
                throw std::runtime_error("not yet supported");
            }

            // set inferred type to found type
            param->_identifier->setInferredType(type);
            param->setInferredType(type);
        }

        void AnnotatedAST::hintFunctionParameters(ASTNode *node) {
            assert(node->type() == ASTNodeType::Lambda || node->type() == ASTNodeType::Function);

            if(node->type() == ASTNodeType::Lambda) {
                // go through all params, check whether there is a type hint or not...

                auto lam = dynamic_cast<NLambda*>(node);
                for(auto& arg : lam->_arguments->_args)
                    assignParameterType(arg);
            }

            if(node->type() == ASTNodeType::Function) {
                auto fun = dynamic_cast<NFunction*>(node);
                for(auto& arg : fun->_parameters->_args)
                    assignParameterType(arg);
            }
        }

        bool AnnotatedAST::defineTypes(bool silentMode, bool removeBranches) {

            // reset err messages
            _typingErrMessages.clear();
            clearCompileErrors();

            if(!_root)
                return false;

            // lazy check
            if(_typesDefined)
                return true;

            // process AST
            // 2. Semantic analysis
            // create symbol table and run type annotator

            // 2.1 (initial) Symboltable
            auto table = SymbolTable::createFromEnvironment(&_globals);
            if(!table) {
                // e.g. modules missing etc.
                // => use fallback mode!
                Logger::instance().defaultLogger().info("Creating symbol table failed.");
                return false;
            }

            // set types explicitly for outer function, if there are type hints perform compatibility check
            hintFunctionParameters(findFunction(_root));

            // 2.2 run type annotator using the symbol table
            TypeAnnotatorVisitor tav(*table, _allowNumericTypeUnification);
            tav.setFailingMode(silentMode);
            table->resetScope();
            table->enterScope(); // enter builtin scope
            table->enterScope(); // enter global scope
            table->enterScope(); // enter module/function level scope

            // TypeAnnotatorVisitor may throw an exception when fatal error is reached, hence surround with try/catch
            try {
                _root->accept(tav);
                addCompileErrors(table->getCompileErrors());
                addCompileErrors(tav.getCompileErrors());
                // table->exitScope(); // leave module/function level scope
                // table->exitScope();  // leave global scope
                // table->exitScope(); // leave builtin scope

                // did tav fail? if so remove branches & try again
                if(removeBranches) {
                    clearCompileErrors();
                    RemoveDeadBranchesVisitor rdb;
                    _root->accept(rdb);

                    // run again
                    table->clearCompileErrors();
                    table->resetScope();
                    tav.reset();
                    tav.setFailingMode(silentMode);
                    table->enterScope(); // enter builtin scope
                    table->enterScope(); // enter global scope
                    table->enterScope(); // enter module/function level scope
                    _root->accept(tav);
                    addCompileErrors(table->getCompileErrors());
                    addCompileErrors(tav.getCompileErrors());
                    table->resetScope();
                }
            } catch(const std::runtime_error& e) {
                _typingErrMessages.push_back(e.what());
                return false;
            }

            // fetch messages:
            auto msgs = tav.getErrorMessages();
            for(auto msg : msgs) {
                _typingErrMessages.push_back(std::get<0>(msg));
            }

            // check whether there are any undefined identifiers. If so, log error and break
            auto missingIdentifiers = toVector(tav.getMissingIdentifiers());
            if(!missingIdentifiers.empty()) {
                std::stringstream infoss;
                infoss<<"\n";
                for(const auto& el: missingIdentifiers)
                    infoss<<el<<" ";
                Logger::instance().logger("codegen").error("encountered undefined identifiers: " + infoss.str());
                return false;
            }

            // failed? return false immediately!
            if(tav.failed())
                return false;


#ifndef NDEBUG
#ifdef GENERATE_PDFS
            // print in debug mode graph
            GraphVizGraph graph;
            graph.createFromAST(_root, true);
            graph.saveAsPDF(std::to_string(g_func_counter++) + "_04_ast_with_types.pdf");
#else
            Logger::instance().defaultLogger().debug("writing type-annotated Python AST to PDF skipped.");
#endif
#endif

            bool success = _root->getInferredType() != python::Type::UNKNOWN
                           && _root->getInferredType() != python::Type::makeTupleType({python::Type::UNKNOWN});

            // if it worked, seal AST and do not rerun typing
            if(success) {
                _typesDefined = true;

                // update params in function nodes to final type result!
                setFunctionType(_root, _root->getInferredType());
            }

            // check whether top level has type != unknown
            return success;
        }

        void AnnotatedAST::checkReturnError() {
            auto err = getReturnError();
            if(err != CompileError::COMPILE_ERROR_NONE) {
                throw std::runtime_error(compileErrorToStr(err));
            }
        }

        void AnnotatedAST::setReturnType(const python::Type &targetType) {
            // check what actual return type is
            if(!canUpcastType(getReturnType(), targetType)) {
                throw std::runtime_error("Can't upcast return type from " + getReturnType().desc() + " to " + targetType.desc());
            }

            // overwrite in function AST the return type.
            assert(canUpcastType(getFunctionAST()->getInferredType().getReturnType(), targetType));
            auto funcType = getFunctionAST()->getInferredType();
            funcType = python::Type::makeFunctionType(funcType.getParamsType(), targetType);
            setFunctionType(getFunctionAST(), funcType);
        }

        void AnnotatedAST::setFunctionType(ASTNode *node, const python::Type &type) {
            assert(node);
            assert(node->type() == ASTNodeType::Lambda || node->type() == ASTNodeType::Function);
            assert(type.isFunctionType());

            auto func = dynamic_cast<NFunction*>(node);
            auto lambda = dynamic_cast<NLambda*>(node);

            node->setInferredType(type);

            // extract parameters type and set args
            auto params_type = type.getParamsType();
            if(func) {
                // how many params does the function have?
                assert(params_type.parameters().size() == func->_parameters->_args.size());
                for(unsigned i = 0; i < params_type.parameters().size(); ++i) {
                    func->_parameters->_args[i]->setInferredType(params_type.parameters()[i]);
                }
                func->_parameters->setInferredType(params_type);
            }

            if(lambda) {
                assert(params_type.parameters().size() == lambda->_arguments->_args.size());
                for(unsigned i = 0; i < params_type.parameters().size(); ++i) {
                    lambda->_arguments->_args[i]->setInferredType(params_type.parameters()[i]);
                }
                lambda->_arguments->setInferredType(params_type);
            }
        }

        void AnnotatedAST::setUnpacking(bool unpack) {

            if(!_root)
                return;

            // check whether function is Lambda or not
            ASTNode* funcRoot = findFunction(_root);
            assert(funcRoot);

            if(funcRoot->type() == ASTNodeType::Lambda) {
                // annotate lambda with unpack
                NLambda* lam = dynamic_cast<NLambda*>(funcRoot);
                assert(lam);
                assert(lam->type() == ASTNodeType::Lambda);
                lam->setFirstArgTreatment(!unpack);
            } else if(funcRoot->type() == ASTNodeType::Function) {
                NFunction* func = dynamic_cast<NFunction*>(funcRoot);
                assert(func);
                // annotate function with first arg unpack
                func->setFirstArgTreatment(!unpack);
            } else {
                Logger::instance().defaultLogger().error("unknown ast node of find func returned: " + std::to_string((int)funcRoot->type()));
            }
        }
    }
}