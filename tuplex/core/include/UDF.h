//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_UDF_H
#define TUPLEX_UDF_H

#include <Utils.h>
#include <Schema.h>
#include <AnnotatedAST.h>
#include <CompiledFunction.h>
#include <LLVMEnvironment.h>
#include <Python.h>
#include <ClosureEnvironment.h>
#include <IFailable.h>

namespace tuplex {
    class UDF : public IFailable {
    private:
        codegen::AnnotatedAST _ast;    //! annotated abstract syntax tree for this UDF
        bool _isCompiled; //! indicate whether the UDF could be compiled or not
        bool _failed; //! UDF not usable?
        std::string _code;  //! actual python code
        std::string _pickledCode; //! fallback mechanism when UDF can't be compiled
        UDF &hintInputParameterType(const std::string &param, const python::Type &type);

        bool hintParams(std::vector<python::Type> hints, std::vector<std::tuple<std::string, python::Type> > params,
                        bool silent = false, bool removeBranches=false);

        Schema _hintedInputSchema; // the schema which was assigned via hintInputSchema
        Schema _inputSchema; // the schema the UDF actually expects, deduced from hintInputSchema
        Schema _outputSchema; // the output schema the UDF produces

        bool _dictAccessFound;
        bool _rewriteDictExecuted;

        python::Type codegenTypeToRowType(const python::Type& type) const;


        void logTypingErrors(bool print=true) const;

        static bool _compilationEnabled; // globally
        static bool _allowNumericTypeUnification; // globally

        /*!
         * checks whether any active branch has a PyObject typing => this would imply
         * it's not compilable
         * @return
         */
        bool hasPythonObjectTyping() const;
    public:
        UDF(const std::string& pythonLambdaStr,
            const std::string& pickledCode="",
            const ClosureEnvironment& globals=ClosureEnvironment());

        UDF(const UDF &other) : _ast(other._ast),
                                _isCompiled(other._isCompiled),
                                _failed(other._failed),
                                _code(other._code),
                                _pickledCode(other._pickledCode),
                                _outputSchema(other._outputSchema),
                                _inputSchema(other._inputSchema),
                                _dictAccessFound(other._dictAccessFound),
                                _rewriteDictExecuted(other._rewriteDictExecuted) {}

        /*!
         * get closure environment back, i.e. all used modules and globals within this UDF.
         * @return ClosureEnvironment class
         */
        const ClosureEnvironment& globals() const { return _ast.globals(); }

        /*!
         * returns output schema with a guaranteed tuple type as row type.
         * @return
         */
        Schema getOutputSchema() const;

        Schema getInputSchema() const;

        std::vector<std::tuple<std::string, python::Type> > getInputParameters() const;

        /*!
         * hint schema to udf so type of input parameters can be deducted.
         * @param schema
         * @param removeBranches whether to remove from AST branches which can't be reached (i.e. must be done for null-value opt to work)
         * @param printErrors whether to print out errors regarding typing or not.
         * @return false, if schema is not compatible with UDF input params or other error happened.
         */
        bool hintInputSchema(const Schema& schema, bool removeBranches=false, bool printErrors=true);

        /*!
         * use PyObjects to trace within UDF and detect types + annotate which branches to use
         * @param sample array of PyObjects to feed to the AST
         * @param inputRowType optional input row type on which to filter samples
         * @param acquireGIL whether function should call lockGil/unlockGil
         * @return whether it succeeded (should be true, unless everyhting resulted in an exception)
         */
        bool hintSchemaWithSample(const std::vector<PyObject*>& sample,
                                  const python::Type& inputRowType=python::Type::UNKNOWN,
                                  bool acquireGIL=false);

        std::string getCode() const { return _code; }

        const codegen::AnnotatedAST& getAnnotatedAST() const { return _ast; }
        codegen::AnnotatedAST& getAnnotatedAST() { return _ast; }

        std::string getPickledCode() const;

        bool isCompiled() const { return _isCompiled; }

        bool empty() const { return _code.empty() && _pickledCode.empty(); }

        bool isPythonLambda() const;
        std::string pythonFunctionName() const;

        /*!
         * set output schema manually. Helpful i.e. when function is not compilable.
         * @param schema
         */
        void setOutputSchema(const Schema& schema) {
            _outputSchema = schema;
        }

        /*!
         * set input schema manually. Necessary for fallback.
         * @param schema
         */
        void setInputSchema(const Schema& schema) {
            _inputSchema = schema;
        }

        /*!
         * remove all internal schemas, type hints etc.
         * @param removeAnnotations whether to remove all annotations from AST nodes as well
         * @return self
         */
        UDF& removeTypes(bool removeAnnotations=true);


        /*!
         * each UDF has a number of parameters. This here is to tell which columns are required for the computation
         * @return list of indices of columns that are accessed.
         */
        std::vector<size_t> getAccessedColumns();

        /*!
         * rewrites UDF in the sense that x['column1'] is converted to x[0] e.g.
         * @param columnNames vector of column names to use for rewriting.
         * @param parameterName which parameter to rewrite, empty string to rewrite single case UDF
         * @return false if e.g. a non-existing column name is accessed.
         */
        bool rewriteDictAccessInAST(const std::vector<std::string>& columnNames, const std::string& parameterName="");

        /*! rewrites UDF to use less params with the given mapping.
         * @param rewriteMap
         */
        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap);

        inline bool allowNumericTypeUnification() const {
            return empty() ? false : getAnnotatedAST().allowNumericTypeUnification();
        }

        /*!
         *
         * @param env
         * @param allowUndefinedBehavior
         * @param sharedObjectPropagation
         */
        codegen::CompiledFunction compile(codegen::LLVMEnvironment& env,
                                          bool allowUndefinedBehavior,
                                          bool sharedObjectPropagation);


        codegen::CompiledFunction compileFallback(codegen::LLVMEnvironment& env,
                                                  llvm::BasicBlock* constructorBlock,
                                                  llvm::BasicBlock* destructorBlock);

        /*!
         * produces using graphviz a pdf of the AST tree for this function.
         * @param filePath where to save the pdf (only local allowed).
         */
        void saveASTToPDF(const std::string& filePath);

        /*!
         * whether input args are expected to be passed as dict
         * @return
         */
        bool dictMode() const;

        /*!
         * disable compilation for this UDF explicitly, i.e. it will need to get
         * executed via the python interpreter.
         */
        inline void markAsNonCompilable() { _isCompiled = false; }

        /*!
         * execute the given UDF over a batch of input objects. Bad rows are ignored.
         * @param in_rows the rows
         * @param columns if non-empty, try out dict mode!
         * @param acquireGIL whether to call lockGil/unlockGil pair
         * @return vector of rows which have been successfully processed via the UDF.
         */
        std::vector<PyObject*> executeBatchViaInterpreter(const std::vector<PyObject*>& in_rows,
                                                          const std::vector<std::string>& columns,
                                                          bool acquireGIL=true) const;

        /*!
         * enable compilation (partial or full) of UDFs
         */
        static void enableCompilation() { _compilationEnabled = true; }

        /*!
         * disable UDF compilation, i.e. they are forced to fallback mode (pure python via cloudpickle)
         */
        static void disableCompilation() { _compilationEnabled = false; }

        /*!
         * enable numeric type unification, i.e. for variables across if branches.
         */
        static void enableNumericTypeUnification() { _allowNumericTypeUnification = true; }

        /*!
         * disable numeric type unification, i.e. for variables across if branches. Leads to correct types,
         * but increases speculation burden.
         */
        static void disableNumericTypeUnification() { _allowNumericTypeUnification = false; }

        // make sure it is ONE kind...
        // --> else rewrite is required...
        // general rewrite would reduce problem to tupleMode...
        // however, rewrite would need to occur in python module...
        // an idea would be also to patch the bytecode from dict mode to tuple...
        // --> https://rushter.com/blog/python-bytecode-patch/
        // maybe this is actually the most elegant solution for cloudpickled code???
        // --> however, this may cause a problem if a user wants to test his/her function.
        // HENCE, best is to use ONE mode exclusively...
    };
}

#endif //TUPLEX_UDF_H