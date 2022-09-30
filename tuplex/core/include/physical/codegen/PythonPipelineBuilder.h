//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONPIPELINEBUILDER_H
#define TUPLEX_PYTHONPIPELINEBUILDER_H

#include <ExceptionCodes.h>
#include <UDF.h>
#include <Base.h>
#include <logical/AggregateOperator.h>

namespace tuplex {


    /// class to generate a pure python version of the pipeline (incl. resolvers & Co).
    // ==> needed i.e. for bad input rows!
    // ==> needed, when the hybrid resolver doesn't work.
    class PythonPipelineBuilder {
    public:
        PythonPipelineBuilder(const std::string& funcName);

        void objInput(int64_t operatorID, const std::vector<std::string>& columns=std::vector<std::string>());

        void csvInput(int64_t operatorID, const std::vector<std::string>& columns=std::vector<std::string>(),
                      const std::vector<std::string>& na_values=std::vector<std::string>());

        /*!
         * add new cell based input, i.e. strings that get parsed to types depending on the hierarchy being defined.
         * @param operatorID ID of the input operator
         * @param columns names of the columns. If empty, ignored
         * @param typeHints column index -> python type hint.
         * @param na_values list of strings to identify as NULL/None
         * @param numColumns number of input columns
         * @param projectionMap mapping how in the case of projection pushdown columns are mapped.
         */
        void cellInput(int64_t operatorID,
                       std::vector<std::string> columns=std::vector<std::string>(),
                       const std::vector<std::string>& na_values=std::vector<std::string>(),
                       const std::unordered_map<size_t, python::Type>& typeHints = std::unordered_map<size_t, python::Type>(),
                       size_t numColumns=0, const std::unordered_map<int, int>& projectionMap=std::unordered_map<int, int>());

        void mapOperation(int64_t operatorID, const UDF& udf, const std::vector<std::string>& output_columns=std::vector<std::string>());
        void filterOperation(int64_t operatorID, const UDF& udf);
        void mapColumn(int64_t operatorID, const std::string& columnName, const UDF& udf);
        void withColumn(int64_t operatorID, const std::string& columnName, const UDF& udf);


        // join operator => note that this simply adds a dict lookup
        void innerJoinDict(int64_t operatorID, const std::string& hashmap_name, tuplex::option<std::string> leftColumn,
                           const std::vector<std::string>& bucketColumns=std::vector<std::string>{},
                           option<std::string> leftPrefix=option<std::string>::none,
                           option<std::string> leftSuffix=option<std::string>::none,
                           option<std::string> rightPrefix=option<std::string>::none,
                           option<std::string> rightSuffix=option<std::string>::none);
        void leftJoinDict(int64_t operatorID, const std::string& hashmap_name,
                           tuplex::option<std::string> leftColumn,
                           const std::vector<std::string>& bucketColumns=std::vector<std::string>{},
                           option<std::string> leftPrefix=option<std::string>::none,
                           option<std::string> leftSuffix=option<std::string>::none,
                           option<std::string> rightPrefix=option<std::string>::none,
                           option<std::string> rightSuffix=option<std::string>::none);

        // output operator, i.e. checks whether necessary output information can be extracted dynamically

        // @TODO: add here type checks for output type! If they fail, exception for this operator is produced!
        // ==> this helps i.e. when statically typed output formats are required!
        void tuplexOutput(int64_t operatorID, const python::Type& finalOutputType);

        // output just python objects (no type check)
        // --> note: this will be also used for hashmap output!!!
        void pythonOutput();


        void csvOutput(char delimiter=',', char quotechar='"', bool newLineDelimited=true);

        void resolve(int64_t operatorID, ExceptionCode ec, const UDF& udf); // resolver UDF (resolve last operator)
        void ignore(int64_t operatorID, ExceptionCode ec); // ignore specific exception code (i.e. filter out based on exception code)

        std::string getCode() const { return _imports + "\n" + _header + "\n" + functionSignature() + headCode() + _ss.str() + tailCode(); }

        // aggregate functions:
        void pythonAggByKey(int64_t operatorID,
                            const std::string& hashmap_name,
                            const UDF& aggUDF,
                            const std::vector<size_t>& aggColumns,
                            const Row& initial_value);
        void pythonAggGeneral(int64_t operatorID,
                              const std::string& agg_intermediate_name,
                              const tuplex::UDF &aggUDF,
                              const Row& initial_value);


        static std::string udfToByteCode(const UDF& udf);
    private:
        std::string _funcName;
        std::stringstream _ss;
        std::string _imports; // where to put import statements
        std::vector<std::string> _optArgs; // optional arguments to add to the function definition
        std::string _header;
        int _indentLevel;
        static const int _tabFactor = 4; // 4 spaces are one tab

        bool _pipelineDone;


        int _envCounter; // for vars & envs

        std::string _lastRowName;
        std::string _lastInputRowName;

        bool _parseCells; // whether to parse input cells


        std::string emitClosure(const UDF& udf);

        std::string lastInputRowName() const {
            return _lastInputRowName;
        }

        void nextEnv() { _envCounter++; }

        std::string envSuffix() const {
            return fmt::format("{:02d}", _envCounter);
        }

        void setLastInputRowName(const std::string& lastInputRowName) { _lastInputRowName = lastInputRowName; }


        std::string row() const {
            return _lastRowName;
        }

        void setRow(const std::string& rowName) {
            _lastRowName = rowName;
        }

        /*!
         * param name for function of input object/input row
         * @return
         */
        std::string inputRowName() const { return "input_row"; }

        /*!
         * generates except code from try block
         * @param os
         */
        inline void exceptCode(std::ostream& os, int64_t opID, std::string inputRow="") {
            os<<"except Exception as e:\n";
            exceptInnerCode(os, opID, "e", "", 1);
        }


        inline void exceptInnerCode(std::ostream& os, int64_t opID, std::string exception_id="e", std::string inputRow="", int indentLevels=0) {
            // if not given, just use last name...
            if(inputRow.empty())
                inputRow = lastInputRowName();

            std::stringstream ss;
            ss<<"res['exception'] = "<<exception_id<<"\n";
            ss<<"res['exceptionOperatorID'] = "<<opID<<"\n";
            ss<<"res['inputRow'] = "<<inputRow<<"\n";
            //ss<<"return res";

            os<<indentLines(indentLevels, ss.str());
        }

        std::vector<std::string> _tailCode; // code at indent levels to complete.
        void addTailCode(const std::string& code) {
            _tailCode.emplace_back(indentLines(_indentLevel, code));
        }

        std::string tailCode() const {
            std::stringstream ss;
            for(auto it = _tailCode.crbegin(); it != _tailCode.crend(); ++it) {
                ss<<*it<<"\n";
            }
            return ss.str();
        }

        std::string _headCode;
        std::string headCode() const {
            return indentLines(1, _headCode);
        }

        // because resolvers are added lazily, need to lazy flush function with exception handling
        struct Function {
            std::string _udfCode; // the UDF code, used to quickly swap out UDFs... Function should be called f
            std::string _code; // the actual code for the function (i.e. what goes into the try block)
            int64_t _operatorID; // the operator ID, required for exception handling...
            std::vector<std::tuple<ExceptionCode, int64_t, std::string>> _handlers; // list of handlers for the function (i.e. generate if...elif...else stmts)
        };

        Function _lastFunction;
        void flushLastFunction(); // add last function to code

        std::string replaceTabs(const std::string& s) const;

        std::string columnsToList(const std::vector<std::string>& columns);
        static std::string toByteCode(const std::string& s);

        //! helper function to write one or more lines at specific indent level
        std::string indentLines(int indentFactor, const std::string& s) const;
        void writeLine(const std::string& s);
        void indent() { _indentLevel++; }
        void dedent() { _indentLevel--; }

        inline std::string functionSignature() const {
            std::stringstream ss;

            // init function via def + add all optional params
            if(_optArgs.empty())
                ss<<"def "<<_funcName<<"("<<inputRowName();
            else {

                ss<<"def "<<_funcName<<"("<<inputRowName();
                for(const auto& arg: _optArgs)
                    ss<<", "<<arg;

            }

            // keyword args (here: the indicator whether str cells are used or not)
            ss<<", parse_cells="<<(_parseCells ? "True" : "False")<<"):\n";
            ss.flush();
            return ss.str();
        }
    };

    /*!
     * generates a function that combines two aggregates
     */
    extern std::string codegenPythonCombineAggregateFunction(const std::string& function_name, int64_t operatorID,
                                                             const AggregateType& agg_type,
                                                             const Row& initial_value,
                                                             const UDF& combine_udf);

}

#endif //TUPLEX_PYTHONPIPELINEBUILDER_H