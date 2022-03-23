//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_SAMPLEPROCESSOR_H
#define TUPLEX_SAMPLEPROCESSOR_H

#include <vector>
#include <string>
#include <logical/LogicalOperator.h>
#include <Row.h>
#include <PythonHelpers.h>
#include <Utils.h>

namespace tuplex {

    class LogicalOperator;

    /*!
     * helper class to process a sample of rows through a pipeline (yet only with a trafo stage)
     * using the python interpreter (embedded)
     */
    class SampleProcessor {
    private:

        struct TraceResult {
            Row outputRow;
            ExceptionCode ec;
            int64_t lastOperatorID;
            std::string exceptionTraceback;

            TraceResult() : ec(ExceptionCode::UNKNOWN), lastOperatorID(-1) {}
        };

        std::vector<std::shared_ptr<LogicalOperator>> _operators; // weak references to operators


        // deserialized python functions for each operator
        bool _udfsCached;
        std::unordered_map<int64_t, PyObject*> _TUPLEXs; // operatorID -> PyObject*

        void releasePythonObjects();

        void cachePythonUDFs();


        // apply operator incl. record of exception handling
        python::PythonCallResult applyOperator(LogicalOperator *op, PyObject* pyRow);

        // individual operators for easier handling
        python::PythonCallResult applyMap(bool dictMode, PyObject* TUPLEX, PyObject* pyRow, const std::vector<std::string>& columns);
        python::PythonCallResult applyWithColumn(bool dictMode, PyObject* TUPLEX, PyObject* pyRow, const std::vector<std::string>& columns, int idx);
        python::PythonCallResult applyMapColumn(bool dictMode, PyObject* TUPLEX, PyObject* pyRow, int idx);

        TraceResult traceRow(const Row& row); //! trace row through pipeline
    public:

        SampleProcessor() = delete;

        ~SampleProcessor() {
            releasePythonObjects();
        }

        explicit SampleProcessor(const std::vector<std::shared_ptr<LogicalOperator>>& operators) : _operators(operators), _udfsCached(false) {
            for(auto op : operators)
                assert(op);

            cachePythonUDFs();

#ifndef NDEBUG
            // test, call it twice because there is some nasty CPython error...
            cachePythonUDFs();
#endif
            _udfsCached = true;
        }

        /*!
         * basic function to get exceptions for rows
         * @param row input row to pipeline (i.e. what the first operator takes!)
         * @param excludeAvailableResolvers whether to traverse/consider resolvers too
         * @return ExceptionSample struct, holding first row traceback + all rows that caused an exception for specific operator
         */
        ExceptionSample generateExceptionSample(const Row& row, bool excludeAvailableResolvers) noexcept;

        std::vector<std::string> getColumnNames(int64_t operatorID);

        std::shared_ptr<LogicalOperator> getOperator(int64_t operatorID);


        /*!
         * returns operator position in this stage, i.e. 0 for first, ...
         * @param operatorID
         * @return index of operator in stage. -1 if not found
         */
        int getOperatorIndex(int64_t operatorID);

        void sample(size_t numSamples=64); // also include option for general python3 list...

#warning "Todo, add here most likely path processor..."
    };
}

#endif //TUPLEX_SAMPLEPROCESSOR_H