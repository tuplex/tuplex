//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONCONTEXT_H
#define TUPLEX_PYTHONCONTEXT_H

#include "PythonWrappers.h"
#include "PythonException.h"
#include "PythonDataSet.h"
#include "PythonMetrics.h"
#include "JobMetrics.h"


namespace tuplex {

    /*!
     * return the current default Context Options as JSON object
     * @return string (JSON)
     */
    extern std::string getDefaultOptionsAsJSON();

    /*!
    * context abstraction of C++ class which provides python bindings
    */
    class PythonContext {
    private:
        Context *_context;

        ///! for serialization, this is a hardcoded constant of the minimum partition size to alloc
#ifndef NDEBUG
        const size_t allocMinSize = 1024 * 64; // 64KB
#else
        const size_t allocMinSize = 1024 * 128; // 128KB
#endif

        //! fast parallelization functions for single objects

        /*!
        * fast Python to framework code for booleans
        * @param listObj list containing bool objects
        * @param columns optional list of column names
        * @return Dataset
        */
        DataSet &fastBoolParallelize(PyObject *listObj, const std::vector<std::string> &columns);

        /*!
         * fast Python to framework code for single integers
         * @param listObj list containing integers
         * @param columns optional list of column names
         * @param upcast upcast booleans if found to integers
         * @return Dataset
         */
        DataSet &fastI64Parallelize(PyObject *listObj, const std::vector<std::string> &columns, bool upcast);

        /*!
         * fast Python to framework code for single floats
         * @param listObj list containing floats
         * @param columns optional list of column names
         * @param upcast upcast booleans and ints if found to floats
         * @return Dataset
         */
        DataSet &fastF64Parallelize(PyObject *listObj, const std::vector<std::string> &columns, bool upcast);

        /*!
         * fast Python to framework code for strings
         * @param listObj list containing strings
         * @param columns optional list of column names
         * @return Dataset
         */
        DataSet &fastStrParallelize(PyObject *listObj, const std::vector<std::string> &columns);

        // Note: for even faster code, perhaps code-generate the translation...

        // fast mixed tuple transfer
        DataSet &
        fastMixedSimpleTypeTupleTransfer(PyObject *listObj, const python::Type& majType, const std::vector<std::string> &columns);


        // maybe also for mixed tuple elements if they are all primitives...


        DataSet &parallelizeAnyType(boost::python::list &L, const python::Type &majType,
                                    const std::vector<std::string> &columns);

        /*!
         * serialize exceptions into partitions with pickled PyObjects
         * @param exceptions map of row index to exception
         * @param opID parallelize operator ID
         * @return vector of exception partitions
         */
        std::vector<Partition *> serializeExceptions(std::vector<std::tuple<size_t, PyObject *>> exceptions, int64_t opID);

        python::Type inferType(const boost::python::list &L) const;

        /*!
         * infer what are the columns in a probabilistic fashion
         * @param L
         * @param normalThreshold if observations are less than the normalThreshold given as prob between 0-1, then ignore
         * @return map of column name and most likely type for each
         */
        std::unordered_map<std::string, python::Type>
        inferColumnsFromDictObjects(const boost::python::list &L, double normalThreshold);

        inline size_t sampleSize(const boost::python::list &L) const {
            // sample size to determine how many entries should be scanned to get python types
            static const size_t DEFAULT_SAMPLE_SIZE = 16;
            // todo: get from options
            size_t numElements = boost::python::len(L);
            auto numSample = numElements < DEFAULT_SAMPLE_SIZE ? numElements : DEFAULT_SAMPLE_SIZE;
            return numSample;
        }

        DataSet &
        strDictParallelize(PyObject *listObj, const python::Type &rowType, const std::vector<std::string> &columns);

        // bad parallelize objects, i.e those who don't fit the inferred type
        std::vector<std::tuple<size_t, PyObject *>> _badParallelizeObjects;

        std::vector<std::tuple<std::string, size_t>> _inputPartitionInfo;
        std::unordered_map<std::string, std::tuple<size_t, size_t, size_t>> _inputPartitionToPythonObjectsMap;

        inline PythonDataSet makeError(const std::string& message) {
            PythonDataSet pds;
            pds.wrap(&_context->makeError(message));
            return pds;
        }
    public:

        /*!
         * gets the metrics for the context object.
         * @return PythonMetrics wrapper around internal JobMetrics class
         */
        PythonMetrics getMetrics() {
            std::shared_ptr<JobMetrics> metrics = _context->getMetrics();
            PythonMetrics pyth_metrics;
            pyth_metrics.wrap(metrics);
            return pyth_metrics;
        }

        /*!
         * starts a context with optional settings
         * @param name optional name of the context
         * @param runtimeLibraryPath path to runtime library
         * @param options json string with parameters.
         */
        PythonContext(const std::string &name,
                      const std::string &runtimeLibraryPath,
                      const std::string& options);

        explicit PythonContext(const std::string &runtimeLibraryPath) : PythonContext("", runtimeLibraryPath,
                                                                             "") {}

        ~PythonContext();

        /*!
         * parallelizes list of simple types (i.e. bool, int, float, str) or tuples
         * @param L python list object
         * @param cols python list object with column names
         * @param schema python object to define a schema
         * @return PythonDataSet wrapper around internal DataSet class
         */
        PythonDataSet parallelize(boost::python::list L, boost::python::object cols = boost::python::object(),
                                  boost::python::object schema = boost::python::object(), bool autoUnpack = true);

        /*!
         * reads one (or multiple) csv files into memory
         * @param pattern file pattern (glob pattern) of csv files to read
         * @param cols None or list of strings to describe column names
         * @param autodetect_header whether to detect header automatically
         * @param header whether files have a header
         * @param delimiter optionally give delimiter
         * @param quotechar quotechar
         * @param null_values list of null values
         * @param type_hints hints for the types of the columns
         * @return PythonDataSet wrapper around internal DataSet class corresponding to a csv read call
         */
        PythonDataSet csv(const std::string &pattern,
                          boost::python::object cols = boost::python::object(),
                          bool autodetect_header = true,
                          bool header = false,
                          const std::string &delimiter = "",
                          const std::string &quotechar = "\"",
                          boost::python::object null_values = boost::python::object(),
                          boost::python::object type_hints = boost::python::object());

        /*!
         * reads one (or multiple) text files into memory
         * @param pattern file pattern (glob pattern) of csv files to read
         * @param null_values list of null values
         * @return PythonDataSet wrapper around internal DataSet class corresponding to a text read call
         */
        PythonDataSet text(const std::string &pattern, boost::python::object null_values = boost::python::object());

        /*!
         * reads one (or multiple) orc files into memory
         * @param pattern file pattern (glob pattern) of orc files to read
         * @param cols None or list of strings to describe column names
         * @return PythonDataSet wrapper around internal DataSet class corresponding to a orc read call
         */
        PythonDataSet orc(const std::string &pattern,
                          boost::python::object cols = boost::python::object());

        /*!
         * retrieves options as flattened dictionary.
         * @return dictionary with all options.
         */
        boost::python::dict options() const;



        // helper functions to deal with file systems
        /*!
         * returns a list of URIs of all files found under the current pattern
         * @param pattern a standard UNIX wildcard pattern with a prefix like file:// or s3://
         * @return list of strings
         */
        boost::python::object ls(const std::string& pattern) const;

        /*!
         * copies all files matching pattern to a target destination
         * @param pattern
         * @param target
         */
        void cp(const std::string& pattern, const std::string& target) const;

        /*!
         * removes all files matching a pattern
         * @param pattern
         */
        void rm(const std::string& pattern) const;
    };

}

#endif //TUPLEX_PYTHONCONTEXT_H