//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CONTEXTOPTIONS_H
#define TUPLEX_CONTEXTOPTIONS_H

#include <map>
#include <string>
#include <URI.h>
#include <StringUtils.h>
#include <Utils.h>

namespace tuplex {

    enum class Backend {
        LOCAL,
        LAMBDA
    };

    /*!
     * represents the parameters passed to a context controlling
     * behaviour of Tuplex
     */
    class ContextOptions {
    private:
        // data is internally stored via a string-string hashmap
        std::map<std::string, std::string> _store;

        // helper function to update values with other options
        void updateWith(const ContextOptions& other);
    public:
        ContextOptions() {}
        ContextOptions(const ContextOptions& other) : _store(other._store)    {}
        inline ContextOptions& operator = (const ContextOptions& other) {
            _store = other._store;
            return *this;
        }
        ~ContextOptions() {}

        // Optimizer configurations
        bool   USE_LLVM_OPTIMIZER() const;                    //! whether to activate the llvm optimizer for generated code or not
        bool OPT_DETAILED_CODE_STATS() const { return stringToBool(_store.at("tuplex.optimizer.codeStats")); }
        bool OPT_GENERATE_PARSER() const { return stringToBool(_store.at("tuplex.optimizer.generateParser")); }
        bool OPT_NULLVALUE_OPTIMIZATION() const { return stringToBool(_store.at("tuplex.optimizer.nullValueOptimization")); }
        bool OPT_SHARED_OBJECT_PROPAGATION() const { return stringToBool(_store.at("tuplex.optimizer.sharedObjectPropagation")); }
        bool OPT_FILTER_PUSHDOWN() const { return stringToBool(_store.at("tuplex.optimizer.filterPushdown")); }
        bool OPT_OPERATOR_REORDERING() const { return stringToBool(_store.at("tuplex.optimizer.operatorReordering")); }
        bool OPT_MERGE_EXCEPTIONS_INORDER() const { return stringToBool(_store.at("tuplex.optimizer.mergeExceptionsInOrder")); }
        bool OPT_INCREMENTAL_RESOLUTION() const { return stringToBool(_store.at("tuplex.optimizer.incrementalResolution")); }
        bool CSV_PARSER_SELECTION_PUSHDOWN() const; //! whether to use selection pushdown in the parser. If false, then full data will be serialized.
        bool INTERLEAVE_IO() const { return stringToBool(_store.at("tuplex.interleaveIO")); } //! whether to first load, compute, then write or use IO thread to interleave IO work with compute work for faster speeds.
        bool RESOLVE_WITH_INTERPRETER_ONLY() const { return stringToBool(_store.at("tuplex.resolveWithInterpreterOnly")); }

        bool REDIRECT_TO_PYTHON_LOGGING() const { return stringToBool(_store.at("tuplex.redirectToPythonLogging")); } //! whether to use always the python logging module or not.

        // AWS backend parameters
        size_t AWS_REQUEST_TIMEOUT() const { return std::stoi(_store.at("tuplex.aws.requestTimeout")); } // 600s?
        size_t AWS_CONNECT_TIMEOUT() const { return std::stoi(_store.at("tuplex.aws.connectTimeout")); } // 30s?
        size_t AWS_MAX_CONCURRENCY() const { return std::stoi(_store.at("tuplex.aws.maxConcurrency")); }
        size_t AWS_NUM_HTTP_THREADS() const { return std::stoi(_store.at("tuplex.aws.httpThreadCount")); }
        std::string AWS_REGION() const { return _store.at("tuplex.aws.region"); }
        size_t AWS_LAMBDA_MEMORY() const { return std::stoi(_store.at("tuplex.aws.lambdaMemory")); } // 1536MB
        size_t AWS_LAMBDA_TIMEOUT() const { return std::stoi(_store.at("tuplex.aws.lambdaTimeout"));  } // 5min?
        bool AWS_REQUESTER_PAY() const { return stringToBool(_store.at("tuplex.aws.requesterPay")); }

        // access parameters via their getter functions
        size_t RUNTIME_MEMORY() const;                        //! in bytes how much memory should be given to UDFs (soft limit)
        size_t RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE() const;     //! how much memory is in one block for the runtime memory
        size_t PARTITION_SIZE() const;                        //! Size of a partition in bytes, equals task size (i.e. how much
                                                        //! memory a thread is processing at a time)
        size_t EXECUTOR_MEMORY() const;                       //! how much memory to use for data computations

        size_t DRIVER_MEMORY() const; //! how much memory to use for the driver? I.e. place where results + parallelized data is stored

        size_t READ_BUFFER_SIZE() const;

        unsigned int EXECUTOR_COUNT() const;                //! how many threads to use for multithreaded execution

        bool   AUTO_UPCAST_NUMBERS() const;                   //! When giving the context a mixture of bools, ints, floats this
                                                        //! will control whether to automatically upcast to the highest
                                                        //! type or produce error tuples
        bool   UNDEFINED_BEHAVIOR_FOR_OPERATORS() const;      //! whether to allow in code generation for undefined behaviour (division by zero, out of index, ...) or not
                                                        //! well written code may perform faster when this is false

        URI    SCRATCH_DIR() const;                           //! where to save temporary files
        URI    LOG_DIR() const;                               //! where to save logs

        URI    RUNTIME_LIBRARY(bool checkWithPythonExtension=false) const;                       //! where the runtime library is stored

        std::vector<char> CSV_COMMENTS() const; //! characters used to identify comments in csv file
        std::vector<char> CSV_SEPARATORS() const; //! potential CSV separators to scan
        char CSV_QUOTECHAR() const; //! quote char used for csv
        size_t CSV_MAX_DETECTION_MEMORY() const; //! maximum bytes to use for CSV schema inference
        size_t CSV_MAX_DETECTION_ROWS() const; //! maximum number of rows to use for CSV schema inference

        double NORMALCASE_THRESHOLD() const; //! threshold for normalcase, between 0.0 and 1.0
        double OPTIONAL_THRESHOLD() const; //! threshold for detecting an optional field, between 0.0 and 1.0

        Backend BACKEND() const; //! which backend to use for pipeline execution

        NetworkSettings AWS_NETWORK_SETTINGS() const; //! retrieve Network settings for AWS

        // general network settings
        std::string NETWORK_CA_FILE() const;
        std::string NETWORK_CA_PATH() const;
        bool NETWORK_VERIFY_SSL() const;


        bool USE_WEBUI() const;
        std::string WEBUI_HOST() const;
        uint16_t WEBUI_PORT() const;
        std::string WEBUI_DATABASE_HOST() const;
        uint16_t WEBUI_DATABASE_PORT() const;

        size_t WEBUI_EXCEPTION_DISPLAY_LIMIT() const;

        size_t INPUT_SPLIT_SIZE() const; //! maximum size of an input file, before it is split. 0 means no splitting

        inline std::string AWS_SCRATCH_DIR() const {
            return get("tuplex.aws.scratchDir");
        }

        /*!
         * return options as JSON string (string,string keys)
         * @return JSON
         */
        std::string asJSON() const;

        /*!
         * saves current configuration object to yaml file
         * @param uri path where to store the data
         * @param overwrite whether to overwrite the file
         * @return true on success
         */
        bool toYAML(const URI& uri, bool overwrite=false);

        /*!
         *
         * @param uri locator where to find the configuration file. Missing keys/options are filled with default values.
         * @return ContextOptions object with info filled from YAML.
         */
        static ContextOptions fromYAML(const URI& uri);

        /*!
         * creates object with default options
         * @return
         */
        static ContextOptions defaults();

        /*!
         * retrieves context options from defaults, then TUPLEX_HOME, then local directory via yaml files
         * @return
         */
        static ContextOptions load(const std::string& filename="config.yaml");



        void set(const std::string& key, const std::string& value);

        /*!
         * prints out options nicely formatted
         * @return
         */
        std::string toString() const;

        /*!
         * check whether key is contained within options
         * @param key key to check for
         * @return true if key represents a valid option key
         */
        bool containsKey(const std::string& key) const;

        /*!
         * internal configuration key/value map
         * @return map with conf keys and conf values
         */
        std::map<std::string, std::string> store() const { return _store; }

        /*!
         * retrieves entry of key, if not there returns alt
         * @param key
         * @param alt
         * @return
         */
        std::string get(const std::string& key, const std::string& alt = "") const;
    };


}

#endif //TUPLEX_CONTEXTOPTIONS_H