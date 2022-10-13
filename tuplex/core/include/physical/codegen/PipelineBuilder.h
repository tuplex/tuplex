//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PIPELINEBUILDER_H
#define TUPLEX_PIPELINEBUILDER_H

#include <codegen/LLVMEnvironment.h>
#include <codegen/CodegenHelper.h>
#include <codegen/FlattenedTuple.h>
#include <UDF.h>
#include <ExceptionCodes.h>
#include <logical/LogicalOperatorType.h>
#include <logical/JoinOperator.h>

namespace tuplex {
    namespace codegen {
        // new class which creates pipeline
        // add here serializers handlers etc.
        class PipelineBuilder {
            // map / filter / mapColumn / filterColumn / flatMap / toMemory / toCSV / ...
        private:
            std::shared_ptr<LLVMEnvironment> _env;
            llvm::Function *_func;

            llvm::Value *_argUserData; // some user defined pointer
            llvm::Value *_argRowNumber; // row number

            python::Type _inputRowType;
            python::Type _lastSchemaType;

            FlattenedTuple _argInputRow; // i.e., the actual input row!
            FlattenedTuple _lastRowResult; // i.e. the row after the last trafo
            FlattenedTuple _lastRowInput; // i.e. the row before the last trafo

            // following vars are needed when adding resolvers, i.e. apply the functions according to the last operator
            llvm::Value *_lastTupleResultVar;
            python::Type _lastInputSchemaType;
            LogicalOperatorType _lastOperatorType; // indicate which type was last added (excl. resolve / ignore)
            int _lastOperatorColumnIndex; // important for mapColumn / withColumn

            std::vector<llvm::BasicBlock *> _exceptionBlocks;
            llvm::BasicBlock *_constructorBlock;
            llvm::BasicBlock *_destructorBlock;
            llvm::BasicBlock *_entryBlock;
            llvm::BasicBlock *_lastBlock; // variable initialization + all constructed objects for this task

            // helper functions
            std::vector<llvm::BasicBlock*> _leaveBlocks;

            /*!
             * returns the basic block to stop processing and leave the pipeline at the current state
             * @return
             */
            llvm::BasicBlock* leaveBlock() const {
                assert(!_leaveBlocks.empty());
                return _leaveBlocks.back();
            }

            int _loopLevel; // at which loop level things are (used to call endLoop)
            void beginForLoop(llvm::IRBuilder<>& builder, llvm::Value* numIterations);
            void endForLoop(llvm::IRBuilder<>& builder);
            std::unordered_map<std::string, llvm::Value*> _args;

            std::string _exceptionCallbackName; //! optional, indicates whether pipeline should call exception handler (or not). Often, this functionaliy is better placed a level up except for single row executors

            python::Type _intermediateType;
            inline llvm::Value* intermediateOutputPtr() {
                return _args.at("intermediate");
            }

            /*!
             * helper function to create internal function.
             * @param Name name of function
             * @param intermediateOutputType if != unknown, an extra argument will be added to write output to.
             */
            void createFunction(const std::string &Name, const python::Type& intermediateOutputType=python::Type::UNKNOWN);

            llvm::BasicBlock *createExceptionBlock(const std::string &name = "except");


            // helper functions to use variables via alloc/store in code
            std::map<std::string, llvm::Value *> _variables;

            void addVariable(llvm::IRBuilder<> &builder, const std::string name, llvm::Type *type,
                             llvm::Value *initialValue = nullptr);

            llvm::Value *getVariable(llvm::IRBuilder<> &builder, const std::string name);

            llvm::Value *getPointerToVariable(llvm::IRBuilder<> &builder, const std::string name);

            void assignToVariable(llvm::IRBuilder<> &builder, const std::string name, llvm::Value *newValue);

//            inline llvm::Value *
//            vec3_i64(llvm::IRBuilder<> &builder, llvm::Value *a0, llvm::Value *a1, llvm::Value *a2) {
//                using namespace llvm;
//                assert(a0->getType() == env().i64Type()
//                       && a1->getType() == env().i64Type()
//                       && a2->getType() == env().i64Type());
//
//                auto vtype = VectorType::get(env().i64Type(), 3);
//
//                // Note: there used to be an old merge instructions, but seems it disappeared -.-
//                // merge ....
//                // same goes for broadcast ...
//
//                // trick: use undef here for inplace construction of vector
//                llvm::Value *v = UndefValue::get(vtype);
//                v = builder.CreateInsertElement(v, a0, (uint64_t) 0ul);
//                v = builder.CreateInsertElement(v, a1, (uint64_t) 1ul);
//                v = builder.CreateInsertElement(v, a2, (uint64_t) 2ul);
//                return v;
//            }


            // helper functions when generating code (program more using functions...)
            void beginLoop();
            void endLoop();


            /*!
             * create key using if statement
             * @param key key, might have null vals etc.
             * @param persist if true, then a copy will be made using C-malloc (not rtmalloc!)
             * @return
             */
            SerializableValue makeKey(llvm::IRBuilder<>& builder, const SerializableValue& key, bool persist=true);

            /*!
             * return builder at current stage of pipeline building!
             */
            llvm::IRBuilder<> builder();


            void createInnerJoinBucketLoop(llvm::IRBuilder<>& builder,
                                           llvm::Value* num_rows_to_join,
                                           llvm::Value* bucketPtrVar,
                                           bool buildRight,
                                           python::Type buildBucketType,
                                           python::Type resultType,
                                           int probeKeyIndex);

            void createLeftJoinBucketLoop(llvm::IRBuilder<>& builder,
                                           llvm::Value* num_rows_to_join,
                                           llvm::Value* bucketPtrVar,
                                           bool buildRight,
                                           python::Type buildBucketType,
                                           python::Type resultType,
                                           int probeKeyIndex,
                                           llvm::Value* match_found);

            static llvm::StructType* resultStructType(llvm::LLVMContext& ctx);

            void assignWriteCallbackReturnValue(llvm::IRBuilder<> &builder, int64_t operatorID,
                                                llvm::CallInst *callbackECVal);
        protected:
            llvm::StructType* resultStructType() const {
                return resultStructType(_env->getContext());
            }
            inline void createRet(llvm::IRBuilder<>& builder, llvm::Value* ecCode, llvm::Value* opID, llvm::Value* numRows) {
                // cast to i32
                auto rc = builder.CreateZExtOrTrunc(ecCode, env().i32Type());
                auto id = builder.CreateZExtOrTrunc(opID, env().i32Type());
                auto nrows = builder.CreateZExtOrTrunc(numRows, env().i32Type());

                // store into ret!
                auto idx_rc = CreateStructGEP(builder, _args["result"], 0);
                auto idx_id = CreateStructGEP(builder, _args["result"], 1);
                auto idx_nrows = CreateStructGEP(builder, _args["result"], 2);

                builder.CreateStore(rc, idx_rc);
                builder.CreateStore(id, idx_id);
                builder.CreateStore(nrows, idx_nrows);
                builder.CreateRetVoid();
            }

            // if/else constructs for better code generation => similar to the idea in https://github.com/cmu-db/peloton/blob/1de89798f271804f8be38a71219a20e761a1b4b6/src/include/codegen/lang/if.h
            //If()
        public:
            PipelineBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                            const python::Type &rowType,
                            const python::Type& intermediateType=python::Type::UNKNOWN,
                            const std::string &funcName = "processRow") : _func(nullptr),
                                                                          _argUserData(nullptr), _argRowNumber(nullptr),
                                                                          _lastTupleResultVar(nullptr),
                                                                          _lastOperatorColumnIndex(-1),
                                                                          _constructorBlock(nullptr),
                                                                          _destructorBlock(nullptr),
                                                                          _entryBlock(nullptr),
                                                                          _lastBlock(nullptr), _env(env),
                                                                          _inputRowType(rowType),
                                                                          _lastSchemaType(rowType),
                                                                          _argInputRow(env.get()),
                                                                          _lastRowInput(env.get()),
                                                                          _lastRowResult(env.get()),
                                                                          _loopLevel(0),
                                                                          _intermediateType(intermediateType) {
                createFunction(funcName, intermediateType);
            }

            LLVMEnvironment &env() { return *_env; }

            llvm::Function *getFunction() const {
                // verify function & return
                std::string err;
                if (!verifyFunction(_func, &err)) {

                    // @TODO: use the better inspection capabilities...
                    const size_t max_length = 50000; // max 50k chars
                    auto irSample = _env->getIR();
                    if(irSample.length() > max_length)
                        irSample = irSample.substr(0, max_length);

                    auto add_err = _env->decodeFunctionParameterError(err);
                    if(!add_err.empty())
                        add_err = "\n" + add_err;

                    std::stringstream ss;
                    ss << "could not verify function " << std::string(_func->getName()) << ". Details: " << err
                       << add_err;
                    ss << "\n" << irSample;
                    Logger::instance().logger("codegen").error(ss.str());

                    throw std::runtime_error("failed to validate pipeline LLVM code");
                    return nullptr;
                }

                return _func;
            }

            /*!
             * calls callback with data serialized as CSV. Data is runtime allocated.
             * @param callbackName name of the callback where the row is sent to
             * @param operatorID operator to raise exceptions for when returnCallbackResult is true
             * @param returnCallbackResult add additional check for callback and return its code
             * @param null_value the value (can be empty string) to write as null value
             * @param newLineDelimited whether to delimit the CSV string with '\n' or not
             * @param delimiter delimiter to use when outputting CSV according to RFC standard
             * @param quotechar quotechar to use when outputting CSV according to RFC standard
             * @return function
             */
            llvm::Function *buildWithCSVRowWriter(const std::string &callbackName,
                                                  int64_t operatorID,
                                                  bool returnCallbackResult,
                                                  const std::string &null_value,
                                                  bool newLineDelimited = true,
                                                  char delimiter = ',',
                                                  char quotechar = '"');

            /*!
             * calls callback with data serialized in Tuplex's internal memory format. Data is runtime allocated.
              * @param callbackName name of the callback where the row is sent to
             * @param operatorID operator to raise exceptions for when returnCallbackResult is true
             * @param returnCallbackResult add additional check for callback and return its code
             * @return function
             */
            llvm::Function *buildWithTuplexWriter(const std::string &callbackName, int64_t operatorID, bool returnCallbackResult);


            /*!
             * this function basically instead of calling a row writer, aggregates internally. Then, lastly a call is issued after all the loops are exited.
             * @param callbackName
             * @param operatorID
             * @param udfAggregator
             * @param initialValue
             * @param allowUndefinedBehavior
             * @param sharedObjectPropagation
             * @return
             */
             //@TODO: implement this...
            llvm::Function *buildWithAggregateWriter(const std::string& callbackName,
                                                     int64_t operatorID,
                                                     const UDF &udfAggregator,
                                                     const Row& initialValue,
                                                     bool allowUndefinedBehavior,
                                                     bool sharedObjectPropagation);

            llvm::Function *build(); // returns the function to be called

            // @TODO: what about thread safety here? => i.e. force processing to be single threaded?
            //  combine multiple hashmaps?

            // TODO: have transformtasks have output data ptr
            // ==> helpful to store allocated hashmap & Co
            // ==> multiple threads may construct individual hashmaps, then these need to be combined together into one large hashmap!
            // ==> i.e. have merge hash maps step!

            /*!
             * output data to a hashmap. I.e. Transformtask will hold a single hashmap as output
             * @param callbackName callback where to pass the hashmap to (i.e. for saving internally)
             * @param colKeys vector that represents the indices of the columns to use as key; if it is empty,
             *        then the entire row is used as the key (in this case, bucketize must be false, because
             *        there are no fields that are not already included in the key)
             * @param hashtableWidth the width of the key of the underlying hashtable (see CodegenHelper::hashtableKeyWidth())
             * @param bucketize whether to save other columns in bucket. (Can be null for unique aggregate e.g.)
             * @param isAggregateByKey whether this is an aggregate by key output
             * @return function with hashmap writer
             */
            llvm::Function *buildWithHashmapWriter(const std::string &callbackName, const std::vector<size_t> &colKeys, const int hashtableWidth,
                                                   bool bucketize = true, bool isAggregateByKey = false);

            // callback should be including nullbucket + hashmap...


            // add operations
            /*!
             * adds a map operation to the task
             * @param operatorID ID of the operator to be associated with when UDF calls can exception
             * @param udf User Defined Function
             * @param allowUndefinedBehavior
             * @param sharedObjectPropagation
             * @return true when code generation a map call to the function succeeded
             */
            bool mapOperation(const int64_t operatorID, const UDF &udf, double normalCaseThreshold, bool allowUndefinedBehavior, bool sharedObjectPropagation);

            /*!
             * adds a filter operation to the task
             * @param operatorID ID of the operator to be associated with when UDF calls can exception
             * @param udf User Defined Function
             * @param allowUndefinedBehavior
             * @param sharedObjectPropagation
             * @return bool when generation of code succeeded for this function
             */
            bool filterOperation(const int64_t operatorID, const UDF &udf, double normalCaseThreshold, bool allowUndefinedBehavior, bool sharedObjectPropagation);

            /*!
             * maps a single column, taking as input the column at columnToMapIndex and overwrite it with the UDF's output
             * @param operatorID ID of the operator to be associated with when UDF calls can exception
             * @param columnToMapIndex index of the column that should be mapped
             * @param udf User Defined Function
             * @param allowUndefinedBehavior
             * @param sharedObjectPropagation
             * @return true when code generation a mapColumn call to the function succeeded
             */
            bool mapColumnOperation(const int64_t operatorID,
                                    int columnToMapIndex,
                                    const UDF &udf,
                                    double normalCaseThreshold,
                                    bool allowUndefinedBehavior,
                                    bool sharedObjectPropagation);

            /*!
             * adds or overwrites a column at columnToMapIndex based on the UDF's output for the full row as input
             * @param operatorID ID of the operator to be associated with when UDF calls can exception
             * @param columnToMapIndex index of the column that should be mapped
             * @param udf User Defined Function
             * @param allowUndefinedBehavior
             * @param sharedObjectPropagation
             * @return true when code generation a withColumn call to the function succeeded
             */
            bool withColumnOperation(const int64_t operatorID,
                                     int columnToMapIndex,
                                     const UDF &udf,
                                     double normalCaseThreshold,
                                     bool allowUndefinedBehavior,
                                     bool sharedObjectPropagation);

            /*!
             * adds a resolver to the last added operation
             * @param ec
             * @param operatorID
             * @param udf
             * @param allowUndefinedBehavior
             * @param sharedObjectPropagation
             * @return
             */
            bool
            addResolver(const ExceptionCode &ec, const int64_t operatorID, const UDF &udf, double normalCaseThreshold, bool allowUndefinedBehavior, bool sharedObjectPropagation);

            /*!
             * use this function to trigger fallback row processing of an operator that does not fit into the general case
             * @param ec the exception code for which this resolver produces rows
             * @param operatorID ID of the resolver
             * @return whether it worked or not.
             */
            bool addNonSchemaConformingResolver(const ExceptionCode& ec, const int64_t operatorID);

            /*!
             * this is a function to add generated code for an ignore operator to be used on the slow path.
             * This is necessary when e.g. a resolver throws an exception which should subsequently be ignored.
             * For all .ignore directly coming after a non-resolve operator, use the quicker check provided by setting
             * ignore codes in BlockBasedTaskBuilder or CellSourceTaskBuilder.
             * @param ec
             * @param operatorID
             * @return
             */
            bool addIgnore(const ExceptionCode& ec, const int64_t operatorID);

            /*!
             * adds calls to a callback (signature: codegen::exception_handler_f)
             * @param callbackName name of callback to be called
             */
            void addExceptionHandler(const std::string &callbackName) { _exceptionCallbackName = callbackName; }

            std::string exceptionHandler() const { return _exceptionCallbackName; }

            bool addHashJoinProbe(int64_t leftKeyIndex, const python::Type &leftRowType,
                                  int64_t rightKeyIndex, const python::Type &rightRowType,
                                  const JoinType &jt, bool buildRight,
                                  llvm::Value* hash_map, llvm::Value* null_bucket);

            /*!
             * add aggregate (using global variables)
             * @param operatorID
             * @param aggUDF the UDF to use together with the global aggregate
             * @param aggType the type of the final aggregate.
             * @param allowUndefinedBehavior
             * @param sharedObjectPropagation
             * @return true if it succeeded
             */
            bool addAggregate(const int64_t operatorID,
                              const UDF& aggUDF,
                              const python::Type& aggType,
                              double normalCaseThreshold,
                              bool allowUndefinedBehavior,
                              bool sharedObjectPropagation);

            /*!
             * casts result to bigger row type (necessary i.e. to transform partially primitive types to option types, bool to int, int to float etc.)
             * @param rowType
             * @return
             */
            bool addTypeUpgrade(const python::Type& rowType);



            struct PipelineResult {
                llvm::Value* resultCode;            // i32 code, 0 if no exceptions
                llvm::Value* exceptionOperatorID;   // i32 id, 0 if everything is ok
                llvm::Value* numProducedRows;       // i32 num rows

                PipelineResult() : resultCode(nullptr), exceptionOperatorID(nullptr), numProducedRows(nullptr)   {}
            };

            /*!
             * use this helper to call a function generated by this class
             * @param func
             * @param ft
             * @param userData
             * @param rowNumber
             * @param intermediate
             * @return return value of this function
             */
            static PipelineResult
            call(llvm::IRBuilder<> &builder, llvm::Function *func, const FlattenedTuple &ft, llvm::Value *userData,
                 llvm::Value *rowNumber, llvm::Value* intermediate=nullptr);


            python::Type inputRowType() const { return _inputRowType; }

            inline bool checkRowTypesCompatible(const python::Type& resultRowType, const python::Type& lastRowType, const std::string& op_name = "operator") {
                if(resultRowType == lastRowType)
                    return true;
               else {
                    if(python::Type::propagateToTupleType(resultRowType) == lastRowType)
                        return true;
                    throw std::runtime_error("result type " + resultRowType.desc() + " of " + op_name + " does not match type of previous operator " + lastRowType.desc());

                }
               return false;
            }

        };


        /*!
         * creates a function which takes (void)(*)(void* userData, uint8_t* inputBuffer, int64_t inputBufferSize, int64_t rowNumber) to call pipeline over single function.
         * @param pip pipeline (note: this should have a caller to memOut or so)
         * @param name name of the wrapper function
         * @return llvm Function
         */
        extern llvm::Function *createSingleProcessRowWrapper(PipelineBuilder &pip, const std::string &name);

        // TODO: there should be an option to exclude the weird String -> type parsing thing here
        // better: have these exceptions be serialized as "normal-case violation exceptions" and have the CSV bad input
        // exception be something like where a single string is presented and then a resolver has to apply something to get the correct
        // type out of it!
        // => the resolve will then get way faster...

        // i.e. change parse row generator to always parse the stuff and throw exception with data in more general case
        // TODO
    }
}


#endif //TUPLEX_PIPELINEBUILDER_H