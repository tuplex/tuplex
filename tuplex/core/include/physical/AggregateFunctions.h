//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_AGGREGATEFUNCTIONS_H
#define TUPLEX_AGGREGATEFUNCTIONS_H

#include <LLVMEnvironment.h>
#include <UDF.h>
#include <LambdaFunction.h>
#include <CompiledFunction.h>
#include <Row.h>

// contains code to create compiled functions to aggregate intermediates + initialize them

namespace tuplex {
    namespace codegen {

        /*!
         * creates a function int64_t initAggregate(void** agg, int64_t* agg_size) which allocates the aggregate, initializes and returns its size
         * @param env
         * @param name
         * @param initialValue
         * @param aggType
         * @param allocator
         * @return the corresponding llvm function.
         */
        extern llvm::Function* createAggregateInitFunction(LLVMEnvironment* env, const std::string& name,
                                                           const Row& initialValue,
                                                           const python::Type aggType=python::Type::UNKNOWN,
                                                           decltype(malloc) allocator=malloc);

        /*!
         * creates a function int64_t combineAggregates(void** out, int64_t* out_size, void* agg, int64_t agg_size) which aggregates agg into out (freeing and reallocating out/out_size if necessary)
         * @param env
         * @param name the name of the resulting function
         * @param udf the aggregation udf to be used in the returned functor
         * @param aggType the type of the aggregate value
         * @param allocator the memory allocation function to use
         * @param allowUndefinedBehavior boolean flag whether or not to allow undefined behavior (passed from user options, used to compile the udf)
         * @param sharedObjectPropagation boolean flag whether or not to propagate shared objects (passed from user options, used to compile the functor)
         * @return the corresponding llvm function.
         */
        extern llvm::Function *createAggregateCombineFunction(LLVMEnvironment *env,
                                                              const std::string &name,
                                                              const UDF &udf,
                                                              const python::Type aggType,
                                                              decltype(malloc) allocator=malloc,
                                                              bool allowUndefinedBehavior=true,
                                                              bool sharedObjectPropagation=true);

        /*!
         * creates a function int64_t aggregate(void** aggOut, void* row, int64_t* row_size) which aggregates in the row to the aggOut buffer (freeing and reallocating it if necessary)
         * the aggOut buffer has format size | data, where size is a uint64_t.
         * @param env
         * @param name the name of the resulting function
         * @param udf the aggregation udf to be used in the returned functor
         * @param aggType the type of the aggregate value
         * @param rowType the type of the incoming row
         * @param allocator the memory allocation function to use
         * @param allowUndefinedBehavior boolean flag whether or not to allow undefined behavior (passed from user options, used to compile the udf)
         * @param sharedObjectPropagation boolean flag whether or not to propagate shared objects (passed from user options, used to compile the functor)
         * @return the corresponding llvm function.
         */
        extern llvm::Function *createAggregateFunction(LLVMEnvironment *env,
                                                       const std::string &name,
                                                       const UDF &udf,
                                                       const python::Type &aggType,
                                                       const python::Type &rowType,
                                                       decltype(malloc) allocator = malloc,
                                                       bool allowUndefinedBehavior = true,
                                                       bool sharedObjectPropagation = true);
    }
}

#endif //TUPLEX_AGGREGATEFUNCTIONS_H