//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IEXCEPTIONABLETASKGENERATOR_H
#define TUPLEX_IEXCEPTIONABLETASKGENERATOR_H

#include <LLVMEnvironment.h>
#include <TypeSystem.h>
#include <CodegenHelper.h>
#include <ExceptionCodes.h>
#include <FlattenedTuple.h>

namespace tuplex {

    namespace codegen {
        //! abstract class that helps with code generation for a task to iterate over a couple things
        class IExceptionableTaskGenerator {
        protected:
            // LLVM variables/internals for code generation
            std::shared_ptr<LLVMEnvironment> _env;


            //! returns builder for where custom code can be generated/inserted
            llvm::IRBuilder<> getBuilder() {

                assert(_lastBlock);
                return llvm::IRBuilder<>(_lastBlock);
            }

            llvm::BasicBlock* lastBlock() {
                assert(_lastBlock);
                return _lastBlock;
            }

            void setLastBlock(llvm::BasicBlock* lastBlock) {
                assert(lastBlock);
                _lastBlock = lastBlock;
            }

            llvm::BasicBlock* taskSuccessBlock() const { assert(_taskSuccessBlock); return _taskSuccessBlock; }
            inline llvm::BasicBlock* taskFailureBlock() {
                // lazy init task failure block
                if(!_taskFailureBlock)
                    createTaskFailureBlock();
                return _taskFailureBlock;
            }

            llvm::Function* getFunction() const { return _func; }

            llvm::Value* getUserDataArg() const { return _parameters.at("userData"); }
            llvm::Value* getInputPtrArg() const { return _parameters.at("input_ptr"); }
            llvm::Value* getInputSizeArg() const { return _parameters.at("input_size"); }


            inline void incRowNumber(llvm::IRBuilder<>& builder) {
                auto currentValue = getVariable(builder, "row");
                assignToVariable(builder, "row", builder.CreateAdd(currentValue, _env->i64Const(1)));
            }
            llvm::Value* getRowNumber(llvm::IRBuilder<>& builder) { return getVariable(builder, "row"); }
        public:

            // (1) typedefs
            // for building customizable/hackable transform stages with code generated task code (One task = iterating over one partition)
            // several functions are needed. Following are some typedefs to allow for easier handling

            //! type of the generated function
            //! takes a pointer to userdata which is passed to other functions and a pointer to a memory location
            //! as well as an additional argument specifying the input size
            //! where the data is stored to be processed. First 8 bytes give the number of rows stored at the location
            //! returns number of bytes written to all output partitions in sum
            typedef int64_t(*executor_f)(void*, uint8_t*, int64_t);

            //! type of the function to request a new output partition
            //! takes pointer to userdata, a minimum requested memory from the function at this point and
            //! a pointer where the function needs to write how many bytes it actually allocated
            //! returns pointer to memory location where task function can write data to. MUST HAVE capacity
            //! as returned via 3rd parameter.
            typedef uint8_t*(*reqMemory_f)(void*, int64_t, int64_t*);


            //! type of exception handler function, handler is called whenever exception occurs in UDF code
            //! parameters are as follows (read top-bottom like left-right)
            //! void*       userData                pointer to some userData passed down via the task function,
            //!                                     need to give her input partition etc. for error handling
            //! int64_t     exceptionCode           exceptionCode (specified via enum)
            //! int64_t     exceptionOperatorID     the ID of the (logical) operator to which the UDF belongs
            //! int64_t     rowNumber               within Task/Partition, starts at 0
            //! uint8_t*    input                   pointer to memory location of data that caused exception
            //! int64_t     dataLength              how many bytes the input data is long, can be used
            //!                                     to easily copy out memory region
            typedef void(*exceptionHandler_f)(void*, int64_t, int64_t, int64_t, uint8_t*, int64_t);


            IExceptionableTaskGenerator(const std::shared_ptr<LLVMEnvironment>& env) : _handler(nullptr) {
                _taskFailureBlock = nullptr;
                _taskSuccessBlock = nullptr;
                _exceptionBlock = nullptr;
                _entryBlock = nullptr;
                _lastBlock = nullptr;

                if(!env)
                    _env = std::make_unique<LLVMEnvironment>();
                else
                    _env = env;
            }

            virtual ~IExceptionableTaskGenerator() {
            }

            /*!
             * creates new task
             * @param name name of task functin to be called
             * @param handler exception handler
             * @return whether init code for task could be produced successfully
             */
            bool createTask(const std::string& name, exceptionHandler_f handler=nullptr);


            /*!
             * adds serialization to memory code.
             * @param requestOutputMemory a function that takes void* --> user data, int64_t minSize (minimum size required to serialize a row), int64_t* capacity --> how much capacity the function yielded
             * return pointer to memory region with *capacity bytes capacity. !!! If function returns null (in case of memory error), the task will be stopped. Further, MAKE SURE
             * THE MEMORY RETURNED has a capacity >= minRequested. NO CHECK FOR THIS IN GENERATED CODE for speed reasons.
             */
            bool toMemory(reqMemory_f requestOutputMemory, const FlattenedTuple& ft);

            /*!
             * generate missing links between blocks
             */
            void linkBlocks();

            llvm::Module* getModule() { return _env->getModule().get(); }
            LLVMEnvironment* getEnv() const { return _env.get(); }
        private:
            std::string _name;
            exceptionHandler_f _handler;

            llvm::Function* _func;

            // blocks for handling
            llvm::BasicBlock* _entryBlock;
            llvm::BasicBlock* _exceptionBlock;
            llvm::BasicBlock* _taskSuccessBlock; // success block (returns bytes written)
            llvm::BasicBlock* _taskFailureBlock; // returns 0

            llvm::BasicBlock* _lastBlock;

            // create exception block
            void createExceptionBlock();
            void createTaskFailureBlock();

            // special values holding different variables (all alloc variables to be later propagated to registers using mem2reg)
            std::map<std::string, llvm::Value*> _parameters;

            // helper functions to use variables via alloc/store in code
            std::map<std::string, llvm::Value*> _variables;
            void addVariable(llvm::IRBuilder<>& builder, const std::string name, llvm::Type* type, llvm::Value* initialValue=nullptr);
            llvm::Value* getVariable(llvm::IRBuilder<>& builder, const std::string name);
            llvm::Value* getPointerToVariable(llvm::IRBuilder<>& builder, const std::string name);
            void assignToVariable(llvm::IRBuilder<>& builder, const std::string name, llvm::Value *newValue);

            // @ Todo: refactor by introducing overloadable variable class for easier code generation

            inline llvm::Value* i8nullptr() {
                return llvm::ConstantPointerNull::get(llvm::Type::getInt8PtrTy(_env->getContext(), 0));
            }
            inline llvm::Value* i64nullptr() {
                return llvm::ConstantPointerNull::get(llvm::Type::getInt64PtrTy(_env->getContext(), 0));
            }
        };
    }
}
#endif //TUPLEX_IEXCEPTIONABLETASKGENERATOR_H