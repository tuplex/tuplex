//
// Created by leonhard on 10/3/22.
//

#include <physical/JsonSourceTaskBuilder.h>

namespace tuplex {
    namespace codegen {
        JsonSourceTaskBuilder::JsonSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment>& env,
                                                     const python::Type& rowType, const std::string& name) : BlockBasedTaskBuilder(env, rowType, name), _functionName(name) {

        }

        llvm::Function* JsonSourceTaskBuilder::build(bool terminateEarlyOnFailureCode) {
            using namespace llvm;

            assert(_env);
            auto& ctx = _env->getContext();
            // build a basic block based function (compatible with function signature read_block_f in CodeDefs.h
            auto arg_types = std::vector<llvm::Type*>({ctypeToLLVM<void*>(ctx),
                                                       ctypeToLLVM<uint8_t*>(ctx),
                                                       ctypeToLLVM<int64_t>(ctx),
                                                       ctypeToLLVM<int64_t*>(ctx),
                                                       ctypeToLLVM<int64_t*>(ctx),
                                                       ctypeToLLVM<int8_t>(ctx)});
            FunctionType* FT = FunctionType::get(ctypeToLLVM<int64_t>(ctx), arg_types, false);
            auto func = getOrInsertFunction(_env->getModule().get(), _functionName, FT);

            auto args = mapLLVMFunctionArgs(func, {"userData", "inPtr", "inSize", "outNormalRowCount", "outBadRowCount", "ignoreLastRow"});

            BasicBlock* bbEntry = BasicBlock::Create(env().getContext(), "entry", func);
            IRBuilder<> builder(bbEntry);

            builder.CreateRet(env().i64Const(0));

            return func;
        }
    }
}