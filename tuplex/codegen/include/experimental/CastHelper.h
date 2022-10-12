//
// Created by leonhard on 10/11/22.
//

#ifndef TUPLEX_CASTHELPER_H
#define TUPLEX_CASTHELPER_H

#include "ListHelper.h"
#include "StructDictHelper.h"
#include "../codegen/FlattenedTuple.h"

namespace tuplex {
    namespace codegen {
        SerializableValue upcast_value(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const SerializableValue& from, const python::Type& from_type, const python::Type& to_type);


        // if(from_type != to_type) {
        //                    if(python::Type::NULLVALUE == from_type) {
        //                        assert(paramsNew[i].isOptionType());
        //                        // nothing todo, values are good
        //                        val = nullptr;
        //                        size = nullptr;
        //                    } else if(!paramsOld[i].isOptionType() && paramsNew[i].isOptionType()) {
        //                        is_null = env->i1Const(false); // not null
        //                    } else if(paramsOld[i].isOptionType() && !p) {
        //                        // nothing todo...
        //                    }
        //                }

    }
}
#endif //TUPLEX_CASTHELPER_H
