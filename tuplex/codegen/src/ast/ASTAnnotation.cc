//
// Created by leonhard on 22.08.22.
//
#include <ast/ASTAnnotation.h>
#include <TypeHelper.h>

namespace tuplex {
    bool Symbol::findFunctionTypeBasedOnParameterType(const python::Type& parameterType, python::Type& specializedFunctionType) {
        // optimized type?
        auto deopt_type = deoptimizedType(parameterType);
        if(deopt_type != parameterType) {
            // try first using optimized type
            if(!typeBasedOnParameterType(parameterType, specializedFunctionType))
                return typeBasedOnParameterType(deopt_type, specializedFunctionType);
            return true;
        } else {
            return typeBasedOnParameterType(parameterType, specializedFunctionType);
        }
    }
}