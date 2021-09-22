//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <IFailable.h>
#include <algorithm>

void IFailable::error(const std::string &message, const std::string &logger) {
    _succeeded = false;
    // in silent mode collect message but do not log it out directly
    if(_silentMode) {
        _messages.push_back(std::make_tuple(message, logger));
    } else {
        if(logger.length() > 0)
            Logger::instance().logger(logger).error(message);
        else
            Logger::instance().defaultLogger().error(message);
    }
}

void IFailable::logMessages() {
    for(const auto& msg : _messages) {
        auto logger = std::get<1>(msg);
        auto message = std::get<0>(msg);
        if(logger.length() > 0)
            Logger::instance().logger(logger).error(message);
        else
            Logger::instance().defaultLogger().error(message);
    }
}

std::string IFailable::compileErrorToStr(const CompileError &err) {
    std::string errMsg;
    switch(err) {
        case CompileError::TYPE_ERROR_LIST_OF_LISTS:
            errMsg = "list of lists not yet supported in UDF";
            break;
        case CompileError::TYPE_ERROR_RETURN_LIST_OF_TUPLES:
            errMsg = "returning list of tuples not yet supported";
            break;
        case CompileError::TYPE_ERROR_RETURN_LIST_OF_DICTS:
            errMsg = "returning list of dictionaries not yet supported";
            break;
        case CompileError::TYPE_ERROR_RETURN_LIST_OF_LISTS:
            errMsg = "returning list of lists not yet supported";
            break;
        case CompileError::TYPE_ERROR_RETURN_LIST_OF_MULTITYPES:
            errMsg = "returning list of different types not yet supported";
            break;
        case CompileError::TYPE_ERROR_LIST_OF_MULTITYPES:
            errMsg = "lists only supported with a single element type";
            break;
        case CompileError::TYPE_ERROR_ITER_CALL_WITH_NONHOMOGENEOUS_TUPLE:
            errMsg = "generating iterator from non-homogeneous tuple not yet supported";
            break;
        case CompileError::TYPE_ERROR_ITER_CALL_WITH_DICTIONARY:
            errMsg = "generating iterator from dictionary not yet supported";
            break;
        case CompileError::TYPE_ERROR_RETURN_ITERATOR:
            errMsg = "returning iterator not yet supported";
            break;
        case CompileError::TYPE_ERROR_NEXT_CALL_DIFFERENT_DEFAULT_TYPE:
            errMsg = "next(iterator[, default]) not yet supported when default value type differing from iterator yield type";
            break;
        case CompileError::TYPE_ERROR_MIXED_ASTNODETYPE_IN_FOR_LOOP_EXPRLIST:
            errMsg = "mixed use of tuple/list of identifiers and single identifier in exprlist not yet supported";
            break;
        default:
            break;
    }
    return errMsg;
}

CompileError IFailable::getReturnError() {
    auto it = std::find_if(_compileErrors.begin(), _compileErrors.end(),
                           [](const CompileError &e){return e == CompileError::TYPE_ERROR_RETURN_LIST_OF_TUPLES || e == CompileError::TYPE_ERROR_RETURN_LIST_OF_LISTS || e == CompileError::TYPE_ERROR_RETURN_LIST_OF_DICTS || e == CompileError::TYPE_ERROR_RETURN_LIST_OF_MULTITYPES;});
    if(it != _compileErrors.end()) {
        return *it;
    }
    return CompileError::COMPILE_ERROR_NONE;
}