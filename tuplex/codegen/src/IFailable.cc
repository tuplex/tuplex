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