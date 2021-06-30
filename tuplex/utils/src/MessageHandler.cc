//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <MessageHandler.h>
#include <Logger.h>
#include <StringUtils.h>

MessageHandler& MessageHandler::info(const std::string &message) {
    auto msg = message;
    tuplex::trim(msg);
    Logger::instance().info(_name, msg);
    return *this;
}

MessageHandler& MessageHandler::warn(const std::string &message) {
    Logger::instance().warn(_name, message);
    return *this;
}
MessageHandler& MessageHandler::error(const std::string &message) {
    Logger::instance().error(_name, message);
    return *this;
}

MessageHandler& MessageHandler::debug(const std::string &message) {
#ifndef NDEBUG
    Logger::instance().debug(_name, message);
#endif
    return *this;
}