//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_MESSAGEHANDLER_H
#define TUPLEX_MESSAGEHANDLER_H

#include <spdlog/spdlog.h>

/*!
 * helper class to handle all kind of logging messages. Can be provided optionally to most interfaces.
 */
class MessageHandler {
private:
    std::string _name;
public:
    MessageHandler() : _name("global")  {}
    MessageHandler(const std::string& name) : _name(name)   {}
    MessageHandler(const MessageHandler& other) : _name(other._name) {}

    virtual ~MessageHandler() {}

    MessageHandler& setName(const std::string& name) { _name = name; return *this; }

    MessageHandler& error(const std::string& message);
    MessageHandler& warn(const std::string& message);
    MessageHandler& info(const std::string& message);
    MessageHandler& debug(const std::string& message);
};

#endif //TUPLEX_MESSAGEHANDLER_H