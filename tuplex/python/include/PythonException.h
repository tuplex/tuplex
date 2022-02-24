//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONEXCEPTION_H
#define TUPLEX_PYTHONEXCEPTION_H

#include "PythonCommon.h"

#include <cassert>
#include <iostream>

// inspired from https://stackoverflow.com/questions/2261858/boostpython-export-custom-exception

namespace tuplex {

    /*!
     * class to propagate internal exceptions to python frontend
     */
    class PythonException : public std::exception {
    private:
        std::string _message;
        std::string _data;

    public:

        PythonException(const std::string& message,
                        const std::string& data = "") : _message(message), _data(data) {
        }

        const char *what() const throw() {
            return _message.c_str();
        }

        ~PythonException() throw() {}

        std::string message() const { return _message; }
        std::string data() const { return _data; }
    };

    extern void translateCCException(const PythonException& e);
}

#endif //TUPLEX_PYTHONEXCEPTION_H