//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PIPE_H
#define TUPLEX_PIPE_H

#include <sstream>
#include <iostream>
#include <cassert>

#include <Base.h>

class Pipe {
private:
    int _retval;
    bool _executed;
    std::string _command;
    std::string _stdout;
    std::string _stderr;

public:
    Pipe(const std::string& command) :_command(command), _retval(-1), _executed(false) {}

    /*!
     * run with a file as input
     * @param file_input
     * @return return value of the process
     */
    int pipe(const std::string& file_input = "", const std::string& tmpdir = "/tmp");

    /*!
     * return value of the command executed
     * @return 0 if command succeeded, else for failure
     */
    int retval()    { assert(_executed); return _retval; }

    std::string stdout() const { assert(_executed); return _stdout; }
    std::string stderr() const { assert(_executed); return _stderr; }
};

#endif //TUPLEX_PIPE_H
