//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <iostream>
#include <Pipe.h>

#include <boost/version.hpp>
#define BOOST_PROCESS_VERSION 1
#if BOOST_VERSION < 108800
#include <boost/process.hpp>
namespace bp_process = boost::process;
#else
#include <boost/process/v1/child.hpp>
#include <boost/process/v1/io.hpp>
#include <boost/process/v1/pipe.hpp>
#include <boost/process/v1/search_path.hpp>
namespace bp_process = boost::process::v1;
#endif

#include <boost/algorithm/string.hpp>
#include <Logger.h>
#include <fstream>
#include <cstdlib>

int Pipe::pipe(const std::string& file_input, const std::string& tmpdir) {

    try {

        using namespace bp_process;

        ipstream pipe_stdout;
        ipstream pipe_stderr;

        assert(_command.length() > 0);

        // get the program name
        auto idx = _command.find(' ');
        if(std::string::npos == idx)
            idx = _command.length();
        std::string name = _command.substr(0, idx);
        std::string tail = _command.substr(idx);

        std::string cmd = boost::process::search_path(name).generic_string() + tail;

        // check if file input is active, if so create a temp file
        if(file_input.length() > 0) {
            // @TODO: global temp dir should be used...
            // needs to be a configurable option...

            char* tmpname = new char[tmpdir.size() + 13];
            snprintf(tmpname, tmpdir.size() + 13, "%s/pipe-XXXXXX", tmpdir.c_str());
            int fd = mkstemp(tmpname);
            if (fd < 0) {
                Logger::instance().logger("pipe").error(std::string("error while creating temporary file"));
                _retval = 1;
                return retval();
            }

            std::FILE* tmp = fdopen(fd, "w");
            if (!tmp) {
                Logger::instance().logger("pipe").error(std::string("error opening temporary file"));
                _retval = 1;
                return retval();
            }

            fwrite(file_input.c_str(), file_input.size(), 1, tmp);
            fclose(tmp);
            cmd += " " + std::string(tmpname);
        }

        child c(cmd, std_err > pipe_stderr, std_out > pipe_stdout);

        std::string line;
        while (pipe_stdout && std::getline(pipe_stdout, line) && !line.empty()) {
            _stdout += line + "\n";
        }
        while (pipe_stderr && std::getline(pipe_stderr, line) && !line.empty()) {
            _stderr += line + "\n";
        }

        c.wait();
        _retval = c.exit_code();

    } catch(std::exception& e) {
        Logger::instance().logger("pipe").error(std::string("error while calling external process: ") + e.what());
        _retval = 1;
    }


    _executed = true;
    return retval();
}
