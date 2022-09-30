//
// Created by Leonhard Spiegelberg on 1/2/22.
//

#ifndef TUPLEX_PROCINFO_H
#define TUPLEX_PROCINFO_H

// from https://stackoverflow.com/questions/1528298/get-path-of-executable, helper functions to get process info
#ifdef _WiN32
#include <windows.h>
typedef DWORD process_t;
#else
#include <sys/types.h>
typedef pid_t process_t;
#endif
#include <string>

namespace tuplex {
    /*!
     * get current process id
     * @return process id
     */
    extern process_t pid_from_self();

    /*!
     * get parent process id
     * @return parent process id
     */
    extern process_t ppid_from_self();

    /*!
     * get path belonging to process id
     * @param pid process id
     * @return string containing absolute path to process
     */
    extern std::string path_from_pid(process_t pid);

    /*!
     * directory in which executable of process belonging to pid resides in
     * @param pid process id
     * @return string containing absolute directory
     */
    extern std::string dir_from_pid(process_t pid);

}
#endif //TUPLEX_PROCINFO_H
