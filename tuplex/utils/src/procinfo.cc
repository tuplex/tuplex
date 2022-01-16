//
// Created by Leonhard Spiegelberg on 1/2/22.
//

#include <procinfo.h>
#include <string>

namespace tuplex {

#ifdef _WiN32
#include <process.h>
#endif
#include <unistd.h>
#include <cstddef>

    process_t pid_from_self() {
#ifdef _WIN32
        return _getpid();
#else
        return getpid();
#endif
    }

    process_t ppid_from_self() {
#ifdef _WIN32
        return ppid_from_pid(pid_from_self());
#else
        return getppid();
#endif
    }

    ::std::string dir_from_pid(process_t pid) {
        ::std::string fname = path_from_pid(pid);
        ::std::size_t fp = fname.find_last_of("/\\");
        return fname.substr(0, fp + 1);
    }

    ::std::string name_from_pid(process_t pid) {
        ::std::string fname = path_from_pid(pid);
        ::std::size_t fp = fname.find_last_of("/\\");
        return fname.substr(fp + 1);
    }

#ifdef __linux__
    #include <cstdlib>

    ::std::string path_from_pid(process_t pid) {
      ::std::string path;
      ::std::string link = ::std::string("/proc/") + ::std::to_string(pid) + ::std::string("/exe");
      char *buffer = realpath(link.c_str(), NULL);
      path = buffer ? : "";
      free(buffer);
      return path;
    }
#endif

#if defined(__APPLE__) && defined(__MACH__)
    #include <libproc.h>

    std::string path_from_pid(process_t pid) {
        std::string path;
        char buffer[PROC_PIDPATHINFO_MAXSIZE];
        if (proc_pidpath(pid, buffer, sizeof(buffer)) > 0) {
            path = std::string(buffer) + "\0";
        }
        return path;
    }
#endif
}
