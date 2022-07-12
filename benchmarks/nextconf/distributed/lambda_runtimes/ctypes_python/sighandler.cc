//
// Created by Leonhard Spiegelberg on 2019-06-23.
//

#include <iostream>
#include <sstream>
#include "sighandler.h"

//#include <boost/stacktrace.hpp>

// global jmp_buf variable
jmp_buf sig_buf;
static std::string lastTrace = "";

std::string getLastStackTrace() { return lastTrace; }

void sigsev_handler(int signum, siginfo_t* info, void* ucontext) {
    std::cout<<"process "<<getpid()<<" got signal "<<signum<<std::endl;

    // create stack dump
    auto err = format_stacktrace(signum, info, ucontext);
    lastTrace = err;

    std::cerr<<err<<std::endl;

    // longjmp to ending Lambda after an exception
    longjmp(sig_buf, signum);
}


// in order to display symbols for own program, link using https://github.com/ldc-developers/ldc/issues/863

// this only works under gcc
// handler to print stack trace out
std::string format_stacktrace(int sig_num, siginfo_t * info, void * ucontext) {
    std::stringstream ss;
    sig_ucontext_t * uc = (sig_ucontext_t *)ucontext;

    void * caller_address = (void *) uc->uc_mcontext.rip; // x86 specific

    ss << "signal " << sig_num
       << " (" << strsignal(sig_num) << "), address is "
       << info->si_addr << " from " << caller_address
       << std::endl << std::endl;

    void * array[50];
    int size = backtrace(array, 50);

    array[1] = caller_address;

    char ** messages = backtrace_symbols(array, size);

    // skip first stack frame (points here)
    for (int i = 1; i < size && messages != nullptr; ++i)
    {
        char *mangled_name = 0, *offset_begin = 0, *offset_end = 0;

        // find parentheses and +address offset surrounding mangled name
        for (char *p = messages[i]; *p; ++p)
        {
            if (*p == '(')
            {
                mangled_name = p;
            }
            else if (*p == '+')
            {
                offset_begin = p;
            }
            else if (*p == ')')
            {
                offset_end = p;
                break;
            }
        }

        // if the line could be processed, attempt to demangle the symbol
        if (mangled_name && offset_begin && offset_end &&
            mangled_name < offset_begin)
        {
            *mangled_name++ = '\0';
            *offset_begin++ = '\0';
            *offset_end++ = '\0';

            int status;
            char * real_name = abi::__cxa_demangle(mangled_name, 0, 0, &status);

            // if demangling is successful, output the demangled function name
            if (status == 0)
            {
                ss << "[bt]: (" << i << ") " << messages[i] << " : "
                          << real_name << "+" << offset_begin << offset_end
                          << std::endl;

            }
                // otherwise, output the mangled function name
            else
            {
                ss << "[bt]: (" << i << ") " << messages[i] << " : "
                          << mangled_name << "+" << offset_begin << offset_end
                          << std::endl;
            }
            free(real_name);
        }
            // otherwise, print the whole line
        else
        {
            ss << "[bt]: (" << i << ") " << messages[i] << std::endl;
        }
    }

    free(messages);
    return ss.str();
}