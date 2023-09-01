//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_SIGHANDLER_H
#define TUPLEX_SIGHANDLER_H


// SIGSEV handler for bugs
#include <cstdio>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <csetjmp>

// from https://stackoverflow.com/questions/77005/how-to-automatically-generate-a-stacktrace-when-my-program-crashes
// and https://github.com/JPNaude/dev_notes/wiki/Produce-a-stacktrace-when-something-goes-wrong-in-your-application
#include <cxxabi.h>
#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ucontext.h>
#include <unistd.h>
#include <fstream>

// old
///* This structure mirrors the one found in /usr/include/asm/ucontext.h */
//typedef struct _sig_ucontext {
//    unsigned long     uc_flags;
//    struct ucontext   *uc_link;
//    stack_t           uc_stack;
//    struct sigcontext uc_mcontext;
//    sigset_t          uc_sigmask;
//} sig_ucontext_t;
//
//
// global sigbuf variable
extern jmp_buf sig_buf;

//// this only works under gcc
//// handler to print stack trace out
//extern std::string format_stacktrace(int sig_num, siginfo_t * info, void * ucontext);
//
extern std::string getLastStackTrace();
extern void store_trace(const std::string& trace);
//extern void sigsev_handler(int signum, siginfo_t* info, void* ucontext);

#include "backward.h"

// use google's backward class, setup here in order to save traces...
namespace tuplex {

    class SignalHandling {
    public:
        static std::vector<int> make_default_signals() {
            const int posix_signals[] = {
                    // Signals for which the default action is "Core".
                    SIGABRT, // Abort signal from abort(3)
                    SIGBUS,  // Bus error (bad memory access)
                    SIGFPE,  // Floating point exception
                    SIGILL,  // Illegal Instruction
                    SIGIOT,  // IOT trap. A synonym for SIGABRT
                    SIGQUIT, // Quit from keyboard
                    SIGSEGV, // Invalid memory reference
                    SIGSYS,  // Bad argument to routine (SVr4)
                    SIGTRAP, // Trace/breakpoint trap
                    SIGXCPU, // CPU time limit exceeded (4.2BSD)
                    SIGXFSZ, // File size limit exceeded (4.2BSD)
#if defined(BACKWARD_SYSTEM_DARWIN)
                    SIGEMT, // emulation instruction executed
#endif
            };
            return std::vector<int>(posix_signals,
                                    posix_signals +
                                    sizeof posix_signals / sizeof posix_signals[0]);
        }

        SignalHandling(const std::vector<int> &posix_signals = make_default_signals())
                : _loaded(false) {
            bool success = true;

            const size_t stack_size = 1024 * 1024 * 8;
            _stack_content.reset(static_cast<char *>(malloc(stack_size)));
            if (_stack_content) {
                stack_t ss;
                ss.ss_sp = _stack_content.get();
                ss.ss_size = stack_size;
                ss.ss_flags = 0;
                if (sigaltstack(&ss, nullptr) < 0) {
                    success = false;
                }
            } else {
                success = false;
            }

            for (size_t i = 0; i < posix_signals.size(); ++i) {
                struct sigaction action;
                memset(&action, 0, sizeof action);
                action.sa_flags =
                        static_cast<int>(SA_SIGINFO | SA_ONSTACK | SA_NODEFER | SA_RESETHAND);
                sigfillset(&action.sa_mask);
                sigdelset(&action.sa_mask, posix_signals[i]);
#if defined(__clang__)
                #pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
#endif
                action.sa_sigaction = &sig_handler;
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

                int r = sigaction(posix_signals[i], &action, nullptr);
                if (r < 0)
                    success = false;
            }

            _loaded = success;
        }

        bool loaded() const { return _loaded; }

        static void handleSignal(int signum, siginfo_t *info, void *_ctx) {
            ucontext_t *uctx = static_cast<ucontext_t *>(_ctx);

            backward::StackTrace st;
            void *error_addr = nullptr;
#ifdef REG_RIP // x86_64
            error_addr = reinterpret_cast<void *>(uctx->uc_mcontext.gregs[REG_RIP]);
#elif defined(REG_EIP) // x86_32
            error_addr = reinterpret_cast<void *>(uctx->uc_mcontext.gregs[REG_EIP]);
#elif defined(__arm__)
    error_addr = reinterpret_cast<void *>(uctx->uc_mcontext.arm_pc);
#elif defined(__aarch64__)
    #if defined(__APPLE__)
      error_addr = reinterpret_cast<void *>(uctx->uc_mcontext->__ss.__pc);
    #else
      error_addr = reinterpret_cast<void *>(uctx->uc_mcontext.pc);
    #endif
#elif defined(__mips__)
    error_addr = reinterpret_cast<void *>(
        reinterpret_cast<struct sigcontext *>(&uctx->uc_mcontext)->sc_pc);
#elif defined(__ppc__) || defined(__powerpc) || defined(__powerpc__) ||        \
    defined(__POWERPC__)
    error_addr = reinterpret_cast<void *>(uctx->uc_mcontext.regs->nip);
#elif defined(__riscv)
    error_addr = reinterpret_cast<void *>(uctx->uc_mcontext.__gregs[REG_PC]);
#elif defined(__s390x__)
    error_addr = reinterpret_cast<void *>(uctx->uc_mcontext.psw.addr);
#elif defined(__APPLE__) && defined(__x86_64__)
    error_addr = reinterpret_cast<void *>(uctx->uc_mcontext->__ss.__rip);
#elif defined(__APPLE__)
    error_addr = reinterpret_cast<void *>(uctx->uc_mcontext->__ss.__eip);
#else
#warning ":/ sorry, ain't know no nothing none not of your architecture!"
#endif
            if (error_addr) {
                st.load_from(error_addr, 32, reinterpret_cast<void *>(uctx),
                             info->si_addr);
            } else {
                st.load_here(32, reinterpret_cast<void *>(uctx), info->si_addr);
            }

            backward::Printer printer;
            printer.address = true;
            std::stringstream ss_st;
            // print to buf
            printer.print(st, ss_st);
            // print to stderr as well
            printer.print(st, stderr);

            // store as last trace
            store_trace(ss_st.str());

#if (defined(_XOPEN_SOURCE) && _XOPEN_SOURCE >= 700) || \
    (defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200809L)
            psiginfo(info, nullptr);
#else
            (void)info;
#endif

            // longjmp to ending Lambda after an exception
            longjmp(sig_buf, signum);
        }

    private:
        backward::details::handle<char *> _stack_content;
        bool _loaded;

#ifdef __GNUC__
        __attribute__((noreturn))
#endif
        static void
        sig_handler(int signo, siginfo_t *info, void *_ctx) {
            handleSignal(signo, info, _ctx);

            // try to forward the signal.
            raise(info->si_signo);

            // terminate the process immediately.
            puts("watf? exit");
            _exit(EXIT_FAILURE);
        }
    };
}
// new: use google's backward lib


#endif //TUPLEX_SIGHANDLER_H