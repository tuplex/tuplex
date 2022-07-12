//
// Created by Leonhard Spiegelberg on 2019-06-23.
//

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

/* This structure mirrors the one found in /usr/include/asm/ucontext.h */
typedef struct _sig_ucontext {
    unsigned long     uc_flags;
    struct ucontext   *uc_link;
    stack_t           uc_stack;
    struct sigcontext uc_mcontext;
    sigset_t          uc_sigmask;
} sig_ucontext_t;


// global sigbuf variable
extern jmp_buf sig_buf;

// this only works under gcc
// handler to print stack trace out
extern std::string format_stacktrace(int sig_num, siginfo_t * info, void * ucontext);

extern std::string getLastStackTrace();
extern void sigsev_handler(int signum, siginfo_t* info, void* ucontext);


#endif //TUPLEX_SIGHANDLER_H
