#ifndef TUPLEX_EXPLAMBDA_MAIN
#define TUPLEX_EXPLAMBDA_MAIN

#include <iostream>
#include <sstream>
#include <Utils.h>

#include <Lambda.pb.h>

// lambda main function, i.e. get a json request and return a json object
//extern const char* handler_worker(const char *payload, int &r);

//extern void global_init();
extern void global_cleanup();
extern bool container_reused();
extern tuplex::uniqueid_t container_id();
extern void reset_executor_setup();

#endif  // TUPLEX_EXPLAMBDA_MAIN
