#ifndef TUPLEX_EXPLAMBDA_MAIN
#define TUPLEX_EXPLAMBDA_MAIN

#include <aws/lambda-runtime/runtime.h>

#include <iostream>
#include <sstream>
#include <Utils.h>

#include <Lambda.pb.h>

// lambda main function, i.e. get a json request and return a json object
extern tuplex::messages::InvocationResponse lambda_main(
    aws::lambda_runtime::invocation_request const& lambda_req);
extern tuplex::messages::InvocationResponse make_exception(const std::string& message);

extern void global_init();
extern void global_cleanup();
extern bool container_reused();
extern tuplex::uniqueid_t container_id();
extern void reset_executor_setup();

#endif  // TUPLEX_EXPLAMBDA_MAIN
