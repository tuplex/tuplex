//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_MAIN_H
#define TUPLEX_MAIN_H

#include <aws/lambda-runtime/runtime.h>

#include <iostream>
#include <sstream>
#include <chrono>
#include <nlohmann/json.hpp>
#include <Utils.h>

#include <Lambda.pb.h>
#include <Python.h>

#include <LambdaWorkerApp.h>

// lambda main function, i.e. get a json request and return a json object
extern tuplex::messages::InvocationResponse lambda_main(aws::lambda_runtime::invocation_request const& lambda_req);
extern tuplex::messages::InvocationResponse make_exception(const std::string& message);


// helper function to get App
extern void init_app();
extern std::shared_ptr<tuplex::LambdaWorkerApp> get_app();

extern void global_init();
extern void global_cleanup();
extern bool container_reused();
extern tuplex::uniqueid_t container_id();
extern void reset_executor_setup();

extern uint64_t g_start_timestamp;
extern uint32_t g_num_requests_served;

#endif //TUPLEX_MAIN_H