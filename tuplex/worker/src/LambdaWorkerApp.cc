//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 12/2/2021                                                                //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#ifdef BUILD_WITH_AWS

#include <LambdaWorkerApp.h>

// AWS specific includes
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/platform/Environment.h>

namespace tuplex {

    // Lambda specific configuration
    const std::string LambdaWorkerApp::caFile = "/etc/pki/tls/certs/ca-bundle.crt";
    const std::string LambdaWorkerApp::tuplexRuntimePath = "lib/tuplex_runtime.so";
    const bool LambdaWorkerApp::verifySSL = true;


    int LambdaWorkerApp::globalInit() {
        // Lambda specific initialization
        auto& logger = Logger::instance().defaultLogger();

        Timer timer;
        Aws::InitAPI(_aws_options);

        // get AWS credentials from Lambda environment...
        // Note that to run on Lambda this requires a session token!
        // e.g., https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
        std::string access_key = Aws::Environment::GetEnv("AWS_ACCESS_KEY_ID").c_str();
        std::string secret_key = Aws::Environment::GetEnv("AWS_SECRET_ACCESS_KEY").c_str();
        std::string session_token = Aws::Environment::GetEnv("AWS_SESSION_TOKEN").c_str();
        // get region from AWS_REGION env
        auto region = Aws::Environment::GetEnv("AWS_REGION");

        NetworkSettings ns;
        ns.verifySSL = verifySSL;
        ns.caFile = caFile;

        VirtualFileSystem::addS3FileSystem(access_key, secret_key, session_token, region.c_str(), ns,
                                           true, true);

        runtime::init(tuplexRuntimePath);
        _compiler = std::make_shared<JITCompiler>();

        // init python & set explicitly python home for Lambda
        std::string task_root = std::getenv("LAMBDA_TASK_ROOT");
        python::python_home_setup(task_root);
        logger.debug("Set PYTHONHOME=" + task_root);
        python::initInterpreter();
        metrics.global_init_time = timer.time();

        return WORKER_OK;
    }


    int LambdaWorkerApp::processMessage(const tuplex::messages::InvocationRequest& req) {

        // validate only S3 uris are given (in debug mode)
#ifdef NDEBUG
        bool invalid_uri_found = false;
        for(const auto& str_path : req.inputuris()) {
	    URI path(str_path);
            // check paths are S3 paths
            if(path.prefix() != "s3://") {
                logger().error("InvalidPath: input path must be s3:// path, is " + path.toPath());
                invalid_uri_found = true;
            }
        }
        if(invalid_uri_found)
            return WORKER_ERROR_INVALID_URI;
#endif

        // @TODO
        // can reuse here infrastructure from WorkerApp!

        // TODO notes for Lambda:
        // 1. scale-out should work (via self-invocation!)
        // 2. Joins (i.e. allow flight query to work)
        // 3. self-specialization (for flights should work) --> requires range optimization + detection on files.
        // ==> need other optimizations as well -.-

        return WORKER_OK;
    }
}

#endif
