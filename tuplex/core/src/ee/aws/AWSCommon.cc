//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifdef BUILD_WITH_AWS

#include <ee/aws/AWSCommon.h>
#include <aws/core/Aws.h>
#include <VirtualFileSystem.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>

#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/FormattedLogSystem.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>

static std::string throw_if_missing_envvar(const std::string &name) {
    auto value = getenv(name.c_str());
    if(!value)
        throw std::runtime_error("missing environment variable: " + name);

    return value;
}

static bool isAWSInitialized = false;

// for Lambda, check: https://docs.aws.amazon.com/code-samples/latest/catalog/cpp-lambda-lambda_example.cpp.html

// https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_utils_1_1_logging_1_1_formatted_log_system.html
class SPDLogConnector : public Aws::Utils::Logging::FormattedLogSystem {
public:

private:
};

static bool initAWSSDK() {
    if(!isAWSInitialized) {
        Aws::SDKOptions options;

//        // hookup to Tuplex logger...
//        // --> https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/logging.html
//        options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;

        // @TODO: add tuplex loggers
        // => https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_utils_1_1_logging_1_1_log_system_interface.html

        // note: AWSSDk uses curl by default, can disable curl init here via https://sdk.amazonaws.com/cpp/api/LATEST/struct_aws_1_1_http_options.html
        Aws::InitAPI(options);

        // init logging
//        Aws::Utils::Logging::InitializeAWSLogging(
//                Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
//                    "tuplex",
//                    Aws::Utils::Logging::LogLevel::Trace,
//                    "aws sdk"));
                Aws::Utils::Logging::InitializeAWSLogging(
                Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>(
                    "tuplex",
                    Aws::Utils::Logging::LogLevel::Trace));
                
        isAWSInitialized = true;
    }
    return isAWSInitialized;
}

namespace tuplex {

    AWSCredentials AWSCredentials::get() {

        // lazy init AWS SDK
        initAWSSDK();

        AWSCredentials credentials;
        // use amazon's default chain
        auto provider = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("tuplex");
        auto aws_cred = provider->GetAWSCredentials();

        credentials.access_key = aws_cred.GetAWSAccessKeyId().c_str();
        credentials.secret_key = aws_cred.GetAWSSecretKey().c_str();

        return credentials;
    }

    // @TODO: add ca configuration options etc. => maybe network settings?
    bool initAWS(const AWSCredentials& credentials, bool requesterPay) {
        initAWSSDK();

        if(credentials.secret_key.empty() || credentials.access_key.empty())
           return false;

        // add S3 file system
        VirtualFileSystem::addS3FileSystem(credentials.access_key, credentials.secret_key, "", false, requesterPay);
        return true;
    }
}

#endif