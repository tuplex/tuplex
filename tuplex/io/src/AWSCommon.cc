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

#include <AWSCommon.h>
#include <aws/core/Aws.h>
#include <VirtualFileSystem.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>

#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/utils/logging/FormattedLogSystem.h>
#include <aws/core/utils/logging/DefaultLogSystem.h>
#include <aws/core/utils/logging/AWSLogging.h>
#include <aws/core/platform/Environment.h>

#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/utils/json/JsonSerializer.h>


#include <Network.h>

#include <regex>
#include <algorithm>
#include <memory>

static std::string throw_if_missing_envvar(const std::string &name) {
    auto value = getenv(name.c_str());
    if(!value)
        throw std::runtime_error("missing environment variable: " + name);

    return value;
}

static bool isAWSInitialized = false;
static Aws::SDKOptions aws_options;

// for Lambda, check: https://docs.aws.amazon.com/code-samples/latest/catalog/cpp-lambda-lambda_example.cpp.html

// https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_utils_1_1_logging_1_1_formatted_log_system.html
class SPDLogConnector : public Aws::Utils::Logging::FormattedLogSystem {
public:
    SPDLogConnector(Aws::Utils::Logging::LogLevel logLevel) : Aws::Utils::Logging::FormattedLogSystem(logLevel) {}

protected:

    // probably need to overwrite: https://github.com/aws/aws-sdk-cpp/blob/main/aws-cpp-sdk-core/source/utils/logging/FormattedLogSystem.cpp

    void ProcessFormattedStatement(Aws::String&& statement) override {
        //
    }
private:

};

static bool initAWSSDK() {
    if(!isAWSInitialized) {
//        // hookup to Tuplex logger...
//        // --> https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/logging.html
//        options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Trace;

        // @TODO: add tuplex loggers
        // => https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_utils_1_1_logging_1_1_log_system_interface.html

        // note: AWSSDk uses curl by default, can disable curl init here via https://sdk.amazonaws.com/cpp/api/LATEST/struct_aws_1_1_http_options.html
        Aws::InitAPI(aws_options);

        // init logging
//        Aws::Utils::Logging::InitializeAWSLogging(
//                Aws::MakeShared<Aws::Utils::Logging::DefaultLogSystem>(
//                    "tuplex",
//                    Aws::Utils::Logging::LogLevel::Trace,
//                    "aws sdk"));
#ifndef NDEBUG
        auto log_level = Aws::Utils::Logging::LogLevel::Trace;
        log_level = Aws::Utils::Logging::LogLevel::Info;
        auto log_system = Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>("tuplex", log_level);
        Aws::Utils::Logging::InitializeAWSLogging(log_system);
#endif
        isAWSInitialized = true;
    }
    return isAWSInitialized;
}

namespace tuplex {

    static Aws::String get_default_region() {

        // check AWS_DEFAULT_REGION, then AWS_REGION
        // i.e., similar to https://aws.amazon.com/blogs/developer/aws-sdk-for-cpp-version-1-8/
        {
            auto region = Aws::Environment::GetEnv("AWS_DEFAULT_REGION");
            if(!region.empty())
                return region;
        }

        {
            auto region = Aws::Environment::GetEnv("AWS_REGION");
            if(!region.empty())
                return region;
        }

        // inspired by https://github.com/aws/aws-sdk-cpp/issues/1310
        auto profile_name = Aws::Auth::GetConfigProfileName();
        if(Aws::Config::HasCachedConfigProfile(profile_name)) {
            auto profile = Aws::Config::GetCachedConfigProfile(profile_name);
            auto region = profile.GetRegion();
            if(!region.empty())
                return region;
        }

        // check credentials profile
        if(Aws::Config::HasCachedCredentialsProfile(profile_name)) {
            auto profile = Aws::Config::GetCachedCredentialsProfile(profile_name);
            auto region = profile.GetRegion();
            if(!region.empty())
                return region;
        }
        return Aws::Region::US_EAST_1;
    }

    AWSCredentials AWSCredentials::get() {

        // lazy init AWS SDK
        initAWSSDK();

        AWSCredentials credentials;

        // AWS default chain issues a bunch of HTTP request, avoid to make Tuplex more responsive.
        auto env_provider =  Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>("tuplex");
        auto aws_cred = env_provider->GetAWSCredentials();

        // empty?
        if(aws_cred.IsEmpty()) {
            // try ~/.aws/credentials next
            auto conf_provider = Aws::MakeShared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>("tuplex");
            aws_cred = conf_provider->GetAWSCredentials();

            // default to most general chain...
            if(aws_cred.IsEmpty()) {
                // use amazon's default chain
                auto provider = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>("tuplex");
                aws_cred = provider->GetAWSCredentials();
            }
        }

        credentials.access_key = aws_cred.GetAWSAccessKeyId().c_str();
        credentials.secret_key = aws_cred.GetAWSSecretKey().c_str();
        credentials.session_token = aws_cred.GetSessionToken().c_str();

        // query default region (avoid also here the HTTP requests...)
        // => use us-east-1 per default else!
        credentials.default_region = get_default_region().c_str();

        return credentials;
    }

    // @TODO: add ca configuration options etc. => maybe network settings?
    bool initAWS(const AWSCredentials& credentials, const NetworkSettings& ns, bool requesterPay) {
        initAWSSDK();

        if(credentials.secret_key.empty() || credentials.access_key.empty())
           return false;

        // add S3 file system
        VirtualFileSystem::addS3FileSystem(credentials.access_key, credentials.secret_key,
                                           credentials.session_token, credentials.default_region,
                                           ns, false, requesterPay);
        return true;
    }

    void shutdownAWS() {

        // remove S3 File System
        VirtualFileSystem::removeS3FileSystem();
        Aws::ShutdownAPI(aws_options);
        isAWSInitialized = false;
    }

    bool isValidAWSZone(const std::string& zone) {
        // names from https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
        static std::set<std::string> valid_names{"us-east-2",
                                                 "us-east-1",
                                                 "us-west-1",
                                                 "us-west-2,",
                                                 "af-south-1",
                                                 "ap-east-1",
                                                 "ap-south-1",
                                                 "ap-northeast-3",
                                                 "ap-northeast-2",
                                                 "ap-southeast-1",
                                                 "ap-southeast-2",
                                                 "ap-northeast-1",
                                                 "ca-central-1",
                                                 "eu-central-1",
                                                 "eu-west-1",
                                                 "eu-west-2",
                                                 "eu-south-1",
                                                 "eu-west-3",
                                                 "eu-north-1",
                                                 "me-south-1",
                                                 "sa-east-1",
                                                 "us-gov-east-1",
                                                 "us-gov-west-1"};
        return std::find(valid_names.cbegin(), valid_names.cend(), zone) != valid_names.end();
    }

    void applyNetworkSettings(const NetworkSettings& ns, Aws::Client::ClientConfiguration& config) {
        // @TODO: could also do request timeout etc.

        config.caFile = ns.caFile.c_str();
        config.caPath = ns.caPath.c_str();
        config.verifySSL = ns.verifySSL;
    }

}

#endif