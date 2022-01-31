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
#ifndef TUPLEX_AWSCOMMON_H
#define TUPLEX_AWSCOMMON_H

#include <string>
#include <unordered_map>
#include <cstdlib>
#include <vector>

#include <Utils.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/HashingUtils.h>

namespace tuplex {

    struct AWSCredentials {
        std::string access_key;
        std::string secret_key;
        std::string session_token; // required for temp credentials like the ones used in Lambda
        std::string default_region;

        static AWSCredentials get();
    };

    // use base64 library from https://github.com/ReneNyffenegger/cpp-base64

    inline std::shared_ptr<Aws::IOStream> stringToAWSStream(const std::string &str, const std::string &tag = "tuplex") {
        auto input = Aws::MakeShared<Aws::StringStream>(tag.c_str());

        *input << str.c_str();
        input->flush();
        return input;
    }

    /*!
     * update clientConfig with given Network settings.
     * @param ns network settings
     * @param config AWS clientConfig
     */
    extern void applyNetworkSettings(const NetworkSettings& ns, Aws::Client::ClientConfiguration& config);

    /*!
     * initializes AWS SDK globally (lazy) and add S3 FileSystem.
     * @return true if initializing, else false
     */
    extern bool initAWS(const AWSCredentials& credentials=AWSCredentials::get(), const NetworkSettings& ns=NetworkSettings(), bool requesterPay=false);

    /*!
     * shuts down AWS SDK (freeing resourced).
     */
    extern void shutdownAWS();

    /*!
     * validates zone string.
     * @param zone
     * @return true/false.
     */
    extern bool isValidAWSZone(const std::string& zone);


    inline std::string decodeAWSBase64(const std::string& log) {
        std::stringstream ss;
        // Decode the result header to see requested log information
        auto byteLogResult = Aws::Utils::HashingUtils::Base64Decode(log.c_str());
        for (unsigned i = 0; i < byteLogResult.GetLength(); i++)
            ss << byteLogResult.GetItem(i);
        auto logTail =  ss.str();
        return logTail;
    }
}

// Amazon frequently changes the parameters of lambda functions,
// this here are a couple constants to update them
// current state: https://aws.amazon.com/about-aws/whats-new/2020/12/aws-lambda-supports-10gb-memory-6-vcpu-cores-lambda-functions/#:~:text=Events-,AWS%20Lambda%20now%20supports%20up%20to%2010%20GB%20of%20memory,vCPU%20cores%20for%20Lambda%20Functions&text=AWS%20Lambda%20customers%20can%20now,previous%20limit%20of%203%2C008%20MB.
// cf. https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
// also check https://www.fourtheorem.com/blog/lambda-10gb for quotas on vCPU, i.e. how much is provisioned.
// https://docs.aws.amazon.com/lambda/latest/dg/configuration-function-common.html

// i.e. at 1,769 MB, a function has the equivalent of one vCPU (one vCPU-second of credits per second).
#define AWS_MINIMUM_LAMBDA_MEMORY_MB 128ul
// how much memory a function for Tuplex should have
#define AWS_MINIMUM_TUPLEX_MEMORY_REQUIREMENT_MB 384ul

// 10240MB, i.e. 10GB
#define AWS_MAXIMUM_LAMBDA_MEMORY_MB 10240ul
// 15 min
#define AWS_MAXIMUM_LAMBDA_TIMEOUT 900ul
// minumum Tuplex requirement (5s?)
#define AWS_MINIMUM_TUPLEX_TIMEOUT_REQUIREMENT 5ul

// note(Leonhard): I think the number of available cores/threads is dependent on the memory size
#define AWS_MAXIMUM_LAMBDA_THREADS 6

// may change, the minimum AWS limit for unreserved concurrency functions. Also a limit how much can get executed...
#define AWS_MINIMUM_UNRESERVED_CONCURRENCY 100

// the 64MB increase limit seems to have been changed now...

#endif //TUPLEX_AWSCOMMON_H
#endif