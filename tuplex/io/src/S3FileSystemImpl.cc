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
#include <S3FileSystemImpl.h>
#include <Logger.h>
#include <S3File.h>

#include <aws/core/platform/Environment.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>

#include <aws/s3/model/ListObjectsV2Request.h>
#include <regex>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <Timer.h>
#include <Utils.h>
#include <FileUtils.h>

#include <AWSCommon.h>

// Notes: a list request costs $0.005 per 1,000 requests
// i.e. S3 charges $0.005 per 1,000 put/copy/post/list requests
// also it charges in us east $0.023 per GB for the first 50TB/month of storage used

std::string unixWildcardToGrep(const std::string& r, bool match_full=true) {
    // need to change syntax a little
    // i.e. when there is * this mean 0 to x chars => transform to .*
    // ? gets transformed to

    std::string s;
    if(match_full)
        s +="^";
    // TODO: [] should be converted properly to!
    // from https://www.rgagnon.com/javadetails/java-0515.html
    for(auto c : r) {
        switch(c) {
            case '*': {
                s += ".*";
                break;
            }
            case '?': {
                s += ".";
                break;
            }
            case '(':
            case ')':
            case '[':
            case ']':
            case '$':
            case '^':
            case '.':
            case '{':
            case '}':
            case '|':
            case '\\': {
                // escape these chars
                s += "\\";
                s += c;
                break;
            }
            default:
                s += c;
                break;
        }
    }
    if(match_full)
        s += "$";
    return s;
}

// common helper functions
std::regex wildcardToRegex(const std::string& r, bool match_full=true) {
    auto s = unixWildcardToGrep(r, match_full);
    return std::regex(s, std::regex_constants::grep);
}


// match until special char
size_t prefixMatchLength(const std::string& s, const std::string& w) {

    // if one is 0, return 0
    if(s.empty() || w.empty())
        return 0;

    // match wildcard recursively

    // stop at /
    if(s[0] == '/')
        return 1;

    // special cases first:
    if(w[0] == '?')
        return 1 + prefixMatchLength(s.substr(1), w.substr(1));

    if(w[0] == '*')
        return 1 + std::max(prefixMatchLength(s.substr(1), w), prefixMatchLength(s.substr(1), w.substr(1)));

    if(w[0] == s[0])
        return 1 + prefixMatchLength(s.substr(1), w.substr(1));
    else
        return 0;
}

std::string remainingMatchSuffix(const std::string& s, const std::string& w) {

    // if one is 0, return 0
    if(s.empty())
        return w;

    // match wildcard recursively

    // stop at /
    if(s[0] == '/')
        return w.substr(1);

    // special cases first:
    if(w[0] == '*')
        return remainingMatchSuffix(s.substr(1), w);

    // consume one letter
    if(w[0] == s[0] || w[0] == '?')
        return remainingMatchSuffix(s.substr(1), w.substr(1));
    else
        return w;
}


// S3 helper functions
// returns numListRequests
static size_t s3walk(const Aws::S3::S3Client& client, const std::string& bucket, const std::string& prefix, const std::string& suffix, std::vector<tuplex::URI>& files, const Aws::S3::Model::RequestPayer &requestPayer) {
    using namespace std;

    size_t numRequests = 0;

    // @TODO: use longest prefix function to speed up querying!

    // prefix is not allowed to contain any wildcard symbols!
    // --> needs to be unescaped!

    // suffix regex
    auto suffix_rgx = wildcardToRegex(suffix);

    // paths that partially match to further recurse, contains tuples of (new_prefix, new_suffix)
    // walk then via s3walk(client, bucket, new_prefix, new_suffix);
    vector<tuple<string, string>> prefix_matches;


    // list one directory
    Aws::S3::Model::ListObjectsV2Request objects_request;
    objects_request.WithBucket(Aws::String(bucket.c_str()));
    objects_request.WithPrefix(Aws::String(prefix.c_str()));
    objects_request.SetRequestPayer(requestPayer);
    // use delimiter if suffix is not yet exhausted
    if(suffix.length() > 0)
        objects_request.WithDelimiter("/");

    auto list_objects_outcome = client.ListObjectsV2(objects_request);
#ifndef NDEBUG
    Logger::instance().defaultLogger().info("made S3 list request");
#endif
    numRequests++;
    while(list_objects_outcome.IsSuccess()) {
        auto& result = list_objects_outcome.GetResult();

        // 1. subfolders
        // cout<<"found "<<result.GetCommonPrefixes().size()<<" subfolders, "<<result.GetContents().size()<<" files in "<<s3path<<endl;

        // match prefix iff there is still / in suffix
        if(suffix.find('/') != std::string::npos)

            for(auto const& cp : result.GetCommonPrefixes()) {
                std::string cp_part = cp.GetPrefix().c_str();

                // longest match to suffix
                auto part_to_match = cp_part.substr(prefix.length());
                auto new_suffix = remainingMatchSuffix(part_to_match, suffix);

                // remove leading trail from suffix
                if(!new_suffix.empty())
                    if('/' == new_suffix.front())
                        new_suffix = new_suffix.substr(1);

                int matchLength  = prefixMatchLength(part_to_match, suffix);
                if(matchLength == part_to_match.length()) {
                    auto new_prefix = prefix + part_to_match.substr(0, matchLength + 1);

                    // special case, new_suffix is empty because of dir match
                    if(new_suffix.empty())
                        files.emplace_back("s3://" + bucket + "/" + new_prefix);
                    else
                        prefix_matches.emplace_back(make_tuple(new_prefix, new_suffix));
                }
            }

        // 2. files in dir
        size_t num_matched_files = 0;
        for(const auto& o : result.GetContents()) {
            std::string key = o.GetKey().c_str();
            auto part_to_match = key.substr(prefix.length());
            if(regex_match(part_to_match, suffix_rgx)) {
                files.emplace_back("s3://" + bucket + "/" + key);
                num_matched_files++;
            }
        }

        // cout<<"  * "<<prefix_matches.size()<<" subfolders, "<<num_matched_files<<" files matched"<<endl;

        if(result.GetIsTruncated()) {
            objects_request.SetContinuationToken(result.GetNextContinuationToken());
            list_objects_outcome = client.ListObjectsV2(objects_request);
            numRequests++;
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("made list request");
#endif
        } else {
            break;
        }
    }

    if(!list_objects_outcome.IsSuccess()){
        cerr<<list_objects_outcome.GetError().GetExceptionName()<<": "<<list_objects_outcome.GetError().GetMessage()<<endl;
    }

    // recurse
    for(auto t : prefix_matches) {
        auto new_prefix = std::get<1>(t);
        numRequests += s3walk(client, bucket, std::get<0>(t), std::get<1>(t), files, requestPayer);
    }

    return numRequests;
}


namespace tuplex {

    std::unique_ptr<VirtualMappedFile> S3FileSystemImpl::map_file(const tuplex::URI &uri) {
        auto& logger = Logger::instance().logger("s3fs");
        logger.error("S3 filesystem does not provide mapping functionality, use open_file instead");
        return nullptr;
    }

    std::unique_ptr<VirtualFile> S3FileSystemImpl::open_file(const tuplex::URI &uri, tuplex::VirtualFileMode vfm) {
        try {
            return std::make_unique<S3File>(*this, uri, vfm, _requestPayer);
        } catch(const std::exception& e) {
            auto& logger = Logger::instance().logger("s3fs");
            logger.error("opening file " + uri.toPath() + " failed with exception: " + e.what());
            return nullptr;
        }
    }

    VirtualFileSystemStatus S3FileSystemImpl::create_dir(const tuplex::URI &uri) {
        auto& logger = Logger::instance().logger("s3fs");
        logger.error("create dir not yet implemented");
        return VirtualFileSystemStatus::VFS_NOTYETIMPLEMENTED;
    }

    VirtualFileSystemStatus S3FileSystemImpl::file_size(const tuplex::URI &uri, uint64_t &size) {

        // quick way to retrieve file size is to make a tiny parts request to the uri
        // range header

        // request 128b
        std::string range = "bytes=" + std::to_string(0) + "-" + std::to_string(127);
        // make AWS S3 part request to uri
        // check how to retrieve object in poarts
        Aws::S3::Model::GetObjectRequest req;
        req.SetBucket(uri.s3Bucket().c_str());
        req.SetKey(uri.s3Key().c_str());
        // retrieve byte range according to http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
        req.SetRange(range.c_str());

        // Get the object
        auto get_object_outcome = client().GetObject(req);
        _getRequests++;
        if (get_object_outcome.IsSuccess()) {
            auto result = get_object_outcome.GetResultWithOwnership();

            // extract extracted byte range + size
            // syntax is: start-inclend/fsize
            auto cr = result.GetContentRange();
            auto idxSlash = cr.find_first_of('/');
            auto idxMinus = cr.find_first_of('-');
            // these are kind of weird, they are already requested range I presume
            // size_t rangeStart = std::strtoull(cr.substr(0, idxMinus).c_str(), nullptr, 10);
            // size_t rangeEnd = std::strtoull(cr.substr(idxMinus + 1, idxSlash).c_str(), nullptr, 10);
            size_t fileSize = std::strtoull(cr.substr(idxSlash + 1).c_str(), nullptr, 10);
            size = fileSize;
            return VirtualFileSystemStatus::VFS_OK;
        } else {
            // MessageHandler& logger = Logger::instance().logger("s3fs");
            // logger.error(outcome_error_message(get_object_outcome));
            // throw std::runtime_error(outcome_error_message(get_object_outcome));
            return VirtualFileSystemStatus::VFS_IOERROR;
        }
    }

    VirtualFileSystemStatus S3FileSystemImpl::ls(const tuplex::URI &parent, std::vector<tuplex::URI>& uris) {

        MessageHandler& logger = Logger::instance().logger("S3 filesystem");

        // extract local path (i.e. remove file://)
        auto path = parent.toString().substr(parent.prefix().length());

        // make sure no wildcard is present yet --> not supported!
        auto prefix = findLongestPrefix(path);

        if(prefix.length() != path.length()) {
            logger.error("URI " + parent.toString() + " contains Unix wildcard characters, not supported yet");
            return VirtualFileSystemStatus::VFS_IOERROR;
        }

        // split into bucket and key
        auto bucket = parent.s3Bucket();
        auto key = parent.s3Key();

        if("" == bucket) {
            // make sure key is empty as well!
            assert("" == key);
            // list simply all buckets...

            auto outcome = this->client().ListBuckets();
            _lsRequests++;
            if(outcome.IsSuccess()) {
                auto buckets = outcome.GetResult().GetBuckets();
                for(auto entry : buckets) {
                    uris.push_back(URI("s3://" + std::string(entry.GetName().c_str())));
                }
                return VirtualFileSystemStatus::VFS_OK;
            } else {
                logger.error("Failed listing buckets. Details: " + std::string(outcome.GetError().GetMessage().c_str()));
                return VirtualFileSystemStatus::VFS_IOERROR;
            }
        } else {
            // this is a listobjects query! => could have continuation token!
            Aws::S3::Model::ListObjectsV2Request objects_request;

            std::vector<URI> output_uris;

            auto s3_prefix = key;
            // does it end in /? if not amend! => can't know if folder or not...
            if(s3_prefix.back() != '/') {

                // perform here single request to check whether it's a single file...
                objects_request.WithBucket(Aws::String(bucket.c_str()));
                objects_request.WithPrefix(Aws::String(s3_prefix.c_str()));
                objects_request.WithDelimiter("/");
                objects_request.SetRequestPayer(_requestPayer);

                auto list_objects_outcome = _client->ListObjectsV2(objects_request);
#ifndef NDEBUG
                Logger::instance().defaultLogger().info("made list request as part of ls function");
#endif
                _lsRequests++;
                if(list_objects_outcome.IsSuccess()) {
                    // can be only a single file...
                    auto& result = list_objects_outcome.GetResult();
                    for(const auto& o : result.GetContents()) {
                        std::string key = o.GetKey().c_str();
                        URI uri("s3://" + bucket + "/" + key);
                        output_uris.push_back(uri);
                    }

                    // these are folders
                    for(const auto& cp : result.GetCommonPrefixes()) {
                        auto key = cp.GetPrefix().c_str();
                        URI uri("s3://" + bucket + "/" + key);
                        output_uris.push_back(uri);
                    }
                }
                // reset request
                objects_request = Aws::S3::Model::ListObjectsV2Request();

                s3_prefix = s3_prefix + "/";
            }

            objects_request.WithBucket(Aws::String(bucket.c_str()));
            objects_request.WithPrefix(Aws::String(s3_prefix.c_str()));
            objects_request.WithDelimiter("/");
            objects_request.SetRequestPayer(_requestPayer);

            auto list_objects_outcome = _client->ListObjectsV2(objects_request);
#ifndef NDEBUG
            Logger::instance().defaultLogger().info("made list request as part of ls function");
#endif
            _lsRequests++;
            while(list_objects_outcome.IsSuccess()) {
                auto& result = list_objects_outcome.GetResult();

                // 2. files in dir
                // these are files
                for(const auto& o : result.GetContents()) {
                    std::string key = o.GetKey().c_str();
                    URI uri("s3://" + bucket + "/" + key);
                    output_uris.push_back(uri);
                }
                // these are folders
                for(const auto& cp : result.GetCommonPrefixes()) {
                    auto key = cp.GetPrefix().c_str();
                    URI uri("s3://" + bucket + "/" + key);
                    output_uris.push_back(uri);
                }
                if(result.GetIsTruncated()) {
                    objects_request.SetContinuationToken(result.GetNextContinuationToken());
                    list_objects_outcome = _client->ListObjectsV2(objects_request);
                   _lsRequests++;
#ifndef NDEBUG
                    Logger::instance().defaultLogger().info("made list request as part of ls function (continuation)");
#endif
                } else {
                    break;
                }
            }

            // clean up -> i.e. single folder case
            if(output_uris.size() > 1) {
                auto it = std::find(output_uris.begin(), output_uris.end(), URI("s3://" + bucket + "/" + s3_prefix));
                if(it != output_uris.end())
                    output_uris.erase(it);
            }

            uris = output_uris;
            return VirtualFileSystemStatus::VFS_OK;
        }

        return VirtualFileSystemStatus::VFS_NOTYETIMPLEMENTED;
    }

    VirtualFileSystemStatus S3FileSystemImpl::touch(const tuplex::URI &uri, bool overwrite) {
        return VirtualFileSystemStatus::VFS_NOTYETIMPLEMENTED;
    }


    bool containsUnescapedChar(const std::string& s, char c, char escape_char = '\\') {
        if(!s.empty()) {
            if(s[0] == c)
                return true;
            for(int i = 1; i < s.length(); ++i)
                if(s[i] == c && s[i - 1] != escape_char)
                    return true;
        }
        return false;
    }

    std::vector<URI> S3FileSystemImpl::glob(const std::string &pattern) {

        // in debug mode check that there is no , in the path
#ifndef NDEBUG
        assert(!containsUnescapedChar(pattern, ','));
#endif

        // perform single uri s3 check
        URI uri(pattern);
        assert(uri.prefix() == "s3://");

        // call glob function
        std::vector<URI> files;

        // make sure there is no pattern in the bucket
        auto bucket = uri.s3Bucket();

        if(containsUnescapedChar(bucket, '*') || containsUnescapedChar(bucket, '?')) {
            // throw exception
            Logger::instance().logger("s3fs").warn("globbing for bucket not supported, '" + pattern + "' invalid. Glob will return empty list.");
            return std::vector<URI>();
        }

        _lsRequests += s3walk(client(), uri.s3Bucket(), "", uri.s3Key(), files, _requestPayer);
        return files;
    }

    S3FileSystemImpl::S3FileSystemImpl(const std::string& access_key, const std::string& secret_key,
                                       const std::string& session_token, const std::string& region,
                                       const NetworkSettings& ns, bool lambdaMode, bool requesterPay) {
        // Note: If current region is different than other region, use S3 transfer acceleration
        // cf. Aws::S3::Model::GetBucketAccelerateConfigurationRequest
        // and https://s3-accelerate-speedtest.s3-accelerate.amazonaws.com/en/accelerate-speed-comparsion.html
        // for same region this is slower...

        using namespace Aws;


        // set counters to zero
        _putRequests = 0;
        _initMultiPartUploadRequests = 0;
        _multiPartPutRequests = 0;
        _closeMultiPartUploadRequests = 0;
        _getRequests = 0;
        _bytesTransferred = 0;
        _bytesReceived = 0;
        _lsRequests = 0;

        Client::ClientConfiguration config;

        AWSCredentials credentials;
        if(access_key.empty() || secret_key.empty() || region.empty())
            credentials = AWSCredentials::get();

        // overwrite with manually specified ones
        if(!access_key.empty())
            credentials.access_key = access_key;
        if(!secret_key.empty())
            credentials.secret_key = secret_key;
        if(!session_token.empty())
            credentials.session_token = session_token;
        if(!region.empty())
            credentials.default_region = region;

        // apply network settings
        applyNetworkSettings(ns, config);

        // fill in config
        config.region = credentials.default_region;

        if(lambdaMode) {
            if(config.region.empty())
                config.region = Aws::Environment::GetEnv("AWS_REGION");
        }

        if(requesterPay)
            _requestPayer = Aws::S3::Model::RequestPayer::requester;
        else
            _requestPayer = Aws::S3::Model::RequestPayer::NOT_SET;

        auto aws_credentials = Auth::AWSCredentials(credentials.access_key.c_str(),
                                                    credentials.secret_key.c_str(),
                                                    credentials.session_token.c_str());

        // lambda Mode? just use default settings.
        if(lambdaMode) {
            _client = std::make_shared<S3::S3Client>(aws_credentials);
            _requestPayer = Aws::S3::Model::RequestPayer::requester;

            std::stringstream ss;
            ss<<"S3 Client initialized using defaults";
            Logger::instance().defaultLogger().info(ss.str());
            return;
        }

        _client = std::make_shared<S3::S3Client>(aws_credentials, config);

//        if(lambdaMode) {
//            // disable virtual host to prevent curl code 6 https://guihao-liang.github.io/2020/04/08/aws-virtual-address
//            _client = std::make_shared<S3::S3Client>(aws_credentials,
//                                                     config,
//                                                     Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
//                                                     false);
//
//            // log out settings quickly (debug)
//            std::stringstream ss;
//            ss<<"S3 settings: REGION="<<config.region.c_str()<<" VERIFY_SSL="<<config.verifySSL<<" CAFILE="<<config.caFile<<" CAPATH="<<config.caPath;
//            Logger::instance().defaultLogger().info(ss.str());
//
//        } else {
//
//        }
    }



// S3 helper functions
// returns numListRequests
#warning "folders don't work here yet..."
    static bool s3walkEx(const Aws::S3::S3Client& client,
                         const std::string& bucket,
                         const std::string& prefix,
                         const std::string& suffix,
                         size_t& numRequests,
                         std::function<bool(void *, const tuplex::URI &, size_t)> callback,
                         void *userData,
                         const Aws::S3::Model::RequestPayer &requestPayer) {
        using namespace std;

        // @TODO: use longest prefix function to speed up querying!

        // prefix is not allowed to contain any wildcard symbols!
        // --> needs to be unescaped!

        // suffix regex
        auto suffix_rgx = wildcardToRegex(suffix);

        // paths that partially match to further recurse, contains tuples of (new_prefix, new_suffix)
        // walk then via s3walk(client, bucket, new_prefix, new_suffix, requestPayer);
        vector<tuple<string, string>> prefix_matches;


        // list one directory
        Aws::S3::Model::ListObjectsV2Request objects_request;
        objects_request.WithBucket(Aws::String(bucket.c_str()));
        objects_request.WithPrefix(Aws::String(prefix.c_str()));
        objects_request.SetRequestPayer(requestPayer);

        // use delimiter if suffix is not yet exhausted
        if(suffix.length() > 0)
            objects_request.WithDelimiter("/");

        auto list_objects_outcome = client.ListObjectsV2(objects_request);
#ifndef NDEBUG
        Logger::instance().defaultLogger().info("made list request");
#endif
        numRequests++;
        while(list_objects_outcome.IsSuccess()) {
            auto& result = list_objects_outcome.GetResult();

            // 1. subfolders
            // cout<<"found "<<result.GetCommonPrefixes().size()<<" subfolders, "<<result.GetContents().size()<<" files in "<<s3path<<endl;

            // match prefix iff there is still / in suffix
            if(suffix.find('/') != std::string::npos)

                for(auto const& cp : result.GetCommonPrefixes()) {
                    std::string cp_part = cp.GetPrefix().c_str();

                    // longest match to suffix
                    auto part_to_match = cp_part.substr(prefix.length());
                    auto new_suffix = remainingMatchSuffix(part_to_match, suffix);

                    // remove leading trail from suffix
                    if(!new_suffix.empty())
                        if('/' == new_suffix.front())
                            new_suffix = new_suffix.substr(1);

                    int matchLength  = prefixMatchLength(part_to_match, suffix);
                    if(matchLength == part_to_match.length()) {
                        auto new_prefix = prefix + part_to_match.substr(0, matchLength + 1);

                        // special case, new_suffix is empty because of dir match
                        if(new_suffix.empty()) {
                            // this here should be a subdirectory!!!
                            URI uri("s3://" + bucket + "/" + new_prefix);
                            size_t file_size = 0;
                            throw std::runtime_error("nyimpl, file size missing here!!!");
                            if(!callback(userData, uri, file_size))
                                return false;
                        }
                        else
                            prefix_matches.emplace_back(make_tuple(new_prefix, new_suffix));
                    }
                }

            // 2. files in dir
            size_t num_matched_files = 0;
            for(const auto& o : result.GetContents()) {
                std::string key = o.GetKey().c_str();
                auto part_to_match = key.substr(prefix.length());
                if(regex_match(part_to_match, suffix_rgx)) {
                    URI uri("s3://" + bucket + "/" + key);
                    size_t file_size = o.GetSize();
                    if(!callback(userData, uri, file_size))
                        return false;
                    num_matched_files++;
                }
            }

            // cout<<"  * "<<prefix_matches.size()<<" subfolders, "<<num_matched_files<<" files matched"<<endl;

            if(result.GetIsTruncated()) {
                objects_request.SetContinuationToken(result.GetNextContinuationToken());
                list_objects_outcome = client.ListObjectsV2(objects_request);
                numRequests++;
#ifndef NDEBUG
                Logger::instance().defaultLogger().info("made list request");
#endif
            } else {
                break;
            }
        }

        if(!list_objects_outcome.IsSuccess()){
            cerr<<list_objects_outcome.GetError().GetExceptionName()<<": "<<list_objects_outcome.GetError().GetMessage()<<endl;
        }

        // recurse
        for(auto t : prefix_matches) {
            auto new_prefix = std::get<1>(t);
            if(!s3walkEx(client, bucket, std::get<0>(t), std::get<1>(t), numRequests, callback, userData, requestPayer))
                return false;
        }

        return true;
    }

    // overwrites default implementation to be a bit more efficient
    bool S3FileSystemImpl::walkPattern(const tuplex::URI &pattern,
                                       std::function<bool(void *, const tuplex::URI &, size_t)> callback,
                                       void *userData) {

        // perform single uri s3 check
        URI uri(pattern);
        assert(uri.prefix() == "s3://");

        // call glob function
        std::vector<URI> files;

        // make sure there is no pattern in the bucket
        auto bucket = uri.s3Bucket();

        if(containsUnescapedChar(bucket, '*') || containsUnescapedChar(bucket, '?')) {
            // throw exception
            Logger::instance().logger("s3fs").warn("globbing for bucket not supported, '" + pattern.toPath() + "' invalid. Glob will return empty list.");
            return false;
        }


        size_t lsRequests = 0;

        // call with prefix="" for original and suffix=uri.s3Key()

        // find longest non pattern prefix to speed up walking queries
        auto prefix = findLongestPrefix(uri.s3Key());
        auto suffix = uri.s3Key().substr(prefix.length());
        auto res = s3walkEx(client(), uri.s3Bucket(), prefix, suffix, lsRequests, callback, userData, _requestPayer);
        _lsRequests += lsRequests;
        return res;
    }

    void S3FileSystemImpl::resetCounters() {
        _putRequests = 0;
        _initMultiPartUploadRequests = 0;
        _multiPartPutRequests = 0;
        _closeMultiPartUploadRequests = 0;
        _getRequests = 0;
        _bytesTransferred = 0;
        _bytesReceived = 0;
        _lsRequests = 0;
    }

    void S3FileSystemImpl::initTransferThreadPool(size_t numThreads) {
        // lazy init
        if(!_thread_pool)
            _thread_pool = std::make_shared<Aws::Utils::Threading::PooledThreadExecutor>(numThreads, Aws::Utils::Threading::OverflowPolicy::QUEUE_TASKS_EVENLY_ACCROSS_THREADS);

        if(!_transfer_manager) {
            Aws::Transfer::TransferManagerConfiguration config(_thread_pool.get());
            config.s3Client = _client;

            // @TODO: add callbacks for better upload etc.?
            // std::function<void(const TransferManager*, const std::shared_ptr<const TransferHandle>&, const Aws::Client::AWSError<Aws::S3::S3Errors>&)>
            Aws::Transfer::ErrorCallback error_callback = [](const Aws::Transfer::TransferManager* manager, const std::shared_ptr<const Aws::Transfer::TransferHandle>& handle,
                                                             const Aws::Client::AWSError<Aws::S3::S3Errors>& err) {
                auto& logger = Logger::instance().logger("filesystem");
                std::string target_path;
                if(handle) {
                    target_path = URI::fromS3(handle->GetBucketName().c_str(), handle->GetKey().c_str()).toPath();
                }

                std::stringstream ss;
                ss<<"AWS File Transfer error ("<<err.GetExceptionName().c_str()<<"). Details: "<<err.GetMessage().c_str();
                logger.error(ss.str());
            };
            config.errorCallback = error_callback;
            _transfer_manager = Aws::Transfer::TransferManager::Create(config);
        }
    }

    std::shared_ptr<Aws::Transfer::TransferHandle> S3FileSystemImpl::downloadFile(const URI &s3_uri,
                                                                                  const std::string &local_path) {
        MessageHandler& logger = Logger::instance().logger("filesystem");
        initTransferThreadPool();

        Timer timer;

        // create parent dirs if needed.
        if(std::string::npos != local_path.find("/")) {
            auto p = URI(local_path).parent();
            auto vfs = VirtualFileSystem::fromURI(p);
            vfs.create_dir(p);
        }

        auto handle = _transfer_manager->DownloadFile(s3_uri.s3Bucket(), s3_uri.s3Key(), local_path);
        handle->WaitUntilFinished();
        double time = timer.time();
        // update stats
        _bytesReceived += handle->GetBytesTransferred();
        _getRequests += !handle->IsMultipart(); // if is not multipart add one
        // how to treat multipart? --> need that for cost info!!!
        // @TODO


        double transfer_rate = handle->GetBytesTransferred() / (1024.0 * 1024.0 * time);

        switch(handle->GetStatus()) {
            case Aws::Transfer::TransferStatus::COMPLETED:
                logger.info("downloaded file (" + sizeToMemString(handle->GetBytesTransferred()) +") from " + s3_uri.toPath() + " in " + std::to_string(time) + "s (" + std::to_string(transfer_rate) + " MB/s)");
                break;
            case Aws::Transfer::TransferStatus::EXACT_OBJECT_ALREADY_EXISTS:
                logger.info("skip file download to " + s3_uri.toPath() + ", file already exists");
                break;
            case Aws::Transfer::TransferStatus::ABORTED:
            case Aws::Transfer::TransferStatus::CANCELED:
            case Aws::Transfer::TransferStatus::FAILED:
                logger.info("failed to download file from " + s3_uri.toPath());
            default:
                return nullptr;
        }

        return handle;
    }

    std::shared_ptr<Aws::Transfer::TransferHandle> S3FileSystemImpl::uploadFile(const std::string &local_path,
                                                                                const URI &s3_uri,
                                                                                const std::string &content_type) {
        MessageHandler& logger = Logger::instance().logger("filesystem");
        initTransferThreadPool();
        Timer timer;
        auto handle = _transfer_manager->UploadFile(local_path, s3_uri.s3Bucket(), s3_uri.s3Key(), content_type, Aws::Map<Aws::String, Aws::String>());
        handle->WaitUntilFinished();
        double time = timer.time();
        // update stats
        _bytesTransferred += handle->GetBytesTransferred();
        _putRequests += !handle->IsMultipart();
        _multiPartPutRequests += handle->IsMultipart();

        double transfer_rate = handle->GetBytesTransferred() / (1024.0 * 1024.0 * time);

        switch(handle->GetStatus()) {
            case Aws::Transfer::TransferStatus::COMPLETED:
                logger.info("uploaded file (" + sizeToMemString(handle->GetBytesTransferred()) +") to " + s3_uri.toPath() + " in " + std::to_string(time) + "s (" + std::to_string(transfer_rate) + " MB/s)");
                break;
            case Aws::Transfer::TransferStatus::EXACT_OBJECT_ALREADY_EXISTS:
                logger.info("skip file upload to " + s3_uri.toPath() + ", file already exists");
                break;
            case Aws::Transfer::TransferStatus::ABORTED:
            case Aws::Transfer::TransferStatus::CANCELED:
            case Aws::Transfer::TransferStatus::FAILED:
                logger.info("failed to upload file to " + s3_uri.toPath());
            default:
                return nullptr;
        }

        return handle;
    }

    VirtualFileSystemStatus S3FileSystemImpl::remove(const URI &uri) {
        assert(uri.prefix() == "s3://");
        MessageHandler& logger = Logger::instance().logger("filesystem");

        // check it's valid, do not support deletion of buckets!
        auto bucket = uri.s3Bucket();
        auto suffix = uri.s3Key();
        if(suffix.empty()) {
            logger.warn("Deletion of bucket " + bucket + " is not supported by Tuplex");
            return VirtualFileSystemStatus::VFS_INVALIDPREFIX;
        }

        // use glob to get all paths
        // Note: could optimize this here, i.e. if no wildcards are contained simply use the paths
        // ignore then does not exist errors!
        auto paths = VirtualFileSystem::globAll(uri.toPath());
        logger.debug("found " + std::to_string(paths.size()) + " objects to remove");
        if(paths.empty())
            return VirtualFileSystemStatus::VFS_OK;

        // now issue for each of the files an async delete request
        // use https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
        Aws::S3::Model::DeleteObjectsRequest del_request;
        Aws::S3::Model::Delete del_keys;
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects_to_delete;
        for(auto p : paths) {
            Aws::S3::Model::ObjectIdentifier id;
            assert(p.s3Bucket() == bucket);
            id.WithKey(p.s3Key());
            objects_to_delete.emplace_back(id);
        }
        del_keys.WithObjects(objects_to_delete);
        del_request.WithBucket(uri.s3Bucket());
        del_request.WithDelete(del_keys);

        // perform request
        auto outcome = client().DeleteObjects(del_request);
        if(outcome.IsSuccess()) {
            auto res = outcome.GetResult();
            auto deleted_paths = res.GetDeleted();
            for(auto p : deleted_paths) {
                logger.debug("removed s3://" + bucket + "/" + p.GetKey() + " from S3");
            }

            // errors? print!
            auto errs = res.GetErrors();
            if(errs.empty())
                return VirtualFileSystemStatus::VFS_OK;

            for(auto err : errs) {
                std::stringstream ss;
                ss<<"Could not remove s3://"<<bucket<<"/"<<err.GetKey()<<" ("<<err.GetCode()<<"), "<<err.GetMessage();
                logger.error(ss.str());
            }

            return VirtualFileSystemStatus::VFS_IOERROR;
        } else {
            std::stringstream ss;
            auto err = outcome.GetError();
            auto err_message = err.GetMessage();
            auto err_name = err.GetExceptionName();
            ss<<"failed to delete "<<paths.size()<<" from "<<uri.toPath()<<". Details: "
              <<err_name<<", "<<err_message;
            logger.error(ss.str());
            return VirtualFileSystemStatus::VFS_IOERROR;
        }

        // i.e. do always two requests:
        // delete + list
        // => can be done multithreaded!
        return VirtualFileSystemStatus::VFS_OK;
    }

    std::vector<URI> S3FileSystemImpl::lsPrefix(const URI &prefix) {
        using namespace std;
        vector<URI> files;
        auto _prefix = findLongestPrefix(prefix.s3Key());             // only on key fraction!
        auto& logger = Logger::instance().logger("filesystem");

        // make sure bucket is not *!
        auto bucket = prefix.s3Bucket();
        if(bucket.empty() || findLongestPrefix(bucket) != bucket) {
            logger.error("patterns across buckets not supported!");
            return files;
        }

        // create regex pattern
        auto grep_pattern = "^" + unixWildcardToGrep(prefix.s3Key(), false);
        auto r_prefix = std::regex(grep_pattern, std::regex_constants::grep);

        // list one directory
        Aws::S3::Model::ListObjectsV2Request objects_request;
        objects_request.WithBucket(Aws::String(prefix.s3Bucket().c_str()));
        objects_request.WithPrefix(Aws::String(_prefix.c_str()));

        auto list_objects_outcome = client().ListObjectsV2(objects_request);
        logger.debug("made S3 list request");
        _lsRequests++;
        while(list_objects_outcome.IsSuccess()) {
            auto& result = list_objects_outcome.GetResult();

            // add all paths where full prefix matches to output
            for(const auto& o : result.GetContents()) {
                std::string key = o.GetKey().c_str();

                if(regex_match(key, r_prefix)) {
                    files.emplace_back("s3://" + bucket + "/" + key);
                }
            }
            if(result.GetIsTruncated()) {
                objects_request.SetContinuationToken(result.GetNextContinuationToken());
                list_objects_outcome = client().ListObjectsV2(objects_request);
                _lsRequests++;
#ifndef NDEBUG
                logger.debug("made list request");
#endif
            } else {
                break;
            }
        }

        if(!list_objects_outcome.IsSuccess()) {
            stringstream ss;
            ss<<"failed to perform S3 list request. "
              <<list_objects_outcome.GetError().GetExceptionName()
              <<": "<<list_objects_outcome.GetError().GetMessage();
            logger.error(ss.str());
        }

        return files;
    }

    bool S3FileSystemImpl::copySingleFileWithinS3(const URI &s3_src, const URI &s3_dest) {
        // perform an S3 to S3 copy request
        assert(s3_src.prefix() == "s3://");
        assert(s3_dest.prefix() == "s3://");
        auto& logger = Logger::instance().logger("filesystem");

        if(s3_src == s3_dest)
            return true;

        // check no wildcard pattern
        assert(findLongestPrefix(s3_src.toString()) == s3_src.toString());

        Aws::S3::Model::CopyObjectRequest req;
        req.WithCopySource(s3_src.s3Bucket() + "/" + s3_src.s3Key())
           .WithBucket(s3_dest.s3Bucket()).WithKey(s3_dest.s3Key());

        auto outcome = client().CopyObject(req);
        if(!outcome.IsSuccess()) {
            auto err = outcome.GetError();
            std::stringstream ss;
            ss<<"failed to copy "<<s3_src.toString()<<" to "<<s3_dest.toString() <<". "
              <<err.GetExceptionName()<<": "<<err.GetMessage();
            logger.error(ss.str());
            return false;
        } else {
            logger.debug("copied " + s3_src.toString() + " to " + s3_dest.toString());
            return true;
        }
    }


    std::string s3GetHeadObject(Aws::S3::S3Client const& client, const URI& uri, std::ostream *os_err) {
        using namespace std;
        string meta_data;

        assert(uri.prefix() == "s3://");

        // perform request
        Aws::S3::Model::HeadObjectRequest request;
        request.WithBucket(uri.s3Bucket().c_str());
        request.WithKey(uri.s3Key().c_str());
        auto head_outcome = client.HeadObject(request);
        if (head_outcome.IsSuccess()) {
            auto& result = head_outcome.GetResult();

            // there's a ton of options, https://docs.aws.amazon.com/cli/latest/reference/s3api/head-object.html
            // just serialize as json out a couple
            stringstream ss;

            ss<<"{";
            ss<<"\"LastModified\":"<<chronoToISO8601(result.GetLastModified().UnderlyingTimestamp())<<","
              <<"\"ContentLength\":"<<result.GetContentLength()<<","
              <<"\"VersionId\":"<<result.GetVersionId().c_str()<<","
              <<"\"ContentType\":"<<result.GetContentType().c_str();
            ss<<"}";

            return ss.str();
        } else {
            if(os_err) {
                *os_err<<"HeadObject Request failed with HTTP code "
                       <<static_cast<int>(head_outcome.GetError().GetResponseCode())
                       <<", details: "
                       <<head_outcome.GetError().GetMessage().c_str();
            }
        }

        return meta_data;
    }

    size_t s3GetContentLength(Aws::S3::S3Client const& client, const URI& uri, std::ostream *os_err) {
        using namespace std;
        string meta_data;

        assert(uri.prefix() == "s3://");

        size_t content_length = 0;

        // perform request
        Aws::S3::Model::HeadObjectRequest request;
        request.WithBucket(uri.s3Bucket().c_str());
        request.WithKey(uri.s3Key().c_str());
        auto head_outcome = client.HeadObject(request);
        if (head_outcome.IsSuccess()) {
            auto& result = head_outcome.GetResult();
            content_length = result.GetContentLength();
        } else {
            if(os_err) {
                *os_err<<"HeadObject Request failed with HTTP code "
                       <<static_cast<int>(head_outcome.GetError().GetResponseCode())
                       <<", details: "
                       <<head_outcome.GetError().GetMessage().c_str();
            }
        }

        return content_length;
    }

}

#endif