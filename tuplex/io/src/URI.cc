//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <URI.h>
#include <Utils.h>
#include <Logger.h>
#include <str_const.h>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>

#include "VirtualFileSystem.h"
#include "S3FileSystemImpl.h"

namespace tuplex {

    const URI URI::INVALID = URI();


    URI::URI(const tuplex::URI &other) : _type(other._type), _uri(other._uri) {}
    URI::URI(tuplex::URI &&other) : _type(std::move(other._type)), _uri(std::move(other._uri)) {}
    URI& URI::operator=(const tuplex::URI &other) {
        _type = other._type;
        _uri = other._uri;
        return *this;
    }

    URI::URI(const std::string &path) {
        MessageHandler& logger = Logger::instance().logger("filesystem");

        if(path.length() == 0) {
            _type = URIType::INVALID;
            _uri = "";
            return;
        }

        // hdfs paths are identified using hdfs://
        // s3 paths using s3://, http://, https://
        // else, it is a local path
        if(startsWith(path, "hdfs://")) {
            logger.error("HDFS not yet supported");
            _type = URIType::HDFS;
            _uri = "";
        } else if(startsWith(path, "s3://")) {
            // || startsWith(path, "http://") || startsWith(path, "https://")
            // note: http:// and https:// should be later also allowed
            // to be some web resources
            // s3 paths can however be accessed via http syntax as
            // described in https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html#access-bucket-intro
            // basically normalize s3 path here to
            // s3:// form.
            // check for s3 and amazonaws.com in url...
            _type = URIType::S3;
            _uri = path;
        } else if(startsWith(path, "file://")) {
            _uri = path;
            _type = URIType::LOCAL;
        } else {
            // treat as local path
            //logger.warn("treating path '" + path + "' as local filesystem path. Consider prefixing the path with file:// to avoid ambiguity.");
            _uri = "file://" + path;
            _type = URIType::LOCAL;
        }

        // expand ~ for local paths
        if(_type == URIType::LOCAL) {
            _uri = expandURI(_uri);
        }
    }

    bool URI::isLocal() const {
        return _type == URIType::LOCAL;
    }

    std::string URI::toPath() const {
        MessageHandler& logger = Logger::instance().logger("filesystem");

        if(_uri.length() == 0)
            return "";

        // return absolute local path
        switch(_type) {
            case URIType::LOCAL: {
                boost::filesystem::path path(_uri.substr(str_constant("file://").length()));
                // canonical will remove symlinks etc.
                // this is buggy
                auto weakly_normal = boost::filesystem::weakly_canonical(path).string();
                // does path end with /.? => replace with /! it's a folder then ...
                if(weakly_normal.size() >= 2 && weakly_normal[weakly_normal.size() -1] == '.' && weakly_normal[weakly_normal.size() - 2] == '/')
                    weakly_normal = weakly_normal.substr(0, weakly_normal.length() - 1);

                // check if weakly normal starts with /, if not use current working directory and append path!
                if(weakly_normal.front() != '/')
                    return current_working_directory() + "/" + _uri.substr(str_constant("file://").length());

                return weakly_normal;
            }
            case URIType::S3: {
                // @TODO: normalize if it is http:// or https://
                return _uri;
            }
            default:
#ifndef NDEBUG
                logger.warn("unsupported URI type found (" + _uri + "). Can't create absolute path.");
#endif
                return "";
        }
    }

    bool URI::isFile() const {
        MessageHandler& logger = Logger::instance().logger("filesystem");

        switch(_type) {
            case URIType::LOCAL: {
                auto path = toPath();
                return boost::filesystem::is_regular_file(path);
            }
            default:
                logger.error("unsupported URI type found. Can't determine whether it specifies a valid file.");
                return false;
        }
    }

    bool URI::exists() const {
        MessageHandler& logger = Logger::instance().logger("filesystem");

        switch(_type) {
            case URIType::LOCAL: {
                auto path = toPath();
                return boost::filesystem::exists(path);
            }
#ifdef BUILD_WITH_AWS
            case URIType::S3: {
                // check meta data
                auto s3fs = VirtualFileSystem::getS3FileSystemImpl();
                if(!s3fs)
                    return false;

                auto response = s3GetHeadObject(s3fs->client(), *this);
                return !response.empty();
            }
#endif
            default:
                logger.error("unsupported URI type found. Can't determine whether file or directory exists.");
                return false;
        }
    }

    URI& URI::operator = (const std::string& s) {
        URI tmp(s);
        this->_uri = tmp._uri;
        this->_type = tmp._type;
        return *this;
    }

    URI URI::searchPaths(const std::string &filename, std::vector<URI> &paths) {

        // make sure no '/' is within filename
        assert(filename.find("/") == std::string::npos);

        // go through all paths and search
        for(auto path : paths) {
            auto p = path.toPath();
            assert(p.length() > 0);
            if(p.at(p.length() - 1) != '/')
                p += "/";
            URI u(p + filename);
            if(u.exists())
                return u;
        }

        return URI::INVALID;
    }

    URI URI::join_path(const std::string &path) const {
        // from https://github.com/TileDB-Inc/TileDB/blob/dev/tiledb/sm/misc/uri.cc
        // Check for empty strings.
        if (path.empty()) {
            return URI(_uri);
        } else if (_uri.empty()) {
            return URI(path);
        }

        if (_uri.back() == '/') {
            if (path.front() == '/') {
                return URI(_uri + path.substr(1, path.size()));
            }
            return URI(_uri + path);
        } else {
            if (path.front() == '/') {
                return URI(_uri + path);
            } else {
                return URI(_uri + "/" + path);
            }
        }
    }

    std::string URI::prefix() const {

        std::string prefixLocator = "://";
        // check for prefix
        auto pos = _uri.find(prefixLocator);
        if(pos != std::string::npos)
            return _uri.substr(0, pos + prefixLocator.length());

        // return empty string
        return "";
    }

    std::string URI::expandURI(const std::string &uri) {
        // replace tilde (and in the future other variables with env)
        // check https://www.dreamincode.net/forums/topic/197372-how-to-expand-a-path-linux/
        // there should be https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man3/wordexp.3.html and
        // https://man7.org/linux/man-pages/man3/wordexp.3.html
        // https://cpp.hotexamples.com/examples/-/-/wordexp/cpp-wordexp-function-examples.html

        // simply replace ~ with home dir
        // replace ~ with current user's home directory or ~user with user's home directory

        // use regex

        // @TODO: implement this...
#warning "implement this here..."
        return uri;
    }

    bool decodeRangeURI(const std::string& uri, URI& target, size_t& rangeStart, size_t& rangeEnd) {
        auto ridx = uri.rfind(':');
        auto prefix_idx = uri.rfind("://");
        if(ridx != std::string::npos && (prefix_idx != std::string::npos && ridx > prefix_idx)) {
            // range present...
            auto target_str = uri.substr(0, ridx);
            auto midx = uri.rfind('-');
            if(midx == std::string::npos || midx <= ridx)
                return false;
            auto start_str = uri.substr(ridx + 1, midx - ridx - 1);
            auto end_str = uri.substr(midx + 1);
            rangeStart = std::stoul(start_str);
            rangeEnd = std::stoul(end_str);
            target = URI(target_str);
        } else {
            rangeStart = 0;
            rangeEnd = 0;
            target = URI(uri);
        }
        return true;
    }

    std::string encodeRangeURI(const URI& uri, size_t rangeStart, size_t rangeEnd) {
        if(0 == rangeStart && 0 == rangeEnd)
            return uri.toString();

        return uri.toString() + ":" + std::to_string(rangeStart) + "-" + std::to_string(rangeEnd);
    }
}



// S3 specific functions
#ifdef BUILD_WITH_AWS
namespace tuplex {
    std::string URI::s3Bucket() const {
        // validate & throw exception if not valid s3 path
        if(_uri.substr(0, 5) != "s3://")
            throw std::runtime_error("S3 path " + _uri + " must start with s3:// " + "started with ");
        auto idx = _uri.substr(5).find_first_of('/');
        return _uri.substr(5, idx);
    }

    std::string URI::s3Key() const {
    // validate & throw exception if not valid s3 path
        if(_uri.substr(0, 5) != "s3://")
            throw std::runtime_error("S3 path " + _uri + " must start with s3:// " + "started with ");
        auto idx = _uri.substr(5).find_first_of('/');
        if(std::string::npos == idx)
            return "";
        return _uri.substr(5 + idx + 1);
    }

    std::string URI::s3GetMIMEType() const {
        // auto infer from uri mime-type
        // else return ''
        if(strEndsWith(_uri, ".csv"))
            return "text/csv";

        return "";
    }

    URI tuplex::URI::fromS3(const std::string &bucket, const std::string &key) {
        return URI("s3://" + bucket + "/" + key);
    }
}
#endif