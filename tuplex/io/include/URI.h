//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_URI_H
#define TUPLEX_URI_H

#include <string>
#include <vector>

namespace tuplex {

    /*!
     * unified resource identifier class to handle file paths for local filesystem, HDFS, S3, ...
     * inspired from tildeb.io code accessable via https://github.com/TileDB-Inc/TileDB/blob/dev/tiledb/sm/misc/uri.h
     */
    class URI {
    public:
        URI() : _type(URIType::INVALID), _uri("") {}

        // copy, move & assign operator
        URI(const URI& other);
        URI(URI&& other);
        URI& operator = (const URI& other);

        enum class URIType {
            INVALID,
            LOCAL,
            HDFS,
            S3
        };

        /*!
         * initializes URI given a path (can be relative, local, HDFS, S3, ...)
         * @param path
         */
        URI(const std::string& path);

        template<std::size_t N> URI(const char(&path)[N]) : URI(std::string(path)) {}

        inline URI join(const std::string& file_name) {
            // ends with '/'?
            if(_uri.empty())
                return URI(file_name);
            if(_uri.back() == '/')
                return URI(_uri + file_name);
            else
                return URI(_uri + "/" + file_name);
        }

        /*!
         * checks whether URI locates a file or not. True if file doesn't exist.
         * @return
         */
        bool isFile() const;

        /*!
         * checks whether URI locates a resource that exists. Necessary check for reading anything.
         * @return
         */
        bool exists() const;

        /*!
         * returns an absolute accessible path
         * @return
         */
        std::string toPath() const;

        /*!
         * returns URI as string
         * @return
         */
        std::string toString() const { return _uri; }

        /*!
         * checks whether URI points to a locally accessible path
         * @return
         */
        bool isLocal() const;

        URI join_path(const std::string& path) const;

        URI& operator = (const std::string& s);

        URIType type() const { return _type; }

        /*!
         * returns prefix of URI, i.e. file://, hdfs://, s3://, ...
         * @return
         */
        std::string prefix() const;

        /*!
         * get non-prefix part of the URI
         * @return string without prefix
         */
        std::string withoutPrefix() const {
            // if it starts with prefix, remove
            if(_uri.rfind(prefix(), 0) == 0) {
                // starts with prefix
                return _uri.substr(prefix().length());
            } else {
                return _uri;
            }
        }

        bool operator == (const URI& other) const {
            if(_type != other._type)
                return false;
            if(toPath().compare(other.toPath()) != 0)
                return false;
            return true;
        }

        bool operator != (const URI& other) const {
            return !(other == *this);
        }

        /*!
         * get parent directory
         * @return
         */
        URI parent() const {
            auto path = _uri;
            auto idx = path.rfind('/');

            // special case: '/' after prefix, i.e. root dir
            if(idx == prefix().length())
                return URI(prefix() + "/");

            // is / last char?
            if(idx == path.length() - 1) {
                while(idx > 0 && path[idx] == '/')
                    idx--;
                idx = path.substr(0, idx).rfind('/');
            }

            // limit to prefix
            if(idx < prefix().length())
                idx = std::string::npos;//prefix().length();

            if(idx != std::string::npos) {
                return URI(path.substr(0, idx));
            } else {
                return URI(prefix() + ".");
            }
        }

        /*!
         * searches in all paths whether a file with the name is available
         * @param filename filename (no subpath!)
         * @param paths list of paths
         * @return uri or invalid uri
         */
        static URI searchPaths(const std::string& filename, std::vector<URI>& paths);

        static const URI INVALID;

#ifdef BUILD_WITH_AWS
        std::string s3Bucket() const;
        std::string s3Key() const;

        std::string s3GetMIMEType() const;

        static URI fromS3(const std::string& bucket, const std::string& key);
#endif
    private:
        std::string _uri;
        URIType _type;

        std::string expandURI(const std::string& uri);
    };


    /*!
     *
     * @param uri string containing uri w. range description
     * @param target to which URI to decode
     * @param rangeStart byte offset
     * @param rangeEnd byte offset
     * @return true if decode was successful, else false
     */
    extern bool decodeRangeURI(const std::string& uri, URI& target, size_t& rangeStart, size_t& rangeEnd);

    /*!
     * encode range into URI
     * @param uri
     * @param rangeStart
     * @param rangeEnd
     * @return string representing uri + ranges
     */
    extern std::string encodeRangeURI(const URI& uri, size_t rangeStart, size_t rangeEnd);

    inline std::ostream& operator << (std::ostream& os, const URI& uri) {
        os<<uri.toString();
        return os;
    }

}

#endif //TUPLEX_URI_H