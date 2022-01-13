//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_VIRTUALFILESYSTEM_H
#define TUPLEX_VIRTUALFILESYSTEM_H

#include <VirtualFile.h>
#include <VirtualMappedFile.h>
#include <IFileSystemImpl.h>
#include "URI.h"
#include "IFileSystemImpl.h"
#include "VirtualFileSystemBase.h"
#include <memory>
#include <vector>
#include <map>
#include <functional>
#include <Utils.h>

#ifdef BUILD_WITH_AWS
#include <aws/transfer/TransferHandle.h>
#endif

namespace tuplex {

    class VirtualFile;
    class VirtualMappedFile;
    class VirtualFileSystem;
    class IFileSystemImpl;

#ifdef BUILD_WITH_AWS
    class S3FileSystemImpl;
#endif

    /*!
     * wrapper class around file systems. Provides interface to easily add new file adapters.
     */
    class VirtualFileSystem {
    public:
        friend class VirtualFile;

        /*!
         * allows for easy extension of the Framework with custom file system. Simply subclass the
         * @param impl pointer to implementation.
         * @param uriPrefix a prefix to associate with the filesystem.
         * @return VFS_PREFIXALREADYREGISTERED if prefix is already in use or VFS_OK
         */
        static VirtualFileSystemStatus registerFileSystem(std::shared_ptr<IFileSystemImpl> impl, const std::string& uriPrefix);


#ifdef BUILD_WITH_AWS
        /*!
         * add S3 file system, must be called after AWSSDK was initialized
         * @param access_key  AWS_ACCESS_KEY
         * @param secret_key  AWS_SECRET_ACCESS_KET
         * @param session_token AWS_SESSION_TOKEN, used together with temporary credentials (as provided via a role)
         * @param region AWS_REGION, e.g. us-east-1
         * @param ns helper struct holding various network settings
         * @param lambdaMode whether called on Lambda runner or not
         * @param requesterPay whether to enable request Pay (i.e., this is a per query field - enable here globally)
         * @return status of adding filesystem
         */
        static VirtualFileSystemStatus addS3FileSystem(const std::string& access_key="",
                                                       const std::string& secret_key="",
                                                       const std::string& session_token="",
                                                       const std::string& region="",
                                                       const NetworkSettings& ns=NetworkSettings(),
                                                       bool lambdaMode=false,
                                                       bool requesterPay=false);

        /*!
         * removes S3 file system if it exists.
         */
        static void removeS3FileSystem();

        /*!
         * helper function to get the S3 file system impl.
         * @return nullptr if not registered, else the implementation.
         */
        static S3FileSystemImpl* getS3FileSystemImpl();

        /*!
         * returns key/value store with transfer statistics for S3 system. Empty if no S3 system was added.
         * @return
         */
        static std::map<std::string, size_t> s3TransferStats();

        /*!
         * reset S3 file system stats to init state
         * @return
         */
        static void s3ResetCounters();

        /*!
         * uploads a local file to an S3 bucket. Helpful for certain operations
         * @param local_path
         * @param s3_uri
         * @param content_type
         * @return handle
         */
        static std::shared_ptr<Aws::Transfer::TransferHandle> s3UploadFile(const std::string& local_path, const URI& s3_uri, const std::string& content_type);

        /*!
         * download s3 file from bucket to local file path.
         * @param s3_uri
         * @param local_path
         * @return handle
         */
        static std::shared_ptr<Aws::Transfer::TransferHandle> s3DownloadFile(const URI& s3_uri, const std::string& local_path);
#endif
        /*!
         * retrives the file system corresponding to a URI
         * @param uri
         * @return FileSystem corresponding to the URI. Throws Exception if file system was not found.
         */
        static VirtualFileSystem fromURI(const URI& uri);

        //!
        //! retrieves individual paths according to a pattern.
        //! Different filesystems may be mixed. I.e.
        //! hdfs:///data/test/*/*.csv, /data/*.csv is a legal pattern. Separate with commas paths
        //! @param pattern pattern to search for
        //! @return vector of URIs to files listed
        //!
        static std::vector<URI> globAll(const std::string& pattern);

        /*!
         * retrieves individual paths according to a pattern for specific filesystem.
         * @param pattern pattern to search for
         * @return vector of URIs to files listed
         */
        std::vector<URI> glob(const std::string& pattern);

        /*!
         * creates a directory as specified by the uri.
         * @param uri
         * @return VFS_IOERROR if directory could not be created
         */
        VirtualFileSystemStatus create_dir(const URI& uri) const;


        /*!
         * removes URI from file system. Can be a directory or a file.
         * @param uri
         * @return VFS_IOERROR if directory does not exist.
         */
        static VirtualFileSystemStatus remove(const URI& uri);

        /*!
         * creates empty file at URI
         * @param uri
         * @return
         */
        VirtualFileSystemStatus touch(const URI& uri) const;

        /*!
         * retrieves size in bytes of file
         * @param uri
         * @param size
         * @return VFS
         */
        VirtualFileSystemStatus file_size(const URI& uri, uint64_t& size) const;

        /*!
         * lists all files and directories for URI (no recursion)
         * @param parent
         * @param uris
         * @return status code
         */
        VirtualFileSystemStatus ls(const URI& parent, std::vector<URI>& uris) const;


        /*!
         * copies all files matching the src_pattern to the target URI
         * @param src_pattern
         * @param target a URI where to store the data
         * @return status code
         */
        static VirtualFileSystemStatus copy(const std::string& src_pattern, const URI& target);

        /*!
         * opens a file and returns a file handle for that
         * @param uri
         * @param vfm filemode (read, write, ...)
         * @return valid ptr if open operation succeeded, else nullptr
         */
        static std::unique_ptr<VirtualFile> open_file(const URI& uri, VirtualFileMode vfm);

        /*!
         * memory maps a file and returns a file handle for it in case mapping succeeded.
         * Implementation may default to a backup when regular IO routines are used.
         * @param uri location of file
         * @return valid ptr if open operation succeeded, else nullptr
         */
        static std::unique_ptr<VirtualMappedFile> map_file(const URI& uri);


        /*!
        * when submitting a pattern, this operation walks the pattern calling the callback when a file or dir was found
        * @param pattern pattern to walk
        * @param callback function to call when a file or directory was found. Signature is bool callback(void* userData, const URI& uri, size_t fileSize) return true to continue walking or false to abandon.
        * @param userData optional userData to be submitted to provide a context to the callback
        * @return whether pattern was exhausted or prematurely abandonded
        */
        static bool walkPattern(const URI& pattern, std::function<bool(void*, const URI&, size_t)> callback, void* userData=nullptr);

    private:
        VirtualFileSystem() : _impl(nullptr)    {}
        IFileSystemImpl *_impl;


        static int copyLocalToLocal(const std::vector<std::string>& src_uris, const URI& target, const std::string& lcp, std::vector<URI>& copied_uris, bool overwrite);

#ifdef BUILD_WITH_AWS
        static int copyLocalToS3(const std::vector<std::string>& src_uris, const URI& target, const std::string& lcp, std::vector<URI>& copied_uris, bool overwrite);
#endif
    };

    //! shortcut for lazy programmers
    using VFS = VirtualFileSystem;

    /*!
     * quick helper function to save string contents to file
     * @param uri where to save
     * @param content content to write
     */
    extern void stringToFile(const URI& uri, const std::string content);

    /*!
     * quick helper to load file contents
     * @param uri for file location
     * @return string with file contents
     */
    extern std::string fileToString(const URI& uri);

    /*!
     * validate file output URI
     * @param baseURI path where to save data to
     * @return false if output can't be written to baseURI
     */
    extern bool validateOutputSpecification(const URI& baseURI);
}

#endif //TUPLEX_VIRTUALFILESYSTEM_H