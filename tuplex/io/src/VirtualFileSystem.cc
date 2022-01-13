//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <IFileSystemImpl.h>
#include <VirtualFileSystem.h>
#include <memory>
#include <unordered_map>
#include <cassert>
#include <MessageHandler.h>
#include <Logger.h>
#include <PosixFileSystemImpl.h>
#ifdef BUILD_WITH_AWS
    #include <S3FileSystemImpl.h>
#endif
#include <string>
#include <StringUtils.h>
#include "MimeType.h"

namespace tuplex {

    /*!
     * registers some default file systems (local, error)
     * @return
     */
    static std::unordered_map<std::string, std::shared_ptr<IFileSystemImpl>> defaults() {
        std::unordered_map<std::string, std::shared_ptr<IFileSystemImpl>> fs;

        // init AWS SDK?
        auto localFS = std::make_shared<PosixFileSystemImpl>();
        fs["file://"] = localFS;
        fs[""] = localFS; // no prefix defaults to local paths
        return fs;
    }

    // init with local file system per default
    static std::unordered_map<std::string, std::shared_ptr<IFileSystemImpl>> fsRegistry = defaults();

#ifdef BUILD_WITH_AWS
    VirtualFileSystemStatus VirtualFileSystem::addS3FileSystem(const std::string& access_key, const std::string& secret_key, const std::string& session_token, const std::string& region, const NetworkSettings& ns, bool lambdaMode, bool requesterPay) {
        return VirtualFileSystem::registerFileSystem(std::make_shared<S3FileSystemImpl>(access_key, secret_key, session_token, region, ns, lambdaMode, requesterPay), "s3://");
    }

    void VirtualFileSystem::removeS3FileSystem() {
        auto it = fsRegistry.find("s3://");
        if(it != fsRegistry.end()) {
            fsRegistry.erase(it);
        }
    }

    std::map<std::string, size_t> VirtualFileSystem::s3TransferStats() {
        MessageHandler& logger = Logger::instance().logger("filesystem");
        std::map<std::string, size_t> m;

        if(fsRegistry.find("s3://") != fsRegistry.end()) {
            auto s3fs = dynamic_cast<S3FileSystemImpl*>(fsRegistry["s3://"].get());

            if(!s3fs) {
                logger.warn("under s3:// a system not called S3FileSystemImpl is registered. Can't retrieve stats");
                return m;
            }

            // fill up values
            m["put"] = s3fs->numPuts();
            m["get"] = s3fs->numGets();
            m["ls"] = s3fs->numLs();
            m["multipart"] = s3fs->numMultipart();
            m["transferred"] = s3fs->bytesTransferred();
            m["received"] = s3fs->bytesReceived();

        } else logger.warn("calling S3 stats, but no system registered under s3://");

        return m;
    }

    void VirtualFileSystem::s3ResetCounters() {
        MessageHandler& logger = Logger::instance().logger("filesystem");

        if(fsRegistry.find("s3://") != fsRegistry.end()) {
            auto s3fs = dynamic_cast<S3FileSystemImpl*>(fsRegistry["s3://"].get());

            if(!s3fs) {
                logger.warn("under s3:// a system not called S3FileSystemImpl is registered. Can't retrieve stats");
                return;
            }

            s3fs->resetCounters();

        } else logger.warn("calling S3 resetCounters, but no system registered under s3://");
    }

    std::shared_ptr<Aws::Transfer::TransferHandle> VirtualFileSystem::s3DownloadFile(const URI &s3_uri, const std::string &local_path) {
        MessageHandler& logger = Logger::instance().logger("filesystem");

        if(fsRegistry.find("s3://") != fsRegistry.end()) {
            auto s3fs = dynamic_cast<S3FileSystemImpl*>(fsRegistry["s3://"].get());

            if(!s3fs) {
                logger.warn("under s3:// a system not called S3FileSystemImpl is registered. Can't download file " + s3_uri.toPath() + " to " + local_path);
                return nullptr;
            }

            return s3fs->downloadFile(s3_uri, local_path);
        } else logger.warn("calling S3 downloadFile, but no system registered under s3://");

        return nullptr;
    }

    // content type is MIME type https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.17
    std::shared_ptr<Aws::Transfer::TransferHandle> VirtualFileSystem::s3UploadFile(const std::string &local_path, const URI &s3_uri,
                                        const std::string &content_type) {
        MessageHandler& logger = Logger::instance().logger("filesystem");

        if(fsRegistry.find("s3://") != fsRegistry.end()) {
            auto s3fs = dynamic_cast<S3FileSystemImpl*>(fsRegistry["s3://"].get());

            if(!s3fs) {
                logger.warn("under s3:// a system not called S3FileSystemImpl is registered. Can't upload file " + local_path + " to " + s3_uri.toPath());
                return nullptr;
            }

            return s3fs->uploadFile(local_path, s3_uri, content_type);
        } else logger.warn("calling S3 uploadFile, but no system registered under s3://");

        return nullptr;
    }

#endif

    static std::shared_ptr<IFileSystemImpl> getFileSystemImpl(const URI& uri) {
        auto prefix = uri.prefix();

        // check if prefix is in registry, else return error file system and issue warning
        if(fsRegistry.find(prefix) == fsRegistry.end()) {
            return nullptr;
        } else {
            return fsRegistry[prefix];
        }
    }

    VirtualFileSystemStatus VirtualFileSystem::registerFileSystem(std::shared_ptr<IFileSystemImpl> impl,
                                                                  const std::string &uriPrefix) {
        MessageHandler& logger = Logger::instance().logger("filesystem");
        assert(impl);

        // check that prefix is not yet registered
        if(fsRegistry.find(uriPrefix) != fsRegistry.end()) {
            logger.debug("filesystem already registered under prefix '" + uriPrefix + "'");
            return VirtualFileSystemStatus::VFS_PREFIXALREADYREGISTERED;
        }

        fsRegistry[uriPrefix] = impl;
        return VirtualFileSystemStatus::VFS_OK;
    }

    VirtualFileSystem VirtualFileSystem::fromURI(const URI &uri) {
        MessageHandler& logger = Logger::instance().logger("filesystem");
        auto prefix = uri.prefix();

        VirtualFileSystem vfs;

        // check if prefix is in registry, else return error file system and issue warning
        if(fsRegistry.find(prefix) == fsRegistry.end()) {
            logger.error("could not find filesystem for URI prefix '" + prefix + "'");
            vfs._impl = nullptr;
        } else {
            vfs._impl = fsRegistry[prefix].get();
        }
        return vfs;
    }

    VirtualFileSystemStatus VirtualFileSystem::create_dir(const URI &uri) const {
        // check impl is valid, else return invalid
        if(!_impl)
            return VirtualFileSystemStatus::VFS_NOFILESYSTEM;

        // call implementation
        return _impl->create_dir(uri);
    }

    VirtualFileSystemStatus VirtualFileSystem::remove(const URI &uri) {
        auto impl = VirtualFileSystem::fromURI(uri)._impl;
        if(!impl)
            return VirtualFileSystemStatus::VFS_NOFILESYSTEM;

        // call implementation
        return impl->remove(uri);
    }


    std::unique_ptr<VirtualFile> VirtualFileSystem::open_file(const URI &uri, VirtualFileMode vfm) {
        auto impl = getFileSystemImpl(uri);
        return impl ? impl->open_file(uri, vfm) : nullptr;
    }

    std::unique_ptr<VirtualMappedFile> VirtualFileSystem::map_file(const URI &uri) {
        auto impl = getFileSystemImpl(uri);
        return impl ? impl->map_file(uri) : nullptr;
    }

    VirtualFileSystemStatus VirtualFileSystem::file_size(const URI &uri, uint64_t &size) const {
        if(!_impl)
            return VirtualFileSystemStatus::VFS_NOFILESYSTEM;

        return _impl->file_size(uri, size);
    }

    std::vector<URI> VirtualFileSystem::globAll(const std::string &pattern) {
        std::vector<URI> files;
        // step 1: separate pattern after ,
        // then for each subpattern, find corresponding filesystem & glob results
        splitString(pattern, ',', [&](std::string pattern) {

            // trim whitespace
            trim(pattern);

           // call filesystems glob
           auto uri = URI(pattern);
           auto vfs = fromURI(uri);
           auto subfiles = vfs.glob(uri.toString());
           files.insert(std::end(files), std::begin(subfiles), std::end(subfiles));
        });

        return files;
    }


    bool VirtualFileSystem::walkPattern(const tuplex::URI &pattern,
                                        std::function<bool(void *, const tuplex::URI &, size_t)> callback,
                                        void *userData) {

        auto v = splitToArray(pattern.toString(), ',');
        // trim all strings with the array
        for(auto& s: v) {
            trim(s);
        }
        // normalize paths...
        for(int i = 0; i < v.size(); ++i)
            v[i] = URI(v[i]).toPath();

        // go through patterns & call walkPattern of impl
        for(const auto& pattern : v) {
            // get filesystem
            auto vfs = fromURI(URI(pattern));

            if(!vfs._impl)
                throw std::runtime_error("could not find file system for prefix " + URI(pattern).prefix());

            if(!vfs._impl->walkPattern(pattern, callback, userData))
                return false;
        }

        return true;
    }

    std::vector<URI> VirtualFileSystem::glob(const std::string &pattern) {
        if(!_impl)
            return std::vector<URI>();

        return _impl->glob(pattern);
    }


    int VirtualFileSystem::copyLocalToLocal(const std::vector<std::string> &src_uris, const URI &target,
                                            const std::string &lcp, std::vector<URI> &copied_uris, bool overwrite) {
        using namespace std;
        auto& logger = Logger::instance().logger("filesystem");
        int rc = 0;
        // simple copy via posix primitives...
        // need to be careful regarding folder structure though...
        for(auto src : src_uris) {
            auto src_without_prefix = URI(src).withoutPrefix();
            URI target_uri = target.join_path(src.substr(lcp.size()));

            // when single file, just overwrite whatever the target is
            // unless it's a separator because then copy into folder is desired...
            if(src_uris.size() == 1 && target.toString().back() != '/') {
                target_uri = target;
            }

            // single file or folder?
            if(URI(src).isFile()) {
                logger.debug("copying local file " + src_without_prefix + " to "
                             + target_uri.toPath());
                // overwrite per default
                if(VirtualFileSystemStatus::VFS_OK != PosixFileSystemImpl::copySingleFile(src, target_uri, overwrite))
                    return -1;
                copied_uris.push_back(target_uri);
            } else {
                // folder!
                // expand URI
                auto expanded_src_uris = PosixFileSystemImpl::expandFolder(src);
                for(auto uri : expanded_src_uris) {
                    target_uri = target.join_path(uri.withoutPrefix().substr(lcp.size()));
                    logger.debug("copying local file " + uri.withoutPrefix() + " to "
                                 + target_uri.toPath());
                    // overwrite per default
                    if(VirtualFileSystemStatus::VFS_OK != PosixFileSystemImpl::copySingleFile(uri, target_uri, overwrite))
                        return -1;
                    copied_uris.push_back(target_uri);
                }
            }
        }

        return rc;
    }

#ifdef BUILD_WITH_AWS

    S3FileSystemImpl* VirtualFileSystem::getS3FileSystemImpl() {
        // check whether s3 is registered
        auto it = fsRegistry.find("s3://");
        if(it == fsRegistry.end())
            return nullptr;
        return static_cast<S3FileSystemImpl*>(it->second.get());
    }


    int VirtualFileSystem::copyLocalToS3(const std::vector<std::string> &src_uris, const URI &target,
                                            const std::string &lcp, std::vector<URI> &copied_uris, bool overwrite) {
        using namespace std;
        auto& logger = Logger::instance().logger("filesystem");
        int rc = 0;
        // simple copy via posix primitives...
        // need to be careful regarding folder structure though...
        for(auto src : src_uris) {
            auto src_without_prefix = URI(src).withoutPrefix();
            URI target_uri = target.join_path(src.substr(lcp.size()));

            // when single file, just overwrite whatever the target is
            // unless it's a separator because then copy into folder is desired...
            if(src_uris.size() == 1 && target.toString().back() != '/') {
                target_uri = target;
            }

            // single file or folder?
            if(URI(src).isFile()) {
                logger.debug("copying local file " + src_without_prefix + " to "
                             + target_uri.toPath());
                auto content_type = detectMIMEType(URI(src).withoutPrefix());
                assert(!content_type.empty());
                logger.debug("uploading local file " + src_without_prefix + " to "
                             + target_uri.toPath() + " (MIME: "+ content_type + ")");
                auto handle = s3UploadFile(src_without_prefix, target_uri, content_type);
                if(!handle)
                    return -1;
                copied_uris.push_back(target_uri);
            } else {
                // folder!
                // expand URI
                auto expanded_src_uris = PosixFileSystemImpl::expandFolder(src);
                for(auto uri : expanded_src_uris) {
                    target_uri = target.join_path(uri.withoutPrefix().substr(lcp.size()));
                    logger.debug("copying local file " + uri.withoutPrefix() + " to "
                                 + target_uri.toPath());
                    auto content_type = detectMIMEType(URI(uri).withoutPrefix());
                    assert(!content_type.empty());
                    logger.debug("uploading local file " + uri.toString() + " to "
                                 + target_uri.toPath() + " (MIME: "+ content_type + ")");
                    auto handle = s3UploadFile(uri.withoutPrefix(), target_uri, content_type);
                    if(!handle)
                        return -1;
                    copied_uris.push_back(target_uri);
                }
            }
        }

        return rc;
    }
#endif

    // @TODO: add overwrite parameter??
    VirtualFileSystemStatus VirtualFileSystem::copy(const std::string &src_pattern, const URI &target) {
        using namespace std;
        auto& logger = Logger::instance().logger("filesystem");

        // TODO: making VFS async could speed up things a lot!

#ifndef BUILD_WITH_AWS
        if(target.prefix() == "s3://") {
            logger.error("Tuplex version was build without AWS SDK support. Can't process S3 URI " + target.toPath());
            return VirtualFileSystemStatus::VFS_IOERROR;
        }
#endif
        // check whether target is supported
        if(target.prefix() != "s3://" && target.prefix() != "file://" && !target.prefix().empty()) {
            logger.error("unsupported target file system prefix " + target.toPath() + " found, aborting copy operation");
            return VirtualFileSystemStatus::VFS_IOERROR;
        }

        // glob the pattern
        vector<URI> src_uris;

        // copy has a different behavior than the other patterns. Therefore, use for S3 different strategy
        splitString(src_pattern, ',', [&](std::string pattern) {

            // trim whitespace
            trim(pattern);

            // call filesystems glob
            auto uri = URI(pattern);
            auto vfs = fromURI(uri);
            vector<URI> subfiles;
            if(uri.isLocal()) {
                subfiles = vfs.glob(uri.toString());
            } else if(uri.prefix() == "s3://") {
#ifdef BUILD_WITH_AWS
                // use prefix match
                assert(vfs._impl);
                subfiles = ((S3FileSystemImpl*)vfs._impl)->lsPrefix(uri);
#endif
            } else {
                logger.error("unsupported fs prefix " + uri.prefix() + " found, skipping copy.");
            }
            src_uris.insert(std::end(src_uris), std::begin(subfiles), std::end(subfiles));
        });


        // sort after file systems (i.e. prefix)
        unordered_map<string, vector<string>> mapped_uris;
        for(auto uri : src_uris) {
            mapped_uris[uri.prefix()].push_back(uri.toPath());
        }

        vector<URI> copied_uris;

        VirtualFileSystemStatus rc = VirtualFileSystemStatus::VFS_OK;
        // go through groups
        for(auto kv : mapped_uris) {
            assert(!kv.first.empty());
            // find longest shared prefix
            auto lcp = longestCommonPrefix(kv.second);
            // adjust lcp to be ending with /
            lcp = lcp.substr(0, lcp.rfind("/"));

            if(kv.first == "file://") {
                // what is the target?
                if(target.isLocal()) {
                    if(0 != copyLocalToLocal(kv.second, target, lcp, copied_uris, true))
                        goto COPY_FAILURE;
                } else if(target.prefix() == "s3://") {
#ifdef BUILD_WITH_AWS
                    if(0 != copyLocalToS3(kv.second, target, lcp, copied_uris, true))
                        goto COPY_FAILURE;
#endif
                }
            } else if(kv.first == "s3://") {
                if(target.isLocal()) {
                    // download file from S3
                    for(auto src : kv.second) {
                        // create dir for local files
                        URI target_uri = target.join_path(src.substr(lcp.size()));

                        // when single file, just overwrite whatever the target is
                        // unless it's a separator because then copy into folder is desired...
                        if(src_uris.size() == 1 && target.toString().back() != '/') {
                            target_uri = target;
                        }

                        auto vfs = VirtualFileSystem::fromURI(target_uri);
                        rc = vfs.create_dir(target_uri.parent());
                        if(rc != VirtualFileSystemStatus::VFS_OK)
                            goto COPY_FAILURE;
#ifdef BUILD_WITH_AWS
                        auto handle = s3DownloadFile(src, target_uri.withoutPrefix());
                        if(!handle) {
                            rc = VirtualFileSystemStatus::VFS_IOERROR;
                            goto COPY_FAILURE;
                        }
                        //s3UploadFile(src_without_prefix, target_uri, content_type);
#endif
                        copied_uris.push_back(target_uri);
                    }
                } else if(target.prefix() == "s3://") {
#ifdef BUILD_WITH_AWS
                    // copy s3 to s3
                    // -> there's a request for this!
                    // https://docs.aws.amazon.com/code-samples/latest/catalog/cpp-s3-copy_object.cpp.html

                    // issue copy requests
                    // --> need to perform one for each file!
                    // -> src uris have been expanded from S3 already, thus simply iterating will work.
                    // only the target scenario needs to be figured out
                    for(auto src : kv.second) {
                        // create dir for local files
                        URI target_uri = target.join_path(src.substr(lcp.size()));

                        // when single file, just overwrite whatever the target is
                        // unless it's a separator because then copy into folder is desired...
                        if (src_uris.size() == 1 && target.toString().back() != '/') {
                            target_uri = target;
                        }

                        assert(target_uri.prefix() == "s3://");
                        auto vfs = VirtualFileSystem::fromURI(target_uri);
                        assert(vfs._impl);
                        auto s3sys = (S3FileSystemImpl*)vfs._impl;

                        // creation of target dir not neccessary for object store
                        if(!s3sys->copySingleFileWithinS3(src, target_uri))
                            goto COPY_FAILURE;
                        copied_uris.push_back(target_uri);
                    }
#endif
                }
            } else {
                logger.error("unsupported file system prefix " + kv.first + " found, aborting copy operation");
                rc = VirtualFileSystemStatus::VFS_IOERROR;
                goto COPY_FAILURE; // ugly, but best way here...
            }
        }
        return VirtualFileSystemStatus::VFS_OK;

COPY_FAILURE:
        // remove all copied uris (s.t. this operation here becomes atomic)
        for(auto uri : copied_uris)
            VirtualFileSystem::remove(uri);
        return rc;
    }

    void stringToFile(const URI& uri, const std::string content) {
        auto vfs = VirtualFileSystem::fromURI(uri);

        auto file = vfs.open_file(uri, VirtualFileMode::VFS_OVERWRITE);
        if(!file) {
            Logger::instance().defaultLogger().error("could not open file " + uri.toString());
            return;
        }

        file->write(content.c_str(), content.length());
        file->close();
    }

    std::string fileToString(const URI& uri) {
        auto vfs = VirtualFileSystem::fromURI(uri);

        auto file = vfs.open_file(uri, VirtualFileMode::VFS_READ | VirtualFileMode::VFS_TEXTMODE);
        if(!file) {
            Logger::instance().defaultLogger().error("could not open file " + uri.toString());
            return "";
        }

        auto numBytes = file->size();
        std::string s(numBytes + 1, '\0');
        file->read(&s[0], numBytes);
        file->close();
        s.resize(numBytes);
        return s;
    }

    VirtualFileSystemStatus VirtualFileSystem::ls(const URI &parent, std::vector<URI> &uris) const {
        // check if impl exists
        if(!_impl)
            return VirtualFileSystemStatus::VFS_NOFILESYSTEM;

        return _impl->ls(parent, uris);
    }

    bool validateOutputSpecification(const URI& baseURI) {
        using namespace std;
        // validates output specification, i.e. following is accepted:

        // for local filesystem
        // it's a file -> okay
        // it's a dir -> must not exist or be empty
        if(baseURI.isLocal()) {
            auto local_path = baseURI.toPath();
            if(strStartsWith(local_path, baseURI.prefix())) {
                local_path = local_path.substr(baseURI.prefix().length());
            }

            if(fileExists(local_path) && isFile(local_path))
                return true;
            if(dirExists(local_path) && isDirectory(local_path)) {
                vector<URI> uris;
                // empty or non empty?
                auto vfs = VirtualFileSystem::fromURI("file://");
                vfs.ls(local_path, uris);

                // Note: for MacOS, .DS_Store might be ok too.
#ifdef MACOS
                if(1 == uris.size()) {
                    if(strEndsWith(uris.front().toPath(), ".DS_Store") && isFile(uris.front().toPath()))
                        return true;
                }
#endif
                return uris.empty();
            } else {
                // ok, check if writable
               return isWritable(local_path);
            }
        } else {
            // S3: same, using ls function!
#ifdef BUILD_WITH_AWS
            vector<URI> uris;
            // empty or non empty?
            auto vfs = VirtualFileSystem::fromURI(baseURI);
            vfs.ls(baseURI, uris);
            if(uris.empty())
                return true;
            else {
                // single file/dir? => overwrite possible!
                return uris.size() == 1;
            }
#else
            Logger::instance().defaultLogger().warn("Tuplex not compiled with S3 support, can't write to S3 output URI");
            return false;
#endif
            return true;
        }
    }
}
