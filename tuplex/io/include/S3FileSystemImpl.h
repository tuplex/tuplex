//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_S3FILESYSTEMIMPL_H
#define TUPLEX_S3FILESYSTEMIMPL_H
#ifdef BUILD_WITH_AWS


#include <aws/s3/S3Client.h>
#include <aws/transfer/TransferHandle.h>
#include <aws/transfer/TransferManager.h>
#include <aws/core/utils/threading/Executor.h>
#include "IFileSystemImpl.h"

#include <Utils.h>

namespace tuplex {
    class S3FileSystemImpl : public IFileSystemImpl {
        friend class S3File;
    public:
        S3FileSystemImpl() = delete;
        S3FileSystemImpl(const std::string& access_key, const std::string& secret_key, const std::string& session_token,
                         const std::string& region, const NetworkSettings& ns, bool lambdaMode, bool requesterPay);

        Aws::S3::S3Client const& client() const { assert(_client); return *_client.get(); }

        // fetch stats
        void resetCounters();
        size_t numPuts() const { return _putRequests; }
        size_t numGets() const { return _getRequests; }
        size_t numMultipart() const { return _initMultiPartUploadRequests + _multiPartPutRequests + _closeMultiPartUploadRequests; }
        size_t numLs() const { return _lsRequests; }
        size_t bytesTransferred() const { return _bytesTransferred; }
        size_t bytesReceived() const { return _bytesReceived; }


        bool walkPattern(const URI& pattern, std::function<bool(void*, const URI&, size_t)> callback, void* userData=nullptr) override;

        // helper functions to deal with S3
        std::shared_ptr<Aws::Transfer::TransferHandle> uploadFile(const std::string& local_path, const URI& s3_uri, const std::string& content_type);
        std::shared_ptr<Aws::Transfer::TransferHandle> downloadFile(const URI& s3_uri, const std::string& local_path);

        /*!
         * return all paths which match the prefix (can have * or ?). This is a less restrictive version than
         * the globall function
         * @param prefix
         * @return vector of matching paths
         */
        std::vector<URI> lsPrefix(const URI& prefix);

        bool copySingleFileWithinS3(const URI& s3_src, const URI& s3_dest);

    private:
        std::shared_ptr<Aws::S3::S3Client> _client;
        Aws::S3::Model::RequestPayer _requestPayer;

        // to compute pricing, use https://calculator.s3.amazonaws.com/index.html
        // counters, practical for price estimation
        std::atomic<size_t> _putRequests;
         std::atomic<size_t> _initMultiPartUploadRequests;
         std::atomic<size_t> _multiPartPutRequests;
         std::atomic<size_t> _closeMultiPartUploadRequests;
         std::atomic<size_t> _getRequests;
         std::atomic<size_t> _bytesTransferred;
         std::atomic<size_t> _bytesReceived;
         std::atomic<size_t> _lsRequests;

        // transfer manager uses a threadpool, simply use here a pool for some additional threads.
        // Note: this design might be not that great together with the executor threadpool!
        // @TOOD: refactor threadpool to work better!
        std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor> _thread_pool;
        std::shared_ptr<Aws::Transfer::TransferManager> _transfer_manager;

        void initTransferThreadPool(size_t numThreads = 4);

    protected:
        VirtualFileSystemStatus create_dir(const URI& uri) override;
        VirtualFileSystemStatus remove(const URI& uri) override;

        // how to design this? better with smart pointer? --> probably
        // make VirtualFile an abstract class then...
        std::unique_ptr<VirtualFile> open_file(const URI& uri, VirtualFileMode vfm) override;
        VirtualFileSystemStatus touch(const URI& uri, bool overwrite=false) override;
        VirtualFileSystemStatus file_size(const URI& uri, uint64_t& size) override;
        VirtualFileSystemStatus ls(const URI& parent, std::vector<URI>& uris) override;
        std::unique_ptr<VirtualMappedFile> map_file(const URI &uri) override;
        std::vector<URI> glob(const std::string& pattern) override;
    };

    /*!
     * retrieves meta-data about bucket. Returns empty string if not found or request fails.
     * @param client on which S3 clien to run request
     * @param uri uri
     * @param os_err optional output stream where to log errors.
     * @return string containing JSON meta-data or empty string for failure
     */
    extern std::string s3GetHeadObject(Aws::S3::S3Client const& client, const URI& uri, std::ostream *os_err=nullptr);


    extern size_t s3GetContentLength(Aws::S3::S3Client const& client, const URI& uri, std::ostream *os_err=nullptr);
}


#endif
#endif //TUPLEX_S3FILESYSTEMIMPL_H