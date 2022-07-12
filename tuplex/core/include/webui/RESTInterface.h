//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef CURL_TEST_RESTINTERFACE_H
#define CURL_TEST_RESTINTERFACE_H

#include <curl/curl.h>
#include <cstdlib>
#include <string>
#include <functional>


class RESTInterface {
private:
    static const size_t defaultChunkSize = 1024; // allocate 1KB per default for response

    struct MemoryChunk {
        uint8_t* memory;
        size_t   size;
    };

    MemoryChunk _chunk;
    curl_slist *_jsonSList;
    long _statusCode;

    // used by curl to write contents from request
    static size_t writeCallback(void *contents, size_t size, size_t nmemb, void *userp);

    CURL* getCurlHandle();

public:
    RESTInterface();

    ~RESTInterface();

    /*!
     * make a post request with using JSON MIMEType.
     * @param url where to post
     * @param content JSON encoded string
     * @param errCallback optional callback in case the request fails
     * @return answer from the endpoint, empty string if request fails
     */
    std::string postJSON(const std::string &url,
                         const std::string &content, std::function<void(const std::string &)> errCallback = [](
            const std::string &curlErrMessage) {});

    /*!
     * make a get request
     * @param url where to make the request
     * @param errCallback optional callback in case the request fails
     * @return answer from the endpoint or empty string if request fails
     */
    std::string get(const std::string& url, std::function<void(const std::string &)> errCallback=[](
            const std::string &curlErrMessage) {});


    static void init();
    static void shutdown();
};


#endif //CURL_TEST_RESTINTERFACE_H