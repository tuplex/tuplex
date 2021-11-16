//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "RESTInterface.h"
#include <cassert>
#include <Logger.h>


static bool curlInitialized = false;

void RESTInterface::init() {
    if(!curlInitialized)
        // change to include ssl
        curl_global_init(CURL_GLOBAL_NOTHING);
    curlInitialized = true;
}

void RESTInterface::shutdown() {
    if(curlInitialized)
        curl_global_cleanup();
    curlInitialized = false;
}


CURL* RESTInterface::getCurlHandle() {
    CURL *handle = curl_easy_init();

    // send data to this function
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writeCallback);

    // pass argument to callback
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, (void*)&_chunk);

    // to avoid requests being blocked, set default user agent
    curl_easy_setopt(handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");

    // // set timeout to 500ms
    // auto timeout = 500L;
    // curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, timeout);
    // curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT_MS, timeout);
    // curl_easy_setopt(handle, CURLOPT_ACCEPTTIMEOUT_MS, timeout);

    // important to set timeouts, else this will hang forever...
    auto timeout = 2000L; // 2s
    curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, timeout); // request timeout
    curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT_MS, 500L); // connect timeout

    // turn signals off because of multi-threaded context
    // check CurlHandleContainer.cpp in AWS SDK C++ for inspiration
    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L);

#ifndef NDEBUG
    // curl_easy_setopt(_handle, CURLOPT_VERBOSE, 1L);
#endif
    return handle;
}

RESTInterface::RESTInterface()  {

    // require curl lib to be initialized
    assert(curlInitialized);

    // init memory where to write responses to
    _chunk.size = 0;
    _chunk.memory = (uint8_t*)malloc(defaultChunkSize);

    // init json list
    _jsonSList = nullptr;
    _jsonSList = curl_slist_append(_jsonSList, "Content-Type: application/json");

    _statusCode = 0;
}

size_t RESTInterface::writeCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    RESTInterface::MemoryChunk *mem = (RESTInterface::MemoryChunk*)userp;

    uint8_t *ptr = (uint8_t *)realloc(mem->memory, mem->size + realsize + 1);
    if(!ptr) {
        printf("not enough memory (realloc returned NULL)\n");
        return 0;
    }

    mem->memory = ptr;
    memcpy(&(mem->memory[mem->size]), contents, realsize);
    mem->size += realsize;
    mem->memory[mem->size] = 0;

    return realsize;
}

RESTInterface::~RESTInterface() {
    assert(_jsonSList);
    curl_slist_free_all(_jsonSList);

    if(_chunk.memory)
       free(_chunk.memory);
    _chunk.memory = nullptr;
    _chunk.size = 0;
}

std::string RESTInterface::get(const std::string &url,
                               std::function<void(const std::string &)> errCallback) {

    auto handle = getCurlHandle();

    curl_easy_setopt(handle, CURLOPT_URL, url.c_str());

    auto res = curl_easy_perform(handle);

    std::string str;
    if(res != CURLE_OK) {
        errCallback(std::string(curl_easy_strerror(res)));
        return "";
    } else {
        // convert to string and return
        str.reserve(_chunk.size);
        str.assign((char*)_chunk.memory, _chunk.size);
    }

    curl_easy_cleanup(handle);
    return str;
}

std::string RESTInterface::postJSON(const std::string &url, const std::string &content,
                                    std::function<void(const std::string &)> errCallback) {
    auto handle = getCurlHandle();

    curl_easy_setopt(handle, CURLOPT_URL, url.c_str());
    /* size of the POST data */
    curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, content.length());

    /* pass in a pointer to the data - libcurl will not copy */
    curl_easy_setopt(handle, CURLOPT_POSTFIELDS, content.c_str());

    curl_easy_setopt(handle, CURLOPT_HTTPHEADER, _jsonSList);

    auto res = curl_easy_perform(handle);

    std::string str;
    if (res != CURLE_OK) {
        errCallback(curl_easy_strerror(res));
        return "";
    } else {
        // convert to string and return
        str.reserve(_chunk.size);
        str.assign((char *) _chunk.memory, _chunk.size);
    }

    curl_easy_cleanup(handle);
    return str;
}