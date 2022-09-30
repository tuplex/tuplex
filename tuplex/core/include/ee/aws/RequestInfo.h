//
// Created by Leonhard Spiegelberg on 1/31/22.
//

#ifndef TUPLEX_REQUESTINFO_H
#define TUPLEX_REQUESTINFO_H

#include <Base.h>
#include <StringUtils.h>
#include <Lambda.pb.h>

namespace tuplex {

    // helper function to calculate how many parts to invoke from recurse specification
    inline size_t lambdaCount(const std::vector<size_t>& recursive_counts) {

        if(recursive_counts.empty())
            return 0;

        size_t total_parts = 1;
        size_t prod = 1;
        size_t num_lambdas_to_invoke = 0;
        for(unsigned i = 0; i < recursive_counts.size(); ++i) {
            auto count = recursive_counts[i];
            if(count != 0) {
                total_parts += count * prod; // this is recursive, so try splitting into that many parts!
                prod *= count;
            }
        }
        return total_parts;
    }

    /*!
     * helper struct holding decoded information obtained from a log of a Lambda request
     */
    struct RequestInfo {
        std::string requestId;
        std::string containerId; //! uuid of container
        double durationInMs;
        size_t billedDurationInMs;
        size_t memorySizeInMb;
        size_t maxMemoryUsedInMb;

        uint64_t tsRequestStart; //! ns UTC timestamp
        uint64_t tsRequestEnd; //! ns UTC timestamp

        // log may display error message!
        int returnCode;
        std::string errorMessage;

        RequestInfo() : durationInMs(0), billedDurationInMs(100), memorySizeInMb(0), maxMemoryUsedInMb(0),
        returnCode(0), tsRequestStart(0), tsRequestEnd(0) {}

        RequestInfo(const messages::RequestInfo& info) : requestId(info.requestid().c_str()),
        containerId(info.containerid()),
        durationInMs(info.durationinms()), billedDurationInMs(info.billeddurationinms()), memorySizeInMb(info.memorysizeinmb()),
        maxMemoryUsedInMb(info.maxmemoryusedinmb()), returnCode(info.returncode()), errorMessage(info.errormessage().c_str()), tsRequestStart(info.tsrequeststart()), tsRequestEnd(info.tsrequestend()) {}


        static RequestInfo parseFromLog(const std::string& log);

        // protobuf representation
        inline std::string asJSON() const {
            std::stringstream ss;
            ss<<"{\"requestId\":\""<<requestId<<"\"";
            ss<<",\"containerId\":\""<<containerId<<"\"";
            ss<<",\"durationInMs\":"<<durationInMs;
            ss<<",\"billedDurationInMs\":"<<billedDurationInMs;
            ss<<",\"memorySizeInMb\":"<<memorySizeInMb;
            ss<<",\"maxMemoryUsedInMb\":"<<maxMemoryUsedInMb;
            ss<<",\"returnCode\":"<<returnCode;
            ss<<",\"errorMessage\":\""<<errorMessage<<"\"";
            ss<<",\"tsRequestStart\":"<<tsRequestStart;
            ss<<",\"tsRequestEnd\":"<<tsRequestEnd;
            ss<<"}";
            return ss.str();
        }

        inline void fill(messages::RequestInfo* r) const {
            if(!r)
                return;

            r->set_requestid(requestId.c_str());
            r->set_containerid(containerId.c_str());
            r->set_durationinms(durationInMs);
            r->set_billeddurationinms(billedDurationInMs);
            r->set_memorysizeinmb(memorySizeInMb);
            r->set_maxmemoryusedinmb(maxMemoryUsedInMb);
            r->set_returncode(returnCode);
            r->set_errormessage(errorMessage.c_str());
            r->set_tsrequeststart(tsRequestStart);
            r->set_tsrequestend(tsRequestEnd);
        }

        inline messages::RequestInfo* to_protobuf() const {
            auto r = new messages::RequestInfo();
            fill(r);
            return r;
        }
    };
}

#endif //TUPLEX_REQUESTINFO_H
