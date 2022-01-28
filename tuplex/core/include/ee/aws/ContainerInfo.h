//
// Created by Leonhard Spiegelberg on 1/27/22.
//

#ifndef TUPLEX_CONTAINERINFO_H
#define TUPLEX_CONTAINERINFO_H

#include <Base.h>
#include <StringUtils.h>
#include <Lambda.pb.h>

namespace tuplex {
    struct ContainerInfo {
        bool reused; //! whether container has been reused or not
        std::string requestId; //! Lambda Request ID
        std::string uuid; //! uuid of container
        uint32_t msRemaining; //! how many milliseconds remain of this container when info was added
        uint32_t requestsServed; //! how many requests did this container already serve? (incl. the current one)
        uint64_t startTimestamp; //! when container was started
        uint64_t deadlineTimestamp; //! when container will shutdown/expire

        ContainerInfo() = default;

        ContainerInfo(const messages::ContainerInfo& info) : reused(info.reused()),
                                                             requestId(info.requestid().c_str()),
                                                             uuid(info.uuid().c_str()),
                                                             msRemaining(info.msremaining()),
                                                             requestsServed(info.requestsserved()),
                                                             startTimestamp(info.start()),
                                                             deadlineTimestamp(info.deadline()) {

        }

        inline std::string asJSON() const {
            std::stringstream ss;
            ss<<"{\"reused\":"<<(reused ? "true" : "false");
            ss<<",\"requestId\":\""<<requestId<<"\"";
            ss<<",\"uuid\":\""<<uuid<<"\"";
            ss<<",\"msRemaining\":"<<msRemaining;
            ss<<",\"requestsServed\":"<<requestsServed;
            ss<<",\"startTimestamp\":"<<startTimestamp;
            ss<<",\"deadlineTimestamp\":"<<deadlineTimestamp;
            ss<<"}";
            return ss.str();
        }


        inline void fill(messages::ContainerInfo* c) const {
            if(!c)
                return;

            c->set_reused(reused);
            c->set_requestid(requestId.c_str());
            c->set_uuid(uuid.c_str());
            c->set_msremaining(msRemaining);
            c->set_requestsserved(requestsServed);
            c->set_start(startTimestamp);
            c->set_deadline(deadlineTimestamp);
        }

        inline messages::ContainerInfo* to_protobuf() const {
            auto c = new messages::ContainerInfo();
            fill(c);
            return c;
        }
    };
}

#endif //TUPLEX_CONTAINERINFO_H
