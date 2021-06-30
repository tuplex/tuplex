//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_HISTORYSERVERCLASSES_H
#define TUPLEX_HISTORYSERVERCLASSES_H

#include <string>
#include <cstdint>

namespace tuplex {

    //@Todo: Failed and aborted when user presses Ctrl-C
    // or kills job from WebUI

    enum class JobStatus {
        SCHEDULED,
        STARTED,
        RUNNING,
        FINISHED
    };

    inline std::string jobStatusToString(const JobStatus js) {
        switch(js) {
            case JobStatus::SCHEDULED:
                return "scheduled";
            case JobStatus::STARTED:
                return "started";
            case JobStatus::RUNNING:
                return "running";
            case JobStatus::FINISHED:
                return "finished";
            default:
                return "unknown";
        }
    }

    struct HistoryServerConnection {
        std::string host;
        uint16_t port;
        std::string db_host;
        uint16_t db_port;
        bool connected;
    };
}

#endif //TUPLEX_HISTORYSERVERCLASSES_H