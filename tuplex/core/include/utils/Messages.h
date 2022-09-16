//
// Created by Leonhard Spiegelberg on 7/8/22.
//

#ifndef TUPLEX_MESSAGES_H
#define TUPLEX_MESSAGES_H

// collection of all protobuf messages + definitions
#include <Lambda.pb.h>

namespace tuplex {
    enum class ResourceType {
        UNKNOWN = 0,
        LOG = 1
    };
}

#endif //TUPLEX_MESSAGES_H
