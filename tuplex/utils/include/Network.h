//
// Created by Leonhard Spiegelberg on 11/16/21.
//

#ifndef TUPLEX_NETWORK_H
#define TUPLEX_NETWORK_H

#include <string>

namespace tuplex {

    // helper struct to store various network related settings to apply to CURL etc.
    struct NetworkSettings {
        std::string caFile;
        std::string caPath;
        bool verifySSL;
        NetworkSettings() : verifySSL(false) {}
    };
}

#endif //TUPLEX_NETWORK_H
