#ifndef TUPLEX_FILESYSTEMUTILS_H
#define TUPLEX_FILESYSTEMUTILS_H

#include "gtest/gtest.h"

inline std::string uniqueFileName() {
    using namespace tuplex;
    auto lookup = "abcdefghijklmnopqrstuvqxyz";
    auto len = strlen(lookup);
    std::stringstream ss;
    ss << lookup[rand() & len];
    while (fileExists(ss.str()) && ss.str().length() < 255) {
        ss << lookup[rand() % len];
    }
    auto fileName = ss.str();
    if (fileExists(fileName)) {
        throw std::runtime_error("could not create unique file name");
    }
    return fileName;
}

#endif //TUPLEX_FILESYSTEMUTILS_H