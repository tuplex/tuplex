#ifndef TUPLEX_FILESYSTEMUTILS_H
#define TUPLEX_FILESYSTEMUTILS_H

#include "gtest/gtest.h"
#include <random>

#include <VirtualFileSystem.h>

inline std::string uniqueFileName(const std::string& prefix="") {
    using namespace tuplex;
    auto lookup = "abcdefghijklmnopqrstuvqxyz";
    auto len = strlen(lookup);
    std::stringstream ss;
    ss << lookup[std::rand() % len];
    while (fileExists(prefix + ss.str()) && ss.str().length() < 255) {
        ss << lookup[std::rand() % len];
    }
    auto fileName = prefix + ss.str();
    if (fileExists(fileName)) {
        printf("%s\n", fileName.c_str());
        throw std::runtime_error("could not create unique file name");
    }
    return fileName;
}

#endif //TUPLEX_FILESYSTEMUTILS_H
