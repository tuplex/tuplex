//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_IBACKEND_H
#define TUPLEX_IBACKEND_H

#include <ee/local/Executor.h>
#include <physical/PhysicalStage.h>
#include <unordered_map>
#include <vector>
#include <ExceptionCodes.h>
#include <cstdint>
#include <memory>

namespace tuplex {

    class IBackend;
    class PhysicalStage;
    class Executor;
    class Context;

    class IBackend {
    public:
        IBackend() = delete;
        IBackend(const IBackend& other) = delete;
        IBackend(const Context& context) : _context(context) {}

        // driver, i.e. where to store local data.
        virtual Executor* driver() = 0;
        virtual void execute(PhysicalStage* stage) = 0;

        virtual ~IBackend() {} // virtual destructor needed b.c. of smart pointers

        virtual const Context& context() const { return _context; }

    private:
        const Context& _context;
    };

    inline std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> merge_ecounts(std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> lhs,
            std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> rhs) {
        // go over lhs copy and merge rhs values in
        for(auto keyval : rhs) {
            auto it = lhs.find(keyval.first);
            if(it == lhs.end())
                lhs[keyval.first] = keyval.second;
            else
                lhs[keyval.first] += keyval.second;
        }

        return lhs;
    }

}
#endif //TUPLEX_IBACKEND_H