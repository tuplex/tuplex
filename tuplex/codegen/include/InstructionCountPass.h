//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_INSTRUCTIONCOUNTPASS_H
#define TUPLEX_INSTRUCTIONCOUNTPASS_H

#include <llvm/Pass.h>
#include <unordered_map>
#include <iostream>
#include <sstream>

namespace tuplex {
    // code taken from https://github.com/cmu-db/peloton/blob/1de89798f271804f8be38a71219a20e761a1b4b6/src/codegen/code_context.cpp
////////////////////////////////////////////////////////////////////////////////
///
/// Instruction Count Pass
///
////////////////////////////////////////////////////////////////////////////////

/**
 * This class analyzes a given LLVM module and keeps statistics on:
 *   1. The number of functions defined in the module
 *   2. The number of externally defined functions called from this module
 *   3. The number of basic blocks in the module
 *   4. The total number of instructions in the module
 *   5. A breakdown of instruction counts by their type.
 *
 * Counts can be retrieved through the accessors, or all statistics can be
 * dumped to the logger through DumpStats().
 */
    class InstructionCounts : public llvm::ModulePass {
    public:
        explicit InstructionCounts(char &pid)
                : ModulePass(pid),
                  external_func_count_(0),
                  func_count_(0),
                  basic_block_count_(0),
                  total_inst_counts_(0) {}

        bool runOnModule(::llvm::Module &module) override {
            for (const auto &func : module) {
                if (func.isDeclaration()) {
                    external_func_count_++;
                } else {
                    func_count_++;
                }
                for (const auto &block : func) {
                    basic_block_count_++;
                    for (const auto &inst : block) {
                        total_inst_counts_++;
                        counts_[inst.getOpcode()]++;
                    }
                }
            }
            return false;
        }

        std::unordered_map<std::string, size_t> countStats() const {

            std::unordered_map<std::string, size_t> countMap;

            countMap["#functions"] = func_count_;
            countMap["#external_functions"] = external_func_count_;
            countMap["#instructions"] = total_inst_counts_;
            countMap["#blocks"] = basic_block_count_;

            // detailed per instruction counts
            for (const auto& iter : counts_) {
                const char *inst_name = llvm::Instruction::getOpcodeName(iter.first);
                countMap[inst_name] = iter.second;
            }

            return countMap;
        }

        std::string formattedStats(bool include_detailed_inst_counts = false) const {
            using namespace std;

            vector<string> generalKeys{"#functions", "#external_functions", "#instructions", "#blocks"};
            stringstream ss;
            ss << "General counts:\n";

            auto m = countStats();

            for (const auto& k : generalKeys) {
                ss << "\t" << k.substr(1) << ": " << m[k] << "\n";
            }

            if (include_detailed_inst_counts) {
                ss << "\nDetailed instruction counts:\n";
                for (const auto& keyval : m) {
                    auto key = keyval.first;

                    if (std::find(generalKeys.begin(), generalKeys.end(), key) == generalKeys.end())
                        ss << "\t" << key << ": " << keyval.second << "\n";

                }

            }

            return ss.str();
        }

    private:
        uint64_t external_func_count_;
        uint64_t func_count_;
        uint64_t basic_block_count_;
        uint64_t total_inst_counts_;
        llvm::DenseMap<uint32_t, uint64_t> counts_;
    };
}



#endif //TUPLEX_INSTRUCTIONCOUNTPASS_H