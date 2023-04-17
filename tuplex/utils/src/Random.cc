//
// Created by leonhards on 4/16/23.
//

#include <Random.h>
#include <random>

namespace tuplex {
    static std::mt19937_64 random_generator;

    void tuplex_seed(uint64_t seed) {
        // initiate global random engine with seed
        random_generator.seed(seed);
    }

    uint64_t tuplex_randi_excl(uint64_t n) {
        auto r = random_generator();
        return r % n;
    }
}