//
// Created by leonhards on 4/16/23.
//

#ifndef TUPLEX_RANDOM_H
#define TUPLEX_RANDOM_H

#include "Base.h"

namespace tuplex {

    /*!
     * seed internal tuplex random engine
     * @param seed some random number
     */
    extern void tuplex_seed(uint64_t seed);

    /*!
     * returns random integers in interval [0, ..., n-1]
     * @param n
     * @return random integer
     */
    extern uint64_t tuplex_randi_excl(uint64_t n);

//    /*!
//     * returns random integers in interval [low, ..., high]
//     * @param low
//     * @param high
//     * @return random integer
//     */
//    extern int64_t tuplex_randi_incl(int64_t low, int64_t high);
}

#endif //TUPLEX_RANDOM_H
