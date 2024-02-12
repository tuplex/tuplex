//
// Created by leonhards on 10/30/23.
//

#ifndef TUPLEX_PARAMETERS_H
#define TUPLEX_PARAMETERS_H

namespace tuplex {
    // use row type to push down compute (should become standard)
    static const bool PARAM_USE_ROW_TYPE=true;

    // check to use generic dict instead of struct dict type to execute
    static const bool PARAM_USE_GENERIC_DICT=true;
}
#endif //TUPLEX_PARAMETERS_H
