//
// Created by Nathaneal Pitt on 7/23/21.
//

#ifndef TUPLEX_SORTBY_H
#define TUPLEX_SORTBY_H

namespace tuplex {
    enum SortBy { ASCENDING = 1,
        DESCENDING = 2,
        ASCENDING_LENGTH = 3,
        DESCENDING_LENGTH = 4,
        ASCENDING_LEXICOGRAPHICALLY = 5,
        DESCENDING_LEXICOGRAPHICALLY = 6 };
}

#endif //TUPLEX_SORTBY_H
