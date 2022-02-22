//
// Created by Leonhard Spiegelberg on 2/22/22.
//

#ifndef HYPERFLIGHTS_AGG_H
#define HYPERFLIGHTS_AGG_H

#include <cstdint>
#include <string>
#include <memory>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cmath>

// define interfaces to export
extern "C" int64_t initAggregate();
extern "C" int64_t process_cells(void *userData, char **cells, int64_t *cell_sizes);
extern "C" int64_t fetchAggregate(uint8_t** buf, size_t* buf_size);

#endif //HYPERFLIGHTS_AGG_H
