//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "int_hashmap.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#define INITIAL_SIZE (2048)
#define MAX_CHAIN_LENGTH (8)

/* A hashmap has some maximum size and current size,
 * as well as the data to hold. */
typedef struct _int64_hashmap_map {
    int table_size;
    int size;
    int64_hashmap_element *data;
} int64_hashmap_map;

/*
 * Return an empty hashmap, or NULL on failure.
 */
map_t int64_hashmap_new() {
    int64_hashmap_map *m = (int64_hashmap_map *) malloc(sizeof(int64_hashmap_map));
    if (!m) goto err;

    m->data = (int64_hashmap_element *) calloc(INITIAL_SIZE, sizeof(int64_hashmap_element));
    if (!m->data) goto err;

    m->table_size = INITIAL_SIZE;
    m->size = 0;

    return m;
    err:
    if (m)
        int64_hashmap_free(m);
    return NULL;
}

unsigned long int64_hashmap_hash(uint64_t key) {
    //MurmurHash64A
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = 0x8445d61a4e774912 ^ (8*m);
    key *= m;
    key ^= key >> r;
    key *= m;
    h ^= key;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

static inline uint32_t int64_hashmap_hash_location(int64_hashmap_map *m, uint64_t k) {

    assert(m); // sanity check!

    uint32_t key = int64_hashmap_hash(k);

    // use fastrange reduction trick from https://arxiv.org/abs/1805.10941
    // https://github.com/lemire/fastrange/blob/master/fastrange.h
    //return (uint32_t)(((uint64_t)word * (uint64_t)p) >> 32);
    return (uint32_t)(((uint64_t)key * (uint64_t)m->table_size) >> 32);
}

/*
 * Return the integer of the location in data
 * to store the point to the item, or MAP_FULL.
 */
int hashmap_hash(map_t in, uint64_t key) {
    int curr;
    int i;

    /* Cast the hashmap */
    int64_hashmap_map *m = (int64_hashmap_map *) in;

    /* If full, return immediately */
    if (m->size >= (m->table_size / 2)) return MAP_FULL;

    /* Find the best index */
    curr = int64_hashmap_hash_location(m, key);

    /* Linear probing */
    for (i = 0; i < MAX_CHAIN_LENGTH; i++) {
        if (m->data[curr].in_use == 0)
            return curr;

        if (m->data[curr].in_use == 1 && m->data[curr].key == key)
            return curr;

        // could use fastrange reduction here as well
        // @TODO
        curr = (curr + 1) % m->table_size;
    }

    return MAP_FULL;
}

/*
 * Doubles the size of the hashmap, and rehashes all the elements
 */
int int64_hashmap_rehash(map_t in) {
    int i;
    int old_size;
    int64_hashmap_element *curr;

    /* Setup the new elements */
    int64_hashmap_map *m = (int64_hashmap_map *) in;
    int64_hashmap_element *temp = (int64_hashmap_element *)
            calloc(2 * m->table_size, sizeof(int64_hashmap_element));
    if (!temp) return MAP_OMEM;

    /* Update the array */
    curr = m->data;
    m->data = temp;

    /* Update the size */
    old_size = m->table_size;
    m->table_size = 2 * m->table_size;
    m->size = 0;

    /* Rehash the elements */
    for (i = 0; i < old_size; i++) {
        int status;

        if (curr[i].in_use == 0)
            continue;

        status = int64_hashmap_put(m, curr[i].key, curr[i].data);
        if (status != MAP_OK)
            return status;
    }

    free(curr);

    return MAP_OK;
}

/*
 * Add a pointer to the hashmap with some key
 */
int int64_hashmap_put(map_t in, uint64_t key, int64_any_t value) {
    int index;
    int64_hashmap_map *m;

    /* Cast the hashmap */
    m = (int64_hashmap_map *) in;

    /* Find a place to put our value */
    index = hashmap_hash(in, key);
    while (index == MAP_FULL) {
        if (int64_hashmap_rehash(in) == MAP_OMEM) {
            return MAP_OMEM;
        }
        index = hashmap_hash(in, key);
    }

    /* Set the data */
    m->data[index].data = value;
    if(m->data[index].in_use == 0) {
        m->data[index].key = key;
        m->size++;
    } else {
#ifndef NDEBUG
        // make sure key is fine...
        assert(m->data[index].key == key);
#endif
    }
    m->data[index].in_use = 1;

    return MAP_OK;
}

/*
 * Get your pointer out of the hashmap with a key
 */
int int64_hashmap_get(map_t in, uint64_t key, int64_any_t *arg) {
    int curr;
    int i;
    int64_hashmap_map *m;

    /* Cast the hashmap */
    m = (int64_hashmap_map *) in;

    /* Find data location */
    curr = int64_hashmap_hash_location(m, key);

    /* Linear probing, if necessary */
    for (i = 0; i < MAX_CHAIN_LENGTH; i++) {

        int in_use = m->data[curr].in_use;
        if (in_use == 1) {
            if (m->data[curr].key == key) {
                *arg = (m->data[curr].data);
                return MAP_OK;
            }
        }

        curr = (curr + 1) % m->table_size;
    }

    *arg = NULL;

    /* Not found */
    return MAP_MISSING;
}

/*
 * Iterate the function parameter over each element in the hashmap.  The
 * additional any_t argument is passed to the function as its first
 * argument and the hashmap element is the second.
 */
int int64_hashmap_iterate(map_t in, PFintany f, int64_any_t item) {
    int i;

    /* Cast the hashmap */
    int64_hashmap_map *m = (int64_hashmap_map *) in;

    /* On empty hashmap, return immediately */
    if (int64_hashmap_length(m) <= 0)
        return MAP_MISSING;

    /* Linear probing */
    for (i = 0; i < m->table_size; i++)
        if (m->data[i].in_use != 0) {
            int status = f(item, &m->data[i]);
            if (status != MAP_OK) {
                return status;
            }
        }

    return MAP_OK;
}

int int64_hashmap_free_key_and_data(map_t in) {
    int i;

    /* Cast the hashmap */
    int64_hashmap_map *m = (int64_hashmap_map *) in;

    /* On empty hashmap, return immediately */
    if (int64_hashmap_length(m) <= 0)
        return MAP_MISSING;

    /* Linear probing */
    for (i = 0; i < m->table_size; i++)
        if (m->data[i].in_use != 0) {
            free(m->data[i].data);
            m->data[i].key = 0;
            m->data[i].data = NULL;
            m->data[i].in_use = 0;
        }

    return MAP_OK;
}

/*
 * Remove an element with that key from the map
 */
int int64_hashmap_remove(map_t in, uint64_t key) {
    int i;
    int curr;
    int64_hashmap_map *m;

    /* Cast the hashmap */
    m = (int64_hashmap_map *) in;

    /* Find key */
    curr = int64_hashmap_hash_location(m, key);

    /* Linear probing, if necessary */
    for (i = 0; i < MAX_CHAIN_LENGTH; i++) {

        int in_use = m->data[curr].in_use;
        if (in_use == 1) {
            if (m->data[curr].key == key) {
                /* Blank out the fields */
                m->data[curr].in_use = 0;
                m->data[curr].data = NULL;
                m->data[curr].key = 0;

                /* Reduce the size */
                m->size--;
                return MAP_OK;
            }
        }
        curr = (curr + 1) % m->table_size;
    }

    /* Data not found */
    return MAP_MISSING;
}

/* Deallocate the hashmap */
void int64_hashmap_free(map_t in) {
    int64_hashmap_map *m = (int64_hashmap_map *) in;
    free(m->data);
    free(m);
}

/* Return the length of the hashmap */
int int64_hashmap_length(map_t in) {
    int64_hashmap_map *m = (int64_hashmap_map *) in;
    if (m != NULL) return m->size;
    else return 0;
}

size_t int64_hashmap_bucket_count(map_t in) {
    int64_hashmap_map *m = (int64_hashmap_map *) in;
    if(!m)
        return 0;
    size_t count = 0;
    for(unsigned i = 0; i < m->table_size; ++i) {
        count += m->data[i].in_use;
    }
    return count;
}

bool int64_hashmap_get_next_key(map_t in, int64_hashmap_iterator_t *it, uint64_t *key) {
    int64_hashmap_map *m = (int64_hashmap_map *)in;
    while(*it < m->table_size) {
        auto cur = *it;
        ++(*it);
        if(m->data[cur].in_use != 0) {
            *key = m->data[cur].key;
            return true;
        }
    }
    return false;
}