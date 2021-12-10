//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef __INT_HASHMAP_H__
#define __INT_HASHMAP_H__


// C guard
#ifdef __cplusplus
extern "C" {
#endif

#include <cstdint>
#include <cstdlib>

#define MAP_MISSING -3  /* No such element */
#define MAP_FULL -2 	/* Hashmap is full */
#define MAP_OMEM -1 	/* Out of Memory */
#define MAP_OK 0 	/* OK */

/*
 * int64_any_t is a pointer.  This allows you to put arbitrary structures in
 * the hashmap.
 */
typedef void *int64_any_t; // TODO: probably pull this out to a common header for both hashtables

/* We need to keep keys and values */
typedef struct _int_hashmap_element {
    uint64_t key;
    int in_use;
    int64_any_t data;
} int64_hashmap_element;

/*
 * PFany is a pointer to a function that can take two int64_any_t arguments
 * and return an integer. Returns status code..
 */
typedef int (*PFintany)(int64_any_t, int64_hashmap_element*);

/*
 * map_t is a pointer to an internally maintained data structure.
 * Clients of this package do not need to know how hashmaps are
 * represented.  They see and manipulate only map_t's.
 */
typedef int64_any_t map_t;

/*
 * Return an empty hashmap. Returns NULL if empty.
*/
extern map_t int64_hashmap_new()  __attribute__((used));

/*
 * Iteratively call f with argument (item, data) for
 * each element data in the hashmap. The function must
 * return a map status code. If it returns anything other
 * than MAP_OK the traversal is terminated. f must
 * not reenter any hashmap functions, or deadlock may arise.
 */
extern int int64_hashmap_iterate(map_t in, PFintany f, int64_any_t item)  __attribute__((used));

/*!
 * calls free(...) on both key and data. Should be followed by hashmap_free call
 * @param in hashmap
 * @return MAP_OK
 */
extern int int64_hashmap_free_key_and_data(map_t in) __attribute__((used));

/*
 * Add an element to the hashmap. Return MAP_OK or MAP_OMEM.
 */
extern int int64_hashmap_put(map_t in, uint64_t key, int64_any_t value)  __attribute__((used));

/*
 * put into hashmap, avoid strlen call
 */
extern int int64_hashmap_fastput(map_t in, uint64_t key, int64_any_t value) __attribute__((used));

/*
 * Get an element from the hashmap. Return MAP_OK or MAP_MISSING.
 */
extern int int64_hashmap_get(map_t in, uint64_t key, int64_any_t *arg)  __attribute__((used));

/*
 * Remove an element from the hashmap. Return MAP_OK or MAP_MISSING.
 */
extern int int64_hashmap_remove(map_t in, uint64_t key)  __attribute__((used));

/*
 * Get any element. Return MAP_OK or MAP_MISSING.
 * remove - should the element be removed from the hashmap
 */
extern int int64_hashmap_get_one(map_t in, int64_any_t *arg, int remove)  __attribute__((used));

/*
 * Free the hashmap
 */
extern void int64_hashmap_free(map_t in)  __attribute__((used));

/*
 * Get the current size of a hashmap
 */
extern int int64_hashmap_length(map_t in)  __attribute__((used));

/*!
 * return how many buckets are used
 * @param in
 * @return how many buckets are used
 */
extern std::size_t int64_hashmap_bucket_count(map_t in) __attribute__((used));

extern unsigned long int64_hashmap_hash(uint64_t key);

typedef int int64_hashmap_iterator_t;
extern bool int64_hashmap_get_next_key(map_t in, int64_hashmap_iterator_t *it, uint64_t *key) __attribute__((used));

// C guard
#ifdef __cplusplus
}
#endif

#endif