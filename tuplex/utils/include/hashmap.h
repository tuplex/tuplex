//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef __HASHMAP_H__
#define __HASHMAP_H__


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
 * any_t is a pointer.  This allows you to put arbitrary structures in
 * the hashmap.
 */
typedef void *any_t;

/* We need to keep keys and values */
typedef struct _hashmap_element {
    char *key;
    uint64_t keylen;
    int in_use;
    any_t data;
} hashmap_element;

/* A hashmap has some maximum size and current size,
 * as well as the data to hold. */
typedef struct _hashmap_map {
    int table_size;
    int size;
    hashmap_element *data;
} hashmap_map;

/*
 * PFany is a pointer to a function that can take two any_t arguments
 * and return an integer. Returns status code..
 */
typedef int (*PFany)(any_t, hashmap_element*);

/*
 * map_t is a pointer to an internally maintained data structure.
 * Clients of this package do not need to know how hashmaps are
 * represented.  They see and manipulate only map_t's.
 */
typedef any_t map_t;

/*
 * Return an empty hashmap. Returns NULL if empty.
*/
extern map_t hashmap_new()  __attribute__((used));

/*
 * Iteratively call f with argument (item, data) for
 * each element data in the hashmap. The function must
 * return a map status code. If it returns anything other
 * than MAP_OK the traversal is terminated. f must
 * not reenter any hashmap functions, or deadlock may arise.
 */
extern int hashmap_iterate(map_t in, PFany f, any_t item)  __attribute__((used));

/*!
 * calls free(...) on both key and data. Should be followed by hashmap_free call
 * @param in hashmap
 * @return MAP_OK
 */
extern int hashmap_free_key_and_data(map_t in) __attribute__((used));

/*
 * Add an element to the hashmap. Return MAP_OK or MAP_OMEM.
 */
extern int hashmap_put(map_t in, const char* key, uint64_t keylen, any_t value)  __attribute__((used));

/*
 * put into hashmap, avoid strlen call
 */
extern int hashmap_fastput(map_t in, char* key, int key_length, any_t value) __attribute__((used));

/*
 * Get an element from the hashmap. Return MAP_OK or MAP_MISSING.
 */
extern int hashmap_get(map_t in, const char* key, uint64_t keylen, any_t *arg)  __attribute__((used));

/*
 * Remove an element from the hashmap. Return MAP_OK or MAP_MISSING.
 */
extern int hashmap_remove(map_t in, char* key, uint64_t keylen)  __attribute__((used));

/*
 * Get any element. Return MAP_OK or MAP_MISSING.
 * remove - should the element be removed from the hashmap
 */
extern int hashmap_get_one(map_t in, any_t *arg, int remove)  __attribute__((used));

/*!
 * computes the size of the hashmap and data elements
 * @param in map
 * @return size in bytes of the hashmap (not incl. data!)
 */
extern int hashmap_size(map_t in);

/*
 * Free the hashmap
 */
extern void hashmap_free(map_t in)  __attribute__((used));

/*
 * Get the current size of a hashmap
 */
extern int hashmap_length(map_t in)  __attribute__((used));

/*!
 * return how many buckets are used
 * @param in
 * @return how many buckets are used
 */
extern std::size_t hashmap_bucket_count(map_t in) __attribute__((used));

extern unsigned long hashmap_crc32(const unsigned char *s, unsigned int len);

typedef int hashmap_iterator_t;
//extern hashmap_iterator_t hashmap_begin();
extern const char* hashmap_get_next_key(map_t in, hashmap_iterator_t *it, uint64_t *keylen) __attribute__((used));

// C guard
#ifdef __cplusplus
}
#endif

#endif