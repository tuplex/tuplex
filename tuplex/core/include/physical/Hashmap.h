//
// Created by Leonhard Spiegelberg on 2/4/22.
//

#ifndef TUPLEX_HASHMAP_H
#define TUPLEX_HASHMAP_H

#include <Base.h>
#include <StringUtils.h>
#include <VirtualFileSystem.h>
#include <HybridHashTable.h>
#include <Python.h>

#include "TransformStage.h"

/// this file holds an overall hashmap (can be of any specialized type)

namespace tuplex {

    enum class HashmapType {
        HT_UNKNOWN = 0,
        HT_BYTESMAP = 1,
        HT_INTMAP = 2
    };


    // perhaps use interface design, i.e. common IHashmap -> then derive specialized versions from it!

    class Hashmap {
    public:

        Hashmap() : _type(HashmapType::HT_UNKNOWN), _underlying(nullptr), _underlyingHybrid(nullptr), _nullBucket(nullptr) {}

        // construct from HashResult??
        Hashmap(const TransformStage::HashResult& hs);

        ~Hashmap();

        void serialize(const URI& uri);
        static Hashmap deserialize(const URI& uri);

        size_t size() const;
        size_t capacity() const;

        void* nullBucket() const { return _nullBucket; }
        void* underlying() const { return _underlying; }
        PyObject* underlyingHybrid() const { return _underlyingHybrid; }

        bool hasNullBucket() const { return _nullBucket; }
        bool hasHybrid() const { return _underlyingHybrid; }

    private:
        HashmapType _type; //! the physical representation of this particular hashmap

//        python::Type _keyType;
//        python::Type _bucketType;

        uint8_t* _nullBucket;

        void* _underlying;
        PyObject* _underlyingHybrid;

        size_t writeBucket(VirtualFile* file, uint8_t* bucket);
        static void mallocAndReadBucket(VirtualFile* file, uint8_t** bucket);

        inline uint64_t bucketSize(uint64_t* bucket) {
            if(!bucket)
                return 0;

            uint64_t bucket_info = *(uint64_t*)bucket;
            uint64_t bucket_size = bucket_info & 0xFFFFFFFF;
            return bucket_size;
        }
    };

    class FixedKeySizeMap : public Hashmap {
    public:
        FixedKeySizeMap(size_t keySizeInBytes) : _keySize(keySizeInBytes) {}

        template<typename T> void put(const T& key, const uint8_t* value, size_t value_size, bool copy_value=true);
        template<typename T> const uint8_t* get(const T& key, size_t *value_size=nullptr);
    private:
        size_t _keySize;


    };
}

#endif //TUPLEX_HASHMAP_H
