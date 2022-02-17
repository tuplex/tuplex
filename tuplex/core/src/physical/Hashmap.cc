//
// Created by Leonhard Spiegelberg on 2/4/22.
//

#include <physical/Hashmap.h>
#include <hashmap.h>

namespace tuplex {


    Hashmap::~Hashmap() {

    }


    Hashmap::Hashmap(const TransformStage::HashResult &hs) {
        // for now, assume hashmap is the bytes hashmap. That's not always true but yeah...
        _type = HashmapType::HT_BYTESMAP;
        _nullBucket = hs.null_bucket;
        _underlying = hs.hash_map;
        _underlyingHybrid = hs.hybrid;
    }

    size_t Hashmap::size() const {
        return 0;
    }

    size_t Hashmap::capacity() const {
        return 0;
    }


    // @TODO: need to refactor C-hashmap


    Hashmap Hashmap::deserialize(const URI &uri) {
        Hashmap h;
        auto& logger = Logger::instance().logger("physical");

        // open file
        auto file = VirtualFileSystem::open_file(uri, VirtualFileMode::VFS_READ);
        if(!file)
            throw std::runtime_error("could not open file " + uri.toString() + " in order to read hashmap.");

        uint64_t header[2];
        size_t bytes_read = 0;
        file->read(header, sizeof(uint64_t) * 2, &bytes_read);
        assert(bytes_read == sizeof(uint64_t) * 2);
        h._type = static_cast<HashmapType>(header[0]);
        // flags
        bool hasNullBucket = header[1] & (0x1 << 1u);
        bool hasHybrid = header[1] & (0x1 << 2u);

        if(hasNullBucket) {
            logger.debug("hash table has null bucket, reading in.");
            uint64_t null_bucket_size = 0;
            mallocAndReadBucket(file.get(), &h._nullBucket);
        }

        // construct main hashmap
        uint64_t h_table_size = 0;
        uint64_t h_size = 0;
        file->read(&h_table_size, sizeof(uint64_t));
        file->read(&h_size, sizeof(uint64_t));

        // all memory is allocated
        hashmap_map *hm = (hashmap_map*)malloc(sizeof(hashmap_map));
        if(!hm)
            throw std::bad_alloc();
        hm->data = (hashmap_element*)calloc(h_table_size, sizeof(hashmap_element));
        if(hm->data)
            throw std::bad_alloc();
        hm->table_size = h_table_size;
        hm->size = 0;
        // fill in data
        for(unsigned i = 0; i < h_size; ++i) {
            uint64_t index = 0;
            uint64_t key_len = 0;
            file->read(&index, sizeof(uint64_t));
            file->read(&key_len, sizeof(uint64_t));

            auto& hm_element = hm->data[index];
            hm_element.in_use = 1;
            hm_element.keylen = key_len;
            hm_element.key = (char*)malloc(key_len);
            if(!hm_element.key)
                throw std::bad_alloc();
            file->read(&hm_element.key, key_len);
            // now alloc and read bucket
            mallocAndReadBucket(file.get(), reinterpret_cast<uint8_t **>(&hm_element.data));
        }
        h._underlying = hm;

        if(hasHybrid) {
            throw std::runtime_error("hybrid not yet supported");
        }

        return h;
    }

    void Hashmap::serialize(const URI &uri) {

        if(HashmapType::HT_BYTESMAP != _type)
            throw std::runtime_error("only supported hashmap type is bytesmap right now...");

        // serialize hashmap to file
        // first serialize type, then what is present (as a flag)
        // then the individual sections
        auto file = VirtualFileSystem::open_file(uri, VirtualFileMode::VFS_OVERWRITE);
        if(!file)
            throw std::runtime_error("could not open file " + uri.toString() + " in order to serialize hashmap.");

        // @TODO: maybe indicate BIG endian/little endian upfront?
        uint64_t header[2];
        header[0] = static_cast<uint64_t>(_type);
        header[1] = 0;
        header[1] = (0x1 << 1u) * hasNullBucket();
        header[1] |= (0x1 << 2u) * hasHybrid();
        file->write(header, sizeof(uint64_t) * 2);

        if(hasNullBucket()) {
            writeBucket(file.get(), _nullBucket);
        }

        // serialize core hashmap to file (i.e. go over map_t structure with the iterator func)
        auto hm = (hashmap_map*)underlying();
        // serialize everything
        uint64_t h_table_size = hm->table_size;
        uint64_t h_size = hm->size;
        file->write(&h_table_size, sizeof(uint64_t));
        file->write(&h_size, sizeof(uint64_t));
        size_t num_valid = 0;
        for(unsigned i = 0; i < h_table_size; ++i) {
            auto hm_element = hm->data[i];
            if(hm_element.in_use) {
                // write out data
                uint64_t index = i;
                uint64_t key_len = hm_element.keylen;

                file->write(&index, sizeof(uint64_t));
                file->write(&key_len, sizeof(uint64_t));
                file->write(hm_element.key, key_len);
                writeBucket(file.get(), static_cast<uint8_t*>(hm_element.data));
                num_valid++;
            }
        }
        if(num_valid != h_size)
            throw std::runtime_error("hash table corrupted");

        if(hasHybrid()) {
            // check how many python objects there are, then serialize them as pickled objects...
            throw std::runtime_error("hybrid not yet supported");
        }

        file->close();
    }

    size_t Hashmap::writeBucket(VirtualFile* file, uint8_t *bucket) {
        if(!bucket || !file)
            return 0;

        // get null bucket size (simply decode straight from null-bucket!)
        uint64_t bucket_info = *(uint64_t*)bucket;
        uint64_t bucket_size = bucket_info & 0xFFFFFFFF;
        auto num_elements = (bucket_info >> 32ul);

        // write first bucket size (this could be skipped in case)
        // and then bucket (bucket size!)
        file->write(&bucket_size, sizeof(uint64_t));
        file->write(bucket, bucket_size);

        return sizeof(uint64_t) + bucket_size;
    }

    void Hashmap::mallocAndReadBucket(VirtualFile *file, uint8_t **bucket) {
        if(!bucket)
            return;

        uint64_t bucket_size = 0;
        file->read(&bucket_size, sizeof(uint64_t));
        // malloc bucket
        *bucket = (uint8_t*)malloc(bucket_size);
        if(!*bucket)
            throw std::bad_alloc();
        file->read(*bucket, bucket_size);
    }
}