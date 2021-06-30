//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_HASHMAP_H
#define TUPLEX_HASHMAP_H

#include <list>
#include <boost/thread/pthread/shared_mutex.hpp>
#include <mutex>

namespace tuplex {

// thread-safe lookup table based on p.171 of C++ Concurrency in Action
template<typename Key, typename Value, typename Hash=std::hash<Key> > class hashmap {
private:
    class bucket_type {
    private:
        using bucket_value=std::pair<Key, Value> ;
        using bucket_data=std::list<bucket_value> ;
        using bucket_const_iterator=typename bucket_data::const_iterator;
        using bucket_iterator=typename bucket_data::iterator;

        bucket_data _data;
        // C++17 feature
        //mutable std::shared_mutex _mutex;

        mutable boost::shared_mutex _mutex;
        bucket_const_iterator find_entry_for(Key const& key) const {
            return std::find_if(_data.begin(), _data.end(), [&](bucket_value const& item) {
                return item.first == key;
            });
        }

        bucket_iterator find_entry_for(Key const& key) {
            return std::find_if(_data.begin(), _data.end(), [&](bucket_value const& item) {
                return item.first == key;
            });
        }
    public:
        Value value_for(Key const& key, Value const& default_value) const {
            boost::shared_lock<boost::shared_mutex> lock(_mutex);
            auto found_entry=find_entry_for(key);
            return (found_entry ==  _data.end()) ? default_value : found_entry->second;
        }

        void add_or_update_mapping(Key const& key, Value const& value) {
            std::unique_lock<boost::shared_mutex> lock(_mutex);
            auto found_entry = find_entry_for(key);
            if(_data.end() == found_entry) {
                _data.push_back(bucket_value(key, value));
            } else {
                found_entry->second = value;
            }
        }

        void remove_mapping(Key const& key) {
            std::unique_lock<boost::shared_mutex> lock(_mutex);
            auto found_entry = find_entry_for(key);
            if(_data.end() != found_entry) {
                _data.erase(found_entry);
            }
        }

        bool exists(Key const& key) const {
            boost::shared_lock<boost::shared_mutex> lock(_mutex);
            auto found_entry=find_entry_for(key);
            return found_entry !=  _data.end();
        }
    };

    std::vector<std::unique_ptr<bucket_type>> _buckets;
    Hash _hasher;

    bucket_type& get_bucket(Key const& key) const {
        std::size_t const bucket_index = _hasher(key) % _buckets.size();
        return *_buckets[bucket_index];
    }

public:
    using key_type=Key;
    using mapped_type=Value;
    using hash_type=Hash;

    hashmap(unsigned num_buckets = 19, Hash const& hasher=Hash()) : _buckets(num_buckets), _hasher(hasher) {
        for(unsigned i = 0; i < num_buckets; ++i) {
            _buckets[i].reset(new bucket_type);
        }
    }

    // non copyable
    hashmap(hashmap const& other) = delete;
    hashmap& operator = (hashmap const& other) = delete;

    Value get(Key const& key, Value const& default_value=Value()) const {
        return get_bucket(key).value_for(key, default_value);
    }

    bool exists(Key const& key) const {
        return get_bucket(key).exists(key);
    }

    void put(Key const& key, Value const& value) {
        get_bucket(key).add_or_update_mapping(key, value);
    }

    void drop(Key const& key) {
        get_bucket(key).remove_mapping(key);
    }
};
}

#endif //TUPLEX_HASHMAP_H