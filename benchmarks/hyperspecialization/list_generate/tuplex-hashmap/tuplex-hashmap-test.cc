#include "hashmap.h"
// #include "int_hashmap.h"
#include <bits/stdc++.h>
#include "../csvmonkey.hpp"

using namespace csvmonkey;

using KEY_TYPE = int;

int countUnique(const std::vector<int>& li) {
    map_t my_map = hashmap_new();
    int res = -999;
    any_t val = 0;

    for(auto x : li) {
        // my_map[x] += 1;
        res = hashmap_get(my_map, x, &val);
        assert(res == -3 || res == 0);
        if (res == MAP_MISSING) {
            res = hashmap_put(my_map, x, (any_t) 1);
            assert(res == -3 || res == 0);
        } else {
            res = hashmap_put(my_map, x, val + 1);
            assert(res == -3 || res == 0);
        }

    }
    return hashmap_length(my_map);
}

std::vector<int> countUniqueList(const std::vector<std::vector<int> >& li) {
    std::vector<int> my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}


template<typename T>
void run(const char* path) {
    // MappedFileCursor
    MappedFileCursor stream;
    stream.open(path);

    CsvReader<MappedFileCursor> reader(stream);
    CsvCursor &row = reader.row();

    std::vector<std::vector<T> > list_of_lists;

    while(reader.read_row()) {
        std::vector<T> v;
        for(size_t i = 0; i < row.count; i++) {
            CsvCell &cell = row.cells[i];
            std::string s = cell.as_str();
            if constexpr (std::is_same<T, int>::value) {
                v.push_back(stoi(s));
            } else {
                v.push_back(s);
            }
        }

        list_of_lists.push_back(v);
    }

    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    auto t1 = high_resolution_clock::now();

    auto result = countUniqueList(list_of_lists);

    auto t2 = high_resolution_clock::now();
    /* Getting number of milliseconds as an integer. */
    auto ms_int = duration_cast<milliseconds>(t2 - t1);
    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;
    std::cout << ms_double.count() << "\n";

}

int main(int argc, char** argv) {
    const char* path = "";
    if (argc > 1) {
        path = argv[1];
    }

    run<KEY_TYPE>(path);
}