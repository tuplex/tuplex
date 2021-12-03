#include <bits/stdc++.h>
#include "../csvmonkey.hpp"
#include <cjson/cJSON.h>
#include <Python.h>

using KEY_TYPE = int;
using namespace csvmonkey;

const int MIN_KEY = 0;
const int MAX_KEY = 1000;

/*
    - char dict[256]
    - perfect hash function
*/

// PyObject* convert_to_dict(int* dict) {
//     PyObject* mydict = PyDict_New();
//     for(int i = 0; i <= )
// }

int countUnique(const std::vector<int>& li) {
    int dict[MAX_KEY + 1];
    memset(dict, 0, sizeof(dict));
    int ctr = 0;

    for(auto x : li) {
        if(dict[x] == 0) ctr++;
        dict[x] += 1;
    }
    return ctr;
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