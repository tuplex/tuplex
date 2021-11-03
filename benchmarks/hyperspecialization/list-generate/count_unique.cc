#include <bits/stdc++.h>
#include "csvmonkey.hpp"

using namespace csvmonkey;

// does NRVO
template<typename T>
std::unordered_map<T, int> count_um(const std::vector<T>& li) {
    std::unordered_map<T, int> my_umap;
    for(auto x : li) {
        my_umap[x] += 1;
    }
    return my_umap;
}

// does NRVO
template<typename T>
std::map<T, int> count_m(const std::vector<T>& li) {
    std::map<T, int> my_map;
    for(auto x : li) {
        my_map[x] += 1;
    }
    return my_map;
}

template<typename T>
auto count_list_of_lists_um(const std::vector<std::vector<T> >& li) {
    std::vector<std::unordered_map<T, int> > my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(count_um(x));
    }
    return my_vec;
}


template<typename T>
auto count_list_of_lists_m(const std::vector<std::vector<T> >& li) {
    std::vector<std::map<T, int> > my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(count_m(x));
    }
    return my_vec;
}

int main(int argc, char** argv) {
    const char* path = "";
    if (argc > 1) {
        path = argv[1];
    }


    // MappedFileCursor
    MappedFileCursor stream;
    stream.open(path);

    CsvReader<MappedFileCursor> reader(stream);
    CsvCursor &row = reader.row();

    std::vector<std::vector<std::string> > list_of_lists;

    while(reader.read_row()) {
        std::vector<std::string> v;
        for(size_t i = 0; i < row.count; i++) {
            CsvCell &cell = row.cells[i];
            std::string s = cell.as_str();
            v.push_back(s);
        }

        list_of_lists.push_back(v);
    }

    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    auto t1 = high_resolution_clock::now();

    auto result = count_list_of_lists_um(list_of_lists);

    auto t2 = high_resolution_clock::now();
    /* Getting number of milliseconds as an integer. */
    auto ms_int = duration_cast<milliseconds>(t2 - t1);
    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;
    std::cout << ms_double.count() << "\n";
}