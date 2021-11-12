#include <vector>
#include <dlfcn.h>
#include <map>
#include <type_traits>
#include <chrono>
#include <iostream>

#include "csvmonkey.hpp"

using namespace csvmonkey;

#define UMAP_DS 0
#define MAP_DS 1

#define FREQ 10 // only count number of keys in map
#define FULL 11 // return entire map (NRVO)

#define CURR_DS UMAP_DS
#define CURR_MP FREQ

using KEY_TYPE = int;

#if CURR_MP == FREQ
auto count_m_int = "./cpp_shared/count_m_freq_int.so";
auto count_m_string = "./cpp_shared/count_m_freq_string.so";
auto count_um_int = "./cpp_shared/count_um_freq_int.so";
auto count_um_string = "./cpp_shared/count_um_freq_string.so";
#else
auto count_m_int = "./cpp_shared/count_m_key_int.so";
auto count_m_string = "./cpp_shared/count_m_key_string.so";
auto count_um_int = "./cpp_shared/count_um_key_int.so";
auto count_um_string = "./cpp_shared/count_um_key_string.so";
#endif

// enclose in template function so that if constexpr doesn't compile not taken branch
template<typename T>
void run(const char* path) {
    void* handle;

    std::string count_unique_path = "";

    if constexpr(std::is_same<T, std::string>::value && CURR_DS == MAP_DS) {
        count_unique_path = count_m_string;
    } else if constexpr(std::is_same<T, std::string>::value && CURR_DS == UMAP_DS) {
        count_unique_path = count_um_string;
    } else if constexpr(std::is_same<T, int>::value && CURR_DS == UMAP_DS) {
        count_unique_path = count_um_int;
    } else {
        count_unique_path = count_m_int;
    }

// choose function pointer type
#if CURR_DS == MAP_DS && CURR_MP == FULL
    std::vector<std::map<T, int> > (*countUniqueList)(std::vector<std::vector<T> >&);
#elif CURR_DS == UMAP_DS && CURR_MP == FULL
    std::vector<std::unordered_map<T, int> > (*countUniqueList)(std::vector<std::vector<T> >&);
#elif CURR_MP == FREQ
    std::vector<int> (*countUniqueList)(std::vector<std::vector<T> >&);
#endif

    char* error;
    handle = dlopen(count_unique_path.c_str(), RTLD_LAZY);
    if (!handle) {
        fprintf(stderr, "%s\n", dlerror());
        exit(1);
    }

    dlerror(); // reset

    *(void **)(&countUniqueList) = dlsym(handle, "countUniqueList");
    if ((error = dlerror()) != nullptr) {
        fprintf(stderr, "%s\n", error);
        exit(1);
    }


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