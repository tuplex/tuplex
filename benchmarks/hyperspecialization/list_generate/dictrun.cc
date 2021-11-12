#include <bits/stdc++.h>
#include <Python.h>
#include "csvmonkey.hpp"
using namespace csvmonkey;
using namespace std;

using KEY_TYPE = int;

long int dictrun(PyObject* mylist) {
    PyObject* mydict = PyDict_New();
    ssize_t listlen = PyList_Size(mylist);
    for(ssize_t i = 0; i < listlen; i++) {
        PyObject* key = PyList_GetItem(mylist, i);
        PyObject* item = PyDict_GetItem(mydict, key);
        if(item != NULL) {
            uint64_t curr = PyLong_AsUnsignedLongLong(item);
            PyDict_SetItem(mydict, key, PyLong_FromUnsignedLongLong(curr + 1));
        } else {
            PyDict_SetItem(mydict, key, PyLong_FromUnsignedLongLong(0));
        }
    }

    return PyDict_Size(mydict);
}

map<uint64_t, uint64_t> mpu;
map<double, uint64_t> mpd;
map<string, uint64_t> mps;

void runWithCPython(PyObject* mylist) {
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    auto t1 = high_resolution_clock::now();

    long int result = dictrun(mylist);

    auto t2 = high_resolution_clock::now();

    std::cerr << result << "\n";
    /* Getting number of milliseconds as an integer. */
    auto ms_int = duration_cast<milliseconds>(t2 - t1);

    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;

    std::cout << ms_double.count() << " ";// << "ms\n";
}


template<typename T>
void run(const char* path) {
    Py_Initialize();

    // PyEval_AcquireLock();
    std::cout << path << "\n";

    MappedFileCursor stream;
    stream.open(path);

    CsvReader<MappedFileCursor> reader(stream);
    CsvCursor &row = reader.row();

    // std::vector<std::vector<T> > list_of_lists;

    PyObject* mylist = PyList_New(0);

    while(reader.read_row()) {
        PyObject* sublist = PyList_New(0);
        for(size_t i = 0; i < row.count; i++) {
            CsvCell &cell = row.cells[i];
            std::string s = cell.as_str();
            if constexpr (std::is_same<T, int>::value) {
                assert(!PyList_Append(mylist, PyLong_FromLong(stoi(s))));
            } else {
                assert(!PyList_Append(mylist, PyUnicode_FromString(s.c_str())));
            }
        }

        assert(!PyList_Append(mylist, sublist));
    }

    std::cout << "length of PyList: " << PyList_Size(mylist) << "\n";

    runWithCPython(mylist);
}

int main(int argc, char** argv) {
    const char* path = "";
    if (argc > 1) {
        path = argv[1];
    }

    run<KEY_TYPE>(path);
}