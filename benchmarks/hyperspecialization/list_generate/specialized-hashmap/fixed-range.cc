#include <bits/stdc++.h>
#include "../csvmonkey.hpp"
#include <Python.h>

using KEY_TYPE = int;
using namespace csvmonkey;

const int MIN_KEY = 0;
const int MAX_KEY = 1000;

/*
    - char dict[256]
    - perfect hash function
*/

// from https://stackoverflow.com/questions/28560045/best-way-to-copy-c-buffer-to-python
PyObject *
vec_to_array(std::vector<int>& vec)
{
    static PyObject *single_array;
    if (!single_array) {
        PyObject *array_module = PyImport_ImportModule("array");
        if (!array_module)
            return NULL;
        PyObject *array_type = PyObject_GetAttrString(array_module, "array");
        Py_DECREF(array_module);
        if (!array_type)
            return NULL;
        // array.array('d', [0.0])
        single_array = PyObject_CallFunction(array_type, "c[d]", 'd', 0);
        Py_DECREF(array_type);
        if (!single_array)
            return NULL;
    }

    // extra-fast way to create an empty array of count elements:
    //   array = single_element_array * count
    PyObject *pysize = PyLong_FromSsize_t(vec.size());
    if (!pysize)
        return NULL;
    PyObject *array = PyNumber_Multiply(single_array, pysize);
    Py_DECREF(pysize);
    if (!array)
        return NULL;

    // now, obtain the address of the array's buffer
    PyObject *buffer_info = PyObject_CallMethod(array, "buffer_info", "");
    if (!buffer_info) {
        Py_DECREF(array);
        return NULL;
    }
    PyObject *pyaddr = PyTuple_GetItem(buffer_info, 0);
    void *addr = PyLong_AsVoidPtr(pyaddr);

    // and, finally, copy the data.
    if (vec.size())
        memcpy(addr, &vec[0], vec.size() * sizeof(int));

    return array;
}


PyObject* convert_to_dict(int* dict) {
    
    // uint32_t num_keys = 0;
    // std::vector<int> temp_keys;

    // temp_keys.reserve(MAX_KEY - MIN_KEY + 1);
    // for(int i = MIN_KEY; i <= MAX_KEY; i++) {
    //     num_keys += (dict[i] > 0);
    //     if(dict[i] > 0) {
    //         temp_keys.push_back(i);
    //     }
    // }

    // PyObject* iterable_keys = vec_to_array(temp_keys);

    // PyObject *array_module = PyImport_ImportModule("array");
    // if (!array_module)
    //     return NULL;
    // PyObject *array_type = PyObject_GetAttrString(array_module, "array");
    // Py_DECREF(array_module);
    // if (!array_type)
    //     return NULL;
    
    // PyObject* mydict = _PyDict_FromKeys(array_type, iterable_keys, PyLong_FromLong(0));
    // Py_DECREF(array_type);

    PyObject* mydict = PyDict_New();

    assert(mydict != NULL);
    for(int i = MIN_KEY; i <= MAX_KEY; i++) {
        if(!dict[i]) continue;

        PyObject* key = PyLong_FromLong(i);
        PyObject* value = PyLong_FromLong(dict[i]);

        assert(key != NULL);
        assert(value != NULL);

        // std::cout << PyDict_Size(mydict) << std::endl;
        PyDict_SetItem(mydict, key, value);
    }
    return mydict;
}

PyObject* countUnique(const std::vector<int>& li) {
    int dict[MAX_KEY + 1];
    memset(dict, 0, sizeof(dict));
    int ctr = 0;

    for(auto x : li) {
        if(dict[x] == 0) ctr++;
        dict[x] += 1;
    }

    return convert_to_dict(dict);
}

std::vector<PyObject*> countUniqueList(const std::vector<std::vector<int> >& li) {
    std::vector<PyObject*> my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        // auto result = countUnique(x);
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}


template<typename T>
void run(const char* path) {

    Py_Initialize();

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