#include <Python.h>
#include <unordered_map>
#include <vector>
#include <string>

#ifdef STR
using KEY_TYPE = std::string;
#else
using KEY_TYPE = int;
#endif

extern "C" {

PyObject* to_pydict(std::unordered_map<KEY_TYPE, int>& dict) {
    PyObject* mydict = PyDict_New();
    for(auto& kv : dict) {
        #ifndef STR
        PyDict_SetItem(mydict, PyLong_FromLong(kv.first), PyLong_FromLong(kv.second));
        #else
        PyDict_SetItem(mydict, PyBytes_FromString(kv.first.c_str()), PyLong_FromLong(kv.second));
        #endif
    }
    return mydict;
}


PyObject* countUnique(const std::vector<KEY_TYPE>& li) {
    std::unordered_map<KEY_TYPE, int> my_map;
    for(auto x : li) {
        my_map[x] += 1;
    }
    return to_pydict(my_map);
}


std::vector<PyObject*> countUniqueList(const std::vector<std::vector<KEY_TYPE> >& li) {
    std::vector<PyObject*> my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}


}