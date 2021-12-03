#include <vector>
#include <Python.h>

using KEY_TYPE = int;

const int MIN_KEY = 0;
const int MAX_KEY = 1000;

/*
    - char dict[256]
    - perfect hash function
*/

// from https://stackoverflow.com/questions/28560045/best-way-to-copy-c-buffer-to-python

extern "C" {

PyObject* to_pydict(std::vector<int>& dict) {
    PyObject* mydict = PyDict_New();
    for (int i = MIN_KEY; i <= MAX_KEY; i++) {
        if(!dict[i]) continue;
        PyDict_SetItem(mydict, PyLong_FromLong(i), PyLong_FromLong(dict[i]));
    }
    return mydict;
}

PyObject* countUnique(const std::vector<int>& li) {
    std::vector<int> dict(MAX_KEY - MIN_KEY + 1, 0);
    int ctr = 0;

    for(auto x : li) {
        if(dict[x] == 0) ctr++;
        dict[x] += 1;
    }

    return to_pydict(dict);
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

}
