#include <map>
#include <unordered_map>
#include <vector>
#include <string>

using INT_KEY_TYPE = int;
using INT_VAL_TYPE = int;

using STR_VAL_TYPE = int;

extern "C" {

std::unordered_map<INT_KEY_TYPE, INT_VAL_TYPE> countUnique(const std::vector<int>& li) {
    std::unordered_map<INT_KEY_TYPE, INT_VAL_TYPE> my_map;
    for(auto x : li) {
        my_map[x] += 1;
    }
    return my_map;
}

std::unordered_map<std::string, STR_VAL_TYPE> countUnique(const std::vector<std::string>& li) {
    std::unordered_map<std::string, STR_VAL_TYPE> my_map;
    for(auto x : li) {
        my_map[x] += 1;
    }
    return my_map;
}


std::vector<std::unordered_map<INT_KEY_TYPE, INT_VAL_TYPE> > countUniqueList(const std::vector<std::vector<int> >& li) {
    std::vector<std::unordered_map<INT_KEY_TYPE, INT_VAL_TYPE> > my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}

std::vector<std::unordered_map<std::string, STR_VAL_TYPE>> countUniqueList(const std::vector<std::vector<std::string> >& li) {
    std::vector<std::unordered_map<std::string, STR_VAL_TYPE>> my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}


}