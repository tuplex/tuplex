#include <map>
#include <unordered_map>
#include <vector>
#include <string>

using INT_KEY_TYPE = int;
using INT_VAL_TYPE = int;

using STR_VAL_TYPE = int;

extern "C" {

int countUnique(const std::vector<int>& li) {
    std::map<INT_KEY_TYPE, INT_VAL_TYPE> my_map;
    for(auto x : li) {
        my_map[x] += 1;
    }
    return my_map.size();
}

int countUnique(const std::vector<std::string>& li) {
    std::map<std::string, STR_VAL_TYPE> my_map;
    for(auto x : li) {
        my_map[x] += 1;
    }
    return my_map.size();
}


std::vector<int> countUniqueList(const std::vector<std::vector<int> >& li) {
    std::vector<int> my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}

std::vector<int> countUniqueList(const std::vector<std::vector<std::string> >& li) {
    std::vector<int> my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}


}