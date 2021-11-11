#include <map>
#include <unordered_map>
#include <vector>

using CURR_TYPE = int;

extern "C" {

int countUnique(const std::vector<CURR_TYPE>& li) {
    std::map<CURR_TYPE, int> my_map;
    for(auto x : li) {
        my_map[x] += 1;
    }
    return my_map.size();
}

}

extern "C" {

std::vector<int> countUniqueList(const std::vector<std::vector<CURR_TYPE> >& li) {
    std::vector<int> my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}

}