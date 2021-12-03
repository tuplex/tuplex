#include <map>
#include <unordered_map>
#include <vector>
#include <string>

#ifdef STR
using KEY_TYPE = std::string;
#else
using KEY_TYPE = int;
#endif

extern "C" {

std::map<KEY_TYPE, int> countUnique(const std::vector<KEY_TYPE>& li) {
    std::map<KEY_TYPE, int> my_map;
    for(auto x : li) {
        my_map[x] += 1;
    }
    return my_map;
}


std::vector<std::map<KEY_TYPE, int> > countUniqueList(const std::vector<std::vector<KEY_TYPE> >& li) {
    std::vector<std::map<KEY_TYPE, int> > my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}


}