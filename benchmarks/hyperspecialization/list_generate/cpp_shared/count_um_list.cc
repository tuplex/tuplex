#include <map>
#include <unordered_map>
#include <vector>

using CURR_TYPE = int;

extern "C" {

std::unordered_map<CURR_TYPE, int> countUnique(const std::vector<CURR_TYPE>& li) {
    std::unordered_map<CURR_TYPE, int> my_umap;
    for(auto x : li) {
        my_umap[x] += 1;
    }
    return my_umap;
}

}

extern "C" {

std::vector<std::unordered_map<CURR_TYPE, int> > countUniqueList(const std::vector<std::vector<CURR_TYPE> >& li) {
    std::vector<std::unordered_map<CURR_TYPE, int> > my_vec;
    my_vec.reserve(li.size());
    for(const auto& x : li) {
        my_vec.push_back(countUnique(x));
    }
    return my_vec;
}

}