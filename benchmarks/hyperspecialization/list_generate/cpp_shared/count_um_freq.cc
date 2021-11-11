#include <map>
#include <unordered_map>
#include <vector>

using CURR_TYPE = std::string;

extern "C" {

int countUnique(const std::vector<CURR_TYPE>& li) {
    std::unordered_map<CURR_TYPE, int> my_umap;
    for(auto x : li) {
        my_umap[x] += 1;
    }
    return my_umap.size();
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