// //--------------------------------------------------------------------------------------------------------------------//
// //                                                                                                                    //
// //                                      Tuplex: Blazing Fast Python Data Science                                      //
// //                                                                                                                    //
// //                                                                                                                    //
// //  (c) 2017 - 2021, Tuplex team                                                                                      //
// //  Created by Leonhard Spiegelberg first on 8/9/2021                                                                 //
// //  License: Apache 2.0                                                                                               //
// //--------------------------------------------------------------------------------------------------------------------//

// #include <BuiltinDictProxy.h>
// #include "gtest/gtest.h"

// class DictProxyTest : public TuplexTest {};

// // helper function to generate combinations with repititions
// template<typename T> void combinations_r_recursive(const std::vector<T> &elements, std::size_t combination_length,
//                               std::vector<unsigned long> &pos, unsigned long depth,
//                               unsigned long margin, std::vector<std::vector<T>>& result) {
//     // Have we selected the number of required elements?
//     if (depth >= combination_length) {
//         std::vector<T> combination;
//         combination.reserve(combination_length);
//         for(unsigned long ii = 0; ii < pos.size(); ++ii)
//             combination.push_back(elements[pos[ii]]);
//         combination.shrink_to_fit();
//         result.push_back(combination);
//         return;
//     }

//     // Try to select new elements to the right of the last selected one.
//     for (unsigned long ii = margin; ii < elements.size(); ++ii) {
//         pos[depth] = ii;
//         combinations_r_recursive(elements, combination_length, pos, depth + 1, ii, result);
//     }
// }

// template<typename T> std::vector<std::vector<T>> combinations_with_repetition(const std::vector<T> &elements, size_t combination_length) {
//     assert(combination_length <= elements.size());
//     std::vector<unsigned long> positions(combination_length, 0);
//     std::vector<std::vector<T>> result;
//     combinations_r_recursive(elements, combination_length, positions, 0, 0, result);

//     return result;
// }



// TEST_F(DictProxyTest, PutItemTest) {
//     using namespace tuplex;
//     using namespace std;

//     // testing the non-codegenerated put item test


//     // tests to write:

//     // 1. heterogenous dict -> basically use modified JSON as in-memory storage format.
//     // 2. homogenous keytype dict -> can encode dict directly & serialize it more efficiently. Represent in-memory as hash table specialized depending on type.
//     // 3. homogenous valuetype -> ignore case, specialize to 1.
//     // 4. compile-time known keys/restricted keyset, keys do not change. -> struct type with fixed offsets!

//     // put and get
//     auto dict_fun_code = "def f(a, b, c, d):\n"
//                          "    M = dict()\n"
//                          "    M[a] = b\n"
//                          "    M[c] = d\n"
//                          "    return M, M[a], M[c]\n";

//     codegen::BuiltinDictProxy dict_proxy(python::Type::UNKNOWN);

//     // create test setups (4 values, all combos)
//     vector<Field> test_values{Field((int64_t)0), Field(10.0), Field(false), Field::null(), Field("hello world"), Field(Tuple(10, 20)), Field(Tuple(3.141, 10, false, "test")), Field(List(1.0, 3.0, 4.0))};

//     // NOTE: list/dict is not hashable in python!
//     // 

//     // create combos
//     // 4 ^ len(test_values)


//     // what about nested dicts?
//     // -> unflatten?
//     // --> unflatten using combined keys? i.e. a/b/c ? which char to use as separator?
//     // maybe start with non-nested dicts.
//     // dicts should be able to store lists etc.

//     auto combos = combinations_with_repetition(test_values, 4);

//     cout<<"Generated "<<combos.size()<<" combinations."<<endl<<endl;
//     for(auto combo : combos) {
//         for(auto f : combo)
//             cout<<f.toPythonString()<<" ";
//         cout<<endl;
//     }

//     // this should a good variety of what things to store in dictionaries
//     for(int i = 0; i < std::pow(4, test_values.size()); ++i) {

//     }


// //
// //    dict_proxy.putItem(Field((int64_t)10), Field("test"));
// //    dict_proxy.putItem(Field((int64_t)20), Field("hello"));


//     // limited keyset, dynamic access etc.

//     // basically we need only a couple dictionary primitives:

//     // 1. fixed set of keys -> can be checked dynamically at runtime. I.e., good for read-only dictionaries, rarely changed ones. etc. --> requires dispatch dictionary for each type for dynamic types. Constants can be translated during compile time.
//     // -> because dicts support in syntax, need to keep additional bitmap to check whether there's a valid entry or not!


//     // 2. fixed key type/value type dicts -> can be used in dynamic settings. E.g., when accumulating things!

//     // 3. other usage should be esoteric...

// }
