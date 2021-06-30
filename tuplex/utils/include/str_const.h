//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_STR_CONST_H
#define TUPLEX_STR_CONST_H


// based on file:///Users/leos/Downloads/schurr_cpp11_tools_for_class_authors.pdf
// this file allows for easier handling of compile time strings
class str_constant {
private:
    const char* const _ptr;
    const std::size_t _len;
public:
    template<std::size_t N> constexpr str_constant(const char(&p)[N]) : _ptr(p), _len(N -1) {}
    constexpr char operator [] (std::size_t i) const {return i < _len ? _ptr[i] : throw std::out_of_range(_ptr); }
    constexpr std::size_t length() const { return _len; }
};

#endif //TUPLEX_STR_CONST_H