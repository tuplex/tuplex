//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_STATUTILS_H
#define TUPLEX_STATUTILS_H

#include <limits>
#include <cmath>

namespace tuplex {
    template<typename T> class RollingStats {
    private:
        size_t _count;
        double _mean;
        double _m2;
        T _min;
        T _max;
    public:


        RollingStats():_count(0),
        _mean(0),
        _m2(0),
        _min(std::numeric_limits<T>::max()),
        _max(std::numeric_limits<T>::min()) {}

        void update(const T value) {
            _count++;
            _min = std::min(_min, value);
            _max = std::max(_max, value);

            auto delta = value - _mean;
            _mean = _mean + delta / _count;
            auto delta2 = value - _mean;
            _m2 = _m2 + delta * delta2;
        }

        size_t count() const { return _count; }
        double mean() const { return _mean;}

        //! sample standard deviation
        double std() const {
            if(_m2 == 0.0) return 0;
            return sqrt(_m2 / (_count-1));
        }
        T min() const { return _min; }
        T max() const { return _max; }
    };
}
#endif //TUPLEX_STATUTILS_H