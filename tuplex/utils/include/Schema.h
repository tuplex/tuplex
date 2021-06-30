//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_SCHEMA_H
#define TUPLEX_SCHEMA_H

#include "TypeSystem.h"
#include "Logger.h"
#include "Base.h"

namespace tuplex {


    class Schema {

    private:
        int recursiveSizeEstimator(const python::Type& t) const;
    public:
        static const Schema UNKNOWN;

        enum class MemoryLayout {
            UNKNOWN,
            COLUMNAR,
            ROW
        };

        Schema() {
            _rowType = UNKNOWN._rowType;
            _memLayout = UNKNOWN._memLayout;
        }

        Schema(const MemoryLayout& memLayout, const python::Type& type) :_memLayout(memLayout), _rowType(type) {

        }

        Schema(const Schema& other) {
            _rowType = other._rowType;
            _memLayout = other._memLayout;
        }

        Schema& operator = (const Schema& other) {
            _rowType = other._rowType;
            _memLayout = other._memLayout;
            return *this;
        }

        MemoryLayout getMemoryLayout() const  { return _memLayout; }
        python::Type getRowType() const      { return _rowType; }

        /*!
         * if no variable length elements (like strings, maps, dicts) are presented and each row has the same size in bytes
         * this function return true
         * @return
         */
        bool hasFixedSize() const;


        friend bool operator == (const Schema& rhs, const Schema& lhs);
        friend bool operator != (const Schema& rhs, const Schema& lhs);
    private:
        // type to describe the a row
        python::Type _rowType;
        MemoryLayout  _memLayout;
    };

    inline bool operator == (const Schema& rhs, const Schema& lhs) {
        return rhs._memLayout == lhs._memLayout && rhs._rowType == lhs._rowType;
    }

    inline bool operator != (const Schema& rhs, const Schema& lhs) {
       return !(rhs == lhs);
    }
}
#endif //TUPLEX_SCHEMA_H