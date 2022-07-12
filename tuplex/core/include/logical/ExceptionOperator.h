//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_EXCEPTIONOPERATOR_H
#define TUPLEX_EXCEPTIONOPERATOR_H

#include "LogicalOperator.h"
#include "LogicalOperatorType.h"

namespace tuplex {

    // use CRTP pattern to mixin exception handling details
    template<typename T> class ExceptionOperator  {
    public:
        /*!
         * return first ancestor node which is not an exception operator
         * @return null if not found
         */
        std::shared_ptr<LogicalOperator> getNormalParent() const {
            const T& underlying = static_cast<const T&>(*this);

            // go up till non-resolver is found
            std::shared_ptr<LogicalOperator> parent = underlying.parent();
            if(!parent)
                return nullptr;

            while(isExceptionOperator(parent->type())) {
                // debug, runtime assert
                assert(dynamic_cast<ExceptionOperator*>(parent.get()));

                parent = parent->parent();
                if(!parent)
                    return nullptr;
            }

            // parent can't be .cache()! disallow for now...
            if(parent->type() == LogicalOperatorType::CACHE)
                throw std::runtime_error("can't mix resolvers/ignores with cache!");

            return parent;
        }

        /*!
         * returns exception code this operator addresses of its parent
         * @return
         */
        ExceptionCode ecCode() const { return _ec; }

        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(_ec);
        }
        template<class Archive> void load(Archive &ar) {
            ar(_ec);
        }
    private:
        ExceptionCode _ec;

    protected:
        void setCode(const ExceptionCode& code) { _ec = code; }
    };
}
#endif //TUPLEX_EXCEPTIONOPERATOR_H