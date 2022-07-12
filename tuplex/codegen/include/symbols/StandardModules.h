//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_STANDARDMODULES_H
#define TUPLEX_STANDARDMODULES_H

#include <string>
#include "ast/ASTAnnotation.h"
#include <TypeSystem.h>
#include <memory>

namespace tuplex {
    namespace module {
        // define here all internal modules for which partial translation support exists.
        extern std::shared_ptr<Symbol> mathModule(std::string alias="math");
        extern std::shared_ptr<Symbol> randomModule(std::string alias="random");
        extern std::shared_ptr<Symbol> reModule(std::string alias="re");
        extern std::shared_ptr<Symbol> stringModule(std::string alias="string");
    }
}
#endif //TUPLEX_STANDARDMODULES_H