//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ENVIRONMENT_H
#define TUPLEX_ENVIRONMENT_H

#include <map>
#include <string>

namespace tuplex {

    /*!
     * constructs tuplex.env options
     * @return
     */
    extern std::map<std::string, std::string> getTuplexEnvironment();
}

#endif //TUPLEX_ENVIRONMENT_H