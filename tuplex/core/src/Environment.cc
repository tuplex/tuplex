//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Environment.h>
#include <cstdlib>
#include <limits.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <Utils.h>

namespace tuplex {

// cf. https://stackoverflow.com/questions/27914311/get-computer-name-and-logged-user-name
    std::string getHostName() {
        char hostname[1024];
        auto res = gethostname(hostname, 1024);
        return std::string(hostname);
    }

    std::map<std::string, std::string> getTuplexEnvironment() {
        std::map<std::string, std::string> m;


        // user, mode, host
        m["tuplex.env.user"] = getUserName();
        m["tuplex.env.hostname"] = getHostName();
        m["tuplex.env.mode"] = "c++";

        return m;
    }
}