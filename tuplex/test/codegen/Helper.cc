//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "Helper.h"
#include <Logger.h>
#include <AnnotatedAST.h>
#include "gtest/gtest.h"


// helper function
void testDefCode(const std::string& code,
                 const std::map<std::string, python::Type>& typeHints,
                 const python::Type& expectedReturnType) {
    tuplex::codegen::AnnotatedAST cg;
    ASSERT_TRUE(cg.parseString(code, false));

    for(auto keyval : typeHints)
        cg.addTypeHint(keyval.first, keyval.second);

    auto env = std::make_unique<tuplex::codegen::LLVMEnvironment>();
    EXPECT_TRUE(cg.generateCode(env.get(), true, true));

    std::cout<<"return type: "<<cg.getReturnType().desc()<<" expected: "<<expectedReturnType.desc()<<std::endl;
    EXPECT_EQ(cg.getReturnType(), expectedReturnType);

    std::cout<<env->getIR()<<std::endl;
}