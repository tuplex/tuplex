//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ASTPRINTER_H
#define TUPLEX_ASTPRINTER_H

#include <iostream>
#include <antlr4-runtime/antlr4-runtime.h>
#include <Python3Parser.h>

namespace tuplex {
    class ASTPrinter {
    private:
        void explore(antlr4::RuleContext* ctx, int indentation, std::ostream& os) {
            using namespace std;

            bool toBeIgnored = _ignoreWrappers && ctx->children.size() == 1 &&
                    dynamic_cast<antlr4::RuleContext*>(ctx->children.front());

            if(!toBeIgnored) {
                auto ruleName = ruleNames[ctx->getRuleIndex()];
                for(int i = 0; i < indentation; ++i) {
                    os<<"  ";
                }
                os<<ruleName<<endl;
            }

            for(auto element : ctx->children) {
                if(dynamic_cast<antlr4::RuleContext*>(element)) {
                    explore((antlr4::RuleContext*)element, indentation + (toBeIgnored ? 0 : 1), os);
                }
            }
        }
        bool _ignoreWrappers;
        std::vector<std::string> ruleNames;

    public:
        ASTPrinter() = delete;


        ASTPrinter(antlr4::Python3Parser* parser, bool ignoreWrappers=true) : _ignoreWrappers(ignoreWrappers) {
            ruleNames = parser->getRuleNames();
        }

        void print(antlr4::RuleContext* ctx, std::ostream& os) {
            explore(ctx, 0, os);
        }
    };
}

#endif //TUPLEX_ASTPRINTER_H