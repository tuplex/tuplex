//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PARSETREEPRINTER_H
#define TUPLEX_PARSETREEPRINTER_H

#include <iostream>
#include <iomanip>

// ANTLR4 Runtime
#include <antlr4-runtime/antlr4-runtime.h>

// generated files
#include <Python3Parser.h>

namespace tuplex {

    /*!
     * helper class to print ANTLR4 parse tree out
     */
    class ParseTreePrinter {
    private:
        void explore(antlr4::RuleContext* ctx, int indentation, std::ostream& os) {
            using namespace std;

            bool toBeIgnored = ignoringWrappers && ctx->children.size() == 1 && dynamic_cast<antlr4::RuleContext*>(ctx->children.front());

            if(!toBeIgnored) {
                auto ruleName = ruleNames[ctx->getRuleIndex()];
                for(int i = 0; i < indentation; ++i) {
                    os<<"  ";
                }
                os<<ruleName<<endl;
            }

            for(auto element : ctx->children) {
                if(dynamic_cast<antlr4::RuleContext*>(element)) {
                    explore((antlr4::RuleContext*)element, indentation + (toBeIgnored ? 0 : 1));
                }
            }
        }
        bool _ignoreWrappers;
        std::vector<std::string> ruleNames;

    public:
        ParseTreePrinter() = delete;

        /*!
         * create a new ParseTreePrinter object
         * @param parser Parser on which this printer shall be based on
         * @param ignoreWrappers
         */
        ParseTreePrinter(Python3Parser& parser, bool ignoreWrappers=true) {
            ruleNames = parser.getRuleNames();
            _ignoreWrappers = ignoreWrappers
        }

        void print(antlr4::RuleContext* ctx, std::ostream& os=std::cout) {
            explore(ctx, 0, os);
        }
    };
}

#endif //TUPLEX_PARSETREEPRINTER_H