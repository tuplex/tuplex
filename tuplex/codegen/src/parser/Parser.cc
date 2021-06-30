//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <parser/Parser.h>
#include <parser/ASTBuilderVisitor.h>
#include <parser/ASTPrinter.h>
#include <antlr4-runtime/antlr4-runtime.h>
#include <Python3Parser.h>
#include <Python3Lexer.h>

namespace tuplex {

    // this is basically a helper class, the actual lifting is done via
    // antlr4 and a visitor to convert things...
    ASTNode* parseToAST(const std::string& code) {

        using namespace std;
        using namespace antlr4;

        // empty code? do not parse...
        if(code.empty())
            return nullptr;

        // make sure code ends with NEWLINE
        ANTLRInputStream input(code.back() != '\n' ? code + "\n" : code);

        Python3Lexer lexer(&input);
        CommonTokenStream tokens(&lexer);
        Python3Parser parser(&tokens);
        auto tree = parser.file_input();
        if(!tree)
            return nullptr;

        // use visitor to construct AST tree
        try {
            ASTBuilderVisitor v;
            tree->accept(&v);
            // convert to AST tree
            return v.getAST();
        } catch(const std::string& message) {
            Logger::instance().logger("python3 parser").error("Could not parse python3 input: " + message);
            return nullptr;
        }
    }


    void printParseTree(const std::string& code, std::ostream& os) {
        using namespace std;
        using namespace antlr4;

        // empty code? do not parse...
        if(code.empty())
            return;

        // make sure code ends with NEWLINE
        ANTLRInputStream input(code.back() != '\n' ? code + "\n" : code);

        Python3Lexer lexer(&input);
        CommonTokenStream tokens(&lexer);
        Python3Parser parser(&tokens);
        auto tree = parser.file_input();

        // print AST
        ASTPrinter printer(&parser, true);
        //    ASTPrinter printer(&parser, false);
        printer.print(tree, os);
    }
}