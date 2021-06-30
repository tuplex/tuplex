//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Token.h>

std::ostream& operator<< (std::ostream& os, const TokenType tt)
{
    switch (tt) {
        case TokenType::UNKNOWN : return os << "UNKNOWN";
        case TokenType::ERROR: return os<<"ERROR";
        case TokenType::NUMBER: return os<<"NUMBER";
        case TokenType::STRING: return os<<"STRING";
        case TokenType::IDENTIFIER: return os<<"IDENTIFIER";
        case TokenType::ENDOFFILE: return os<<"ENDOFFILE";
        case TokenType::NEWLINE: return os<<"NEWLINE";
        case TokenType::INDENT: return os<<"INDENT";
        case TokenType::DEDENT: return os<<"DEDENT";
        case TokenType::COMMENT: return os<<"COMMENT";
        case TokenType::FALSE: return os<<"FALSE";
        case TokenType::NONE: return os<<"NONE";
        case TokenType::TRUE: return os<<"TRUE";
        case TokenType::AND: return os<<"AND";
        case TokenType::AS: return os<<"AS";
        case TokenType::ASSERT: return os<<"ASSERT";
        case TokenType::BREAK: return os<<"BREAK";
        case TokenType::CLASS: return os<<"CLASS";
        case TokenType::CONTINUE: return os<<"CONTINUE";
        case TokenType::DEF: return os<<"DEF";
        case TokenType::DEL: return os<<"DEL";
        case TokenType::ELIF: return os<<"ELIF";
        case TokenType::ELSE: return os<<"ELSE";
        case TokenType::EXCEPT: return os<<"EXCEPT";
        case TokenType::FINALLY: return os<<"FINALLY";
        case TokenType::FOR: return os<<"FOR";
        case TokenType::FROM: return os<<"FROM";
        case TokenType::GLOBAL: return os<<"GLOBAL";
        case TokenType::IF: return os<<"IF";
        case TokenType::IMPORT: return os<<"IMPORT";
        case TokenType::IN: return os<<"IN";
        case TokenType::IS: return os<<"IS";
        case TokenType::LAMBDA: return os<<"LAMBDA";
        case TokenType::NONLOCAL: return os<<"NONLOCAL";
        case TokenType::NOT: return os<<"NOT";
        case TokenType::OR: return os<<"OR";
        case TokenType::PASS: return os<<"PASS";
        case TokenType::RAISE: return os<<"RAISE";
        case TokenType::RETURN: return os<<"RETURN";
        case TokenType::TRY: return os<<"TRY";
        case TokenType::WHILE: return os<<"WHILE";
        case TokenType::WITH: return os<<"WITH";
        case TokenType::YIELD: return os<<"YIELD";
        case TokenType::LPAR: return os<<"LPAR";
        case TokenType::RPAR: return os<<"RPAR";
        case TokenType::LSQB: return os<<"LSQB";
        case TokenType::RSQB: return os<<"RSQB";
        case TokenType::LBRACE: return os<<"LBRACE";
        case TokenType::RBRACE: return os<<"RBRACE";
        case TokenType::COLON: return os<<"COLON";
        case TokenType::COMMA: return os<<"COMMA";
        case TokenType::SEMI: return os<<"SEMI";
        case TokenType::PLUS: return os<<"PLUS";
        case TokenType::MINUS: return os<<"MINUS";
        case TokenType::STAR: return os<<"STAR";
        case TokenType::SLASH: return os<<"SLASH";
        case TokenType::VBAR: return os<<"VBAR";
        case TokenType::AMPER: return os<<"AMPER";
        case TokenType::LESS: return os<<"LESS";
        case TokenType::GREATER: return os<<"GREATER";
        case TokenType::EQUAL: return os<<"EQUAL";
        case TokenType::DOT: return os<<"DOT";
        case TokenType::PERCENT: return os<<"PERCENT";
        case TokenType::CIRCUMFLEX: return os<<"CIRCUMFLEX";
        case TokenType::TILDE: return os<<"TILDE";
        case TokenType::AT: return os<<"AT";
        case TokenType::EQEQUAL: return os<<"EQEQUAL";
        case TokenType::NOTEQUAL: return os<<"NOTEQUAL";
        case TokenType::LESSEQUAL: return os<<"LESSEQUAL";
        case TokenType::LEFTSHIFT: return os<<"LEFTSHIFT";
        case TokenType::GREATEREQUAL: return os<<"GREATEREQUAL";
        case TokenType::RIGHTSHIFT: return os<<"RIGHTSHIFT";
        case TokenType::PLUSEQUAL: return os<<"PLUSEQUAL";
        case TokenType::MINEQUAL: return os<<"MINEQUAL";
        case TokenType::RARROW: return os<<"RARROW";
        case TokenType::DOUBLESTAR: return os<<"DOUBLESTAR";
        case TokenType::STAREQUAL: return os<<"STAREQUAL";
        case TokenType::DOUBLESLASH: return os<<"DOUBLESLASH";
        case TokenType::SLASHEQUAL: return os<<"SLASHEQUAL";
        case TokenType::VBAREQUAL: return os<<"VBAREQUAL";
        case TokenType::PERCENTEQUAL: return os<<"PERCENTEQUAL";
        case TokenType::AMPEREQUAL: return os<<"AMPEREQUAL";
        case TokenType::CIRCUMFLEXEQUAL: return os<<"CIRCUMFLEXEQUAL";
        case TokenType::ATEQUAL: return os<<"ATEQUAL";
        case TokenType::LEFTSHIFTEQUAL: return os<<"LEFTSHIFTEQUAL";
        case TokenType::RIGHTSHIFTEQUAL: return os<<"RIGHTSHIFTEQUAL";
        case TokenType::DOUBLESTAREQUAL: return os<<"DOUBLESTAREQUAL";
        case TokenType::DOUBLESLASHEQUAL: return os<<"DOUBLESLASHEQUAL";
        case TokenType::ELLIPSIS: return os<<"ELLIPSIS";
        default: return os << static_cast<std::uint16_t>(tt);
    }
}


TokenType stringToToken(const std::string& s) {
    if(s == "in")
        return TokenType::IN;
    if(s == "notin")
        return TokenType::NOTIN;

    if(s == "&&" || s == "and")
        return TokenType::AND;
    if(s == "!")
        return TokenType::NOT;
    if(s == "||" || s == "or")
        return TokenType::OR;
    if(s == "(")
        return TokenType::LPAR;
    if(s == ")")
        return TokenType::RPAR;
    if(s == "[")
        return TokenType::LSQB;
    if(s == "]")
        return TokenType::RSQB;
    if(s == "{")
        return TokenType::LBRACE;
    if(s == "}")
        return TokenType::RBRACE;
    if(s == ":")
        return TokenType::COLON;
    if(s == ",")
        return TokenType::COMMA;
    if(s == ";")
        return TokenType::SEMI;
    if(s == "+")
        return TokenType::PLUS;
    if(s == "-")
        return TokenType::MINUS;
    if(s == "*")
        return TokenType::STAR;
    if(s == "/")
        return TokenType::SLASH;
    if(s == "|")
        return TokenType::VBAR;
    if(s == "&")
        return TokenType::AMPER;
    if(s == "<")
        return TokenType::LESS;
    if(s == ">")
        return TokenType::GREATER;
    if(s == "=")
        return TokenType::EQUAL;
    if(s == ".")
        return TokenType::DOT;
    if(s == "%")
        return TokenType::PERCENT;
    if(s == "^")
        return TokenType::CIRCUMFLEX;
    if(s == "~")
        return TokenType::TILDE;
    if(s == "@")
        return TokenType::AT;
    if(s == "==")
        return TokenType::EQEQUAL;
    if(s == "!=")
        return TokenType::NOTEQUAL;
    if(s == "<=")
        return TokenType::LESSEQUAL;
    if(s == "<<")
        return TokenType::LEFTSHIFT;
    if(s == ">=")
        return TokenType::GREATEREQUAL;
    if(s == ">>")
        return TokenType::RIGHTSHIFT;
    if(s == "+=")
        return TokenType::PLUSEQUAL;
    if(s == "-=")
        return TokenType::MINEQUAL;
    if(s == "->")
        return TokenType::RARROW;
    if(s == "**")
        return TokenType::DOUBLESTAR;
    if(s == "*=")
        return TokenType::STAREQUAL;
    if(s == "//")
        return TokenType::DOUBLESLASH;
    if(s == "/=")
        return TokenType::SLASHEQUAL;
    if(s == "|=")
        return TokenType::VBAREQUAL;
    if(s == "%=")
        return TokenType::PERCENTEQUAL;
    if(s == "&=")
        return TokenType::AMPEREQUAL;
    if(s == "^=")
        return TokenType::CIRCUMFLEXEQUAL;
    if(s == "@=")
        return TokenType::ATEQUAL;
    if(s == "<<=")
        return TokenType::LEFTSHIFTEQUAL;
    if(s == ">>=")
        return TokenType::RIGHTSHIFTEQUAL;
    if(s == "**=")
        return TokenType::DOUBLESTAREQUAL;
    if(s == "//=")
        return TokenType::DOUBLESLASHEQUAL;
    if(s == "...")
        return TokenType::ELLIPSIS;
    return TokenType::UNKNOWN;
}

std::string opToString(const TokenType& tt) {
    switch (tt) {

        case TokenType::AND: return "&&";
        case TokenType::NOT: return "not";
        case TokenType::OR: return "||";
        case TokenType::LPAR: return "(";
        case TokenType::RPAR: return ")";
        case TokenType::LSQB: return "[";
        case TokenType::RSQB: return "]";
        case TokenType::LBRACE: return "{";
        case TokenType::RBRACE: return "}";
        case TokenType::COLON: return ":";
        case TokenType::COMMA: return ",";
        case TokenType::SEMI: return ";";
        case TokenType::PLUS: return "+";
        case TokenType::MINUS: return "-";
        case TokenType::STAR: return "*";
        case TokenType::SLASH: return "/";
        case TokenType::VBAR: return "|";
        case TokenType::AMPER: return "&";
        case TokenType::LESS: return "<";
        case TokenType::GREATER: return ">";
        case TokenType::EQUAL: return "=";
        case TokenType::DOT: return ".";
        case TokenType::PERCENT: return "%";
        case TokenType::CIRCUMFLEX: return "^";
        case TokenType::TILDE: return "~";
        case TokenType::AT: return "@";
        case TokenType::EQEQUAL: return "==";
        case TokenType::NOTEQUAL: return "!=";
        case TokenType::LESSEQUAL: return "<=";
        case TokenType::LEFTSHIFT: return "<<";
        case TokenType::GREATEREQUAL: return ">=";
        case TokenType::RIGHTSHIFT: return ">>";
        case TokenType::PLUSEQUAL: return "+=";
        case TokenType::MINEQUAL: return "-=";
        case TokenType::RARROW: return "->";
        case TokenType::DOUBLESTAR: return "**";
        case TokenType::STAREQUAL: return "*=";
        case TokenType::DOUBLESLASH: return "//";
        case TokenType::SLASHEQUAL: return "/=";
        case TokenType::VBAREQUAL: return "|=";
        case TokenType::PERCENTEQUAL: return "%=";
        case TokenType::AMPEREQUAL: return "&=";
        case TokenType::CIRCUMFLEXEQUAL: return "^=";
        case TokenType::ATEQUAL: return "@=";
        case TokenType::LEFTSHIFTEQUAL: return "<<=";
        case TokenType::RIGHTSHIFTEQUAL: return ">>=";
        case TokenType::DOUBLESTAREQUAL: return "**=";
        case TokenType::DOUBLESLASHEQUAL: return "//=";
        case TokenType::ELLIPSIS: return "...";
        case TokenType::IN: return "in";
        default: return std::to_string(static_cast<std::uint16_t>(tt));
    }
}