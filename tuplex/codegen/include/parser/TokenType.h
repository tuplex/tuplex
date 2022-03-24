//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TOKENTYPE_H
#define TUPLEX_TOKENTYPE_H

#undef FALSE
#undef TRUE

enum class TokenType {
    UNKNOWN,
    ERROR,
    // general token types
    NUMBER,
    STRING,
    IDENTIFIER,
    // file formatting related markers
    ENDOFFILE,
    NEWLINE,
    INDENT,
    DEDENT,
    COMMENT,
    // Python keywords
    FALSE,
    NONE,
    TRUE,
    AND,
    AS,
    ASSERT,
    BREAK,
    CLASS,
    CONTINUE,
    DEF,
    DEL,
    ELIF,
    ELSE,
    EXCEPT,
    FINALLY,
    FOR,
    FROM,
    GLOBAL,
    IF,
    IMPORT,
    IN,
    NOTIN,
    IS,
    ISNOT,
    LAMBDA,
    NONLOCAL,
    NOT,
    OR,
    PASS,
    RAISE,
    RETURN,
    TRY,
    WHILE,
    WITH,
    YIELD,
    // operators (includes parentheses etc.)
    // one character operators
    LPAR, // (
    RPAR, // )
    LSQB, // [
    RSQB, // ]
    LBRACE, // {
    RBRACE, // }
    COLON, // :
    COMMA, // ,
    SEMI, // ;
    PLUS, // +
    MINUS, // -
    STAR, // *
    SLASH, // /
    VBAR, // |
    AMPER, // &
    LESS, // <
    GREATER, // >
    EQUAL, // ==
    DOT, // .
    PERCENT, // %
    CIRCUMFLEX, // ^
    TILDE, // ~
    AT, // @
    // two character operators
    EQEQUAL, // ==
    NOTEQUAL, // != or <>
    LESSEQUAL, // <=
    LEFTSHIFT, // <<
    GREATEREQUAL, // >=
    RIGHTSHIFT, // >>
    PLUSEQUAL, // +=
    MINEQUAL, // -=
    RARROW, // ->
    DOUBLESTAR, // **
    STAREQUAL, // *=
    DOUBLESLASH, // //
    SLASHEQUAL, // /=
    VBAREQUAL, // |=
    PERCENTEQUAL, // %=
    AMPEREQUAL, // &=
    CIRCUMFLEXEQUAL, // ^=
    ATEQUAL, // @=
    // three character operators
    LEFTSHIFTEQUAL, // <<=
    RIGHTSHIFTEQUAL, // >>=
    DOUBLESTAREQUAL, // **=
    DOUBLESLASHEQUAL, // //=
    ELLIPSIS // ...
};


/*!
 * tokens that define a bit operation (used in type inference)
 * @param tt
 * @return
 */
inline bool isBitOperation(const TokenType tt) {
    if(tt == TokenType::VBAR)
        return true;
    if(tt == TokenType::CIRCUMFLEX)
        return true;
    if(tt == TokenType::AMPER)
        return true;
    if(tt == TokenType::LEFTSHIFT) // always int
        return true;
    if(tt == TokenType::RIGHTSHIFT) // always int
        return true;
    if(tt == TokenType::TILDE) // always int
        return true;

    return false;
}

/*!
 * tokens that define an arithmetic operation (used in type inference)
 * @param tt
 * @return
 */
inline bool isArithmeticOperation(const TokenType tt) {
    if(tt == TokenType::PLUS) // +
        return true;
    if(tt == TokenType::MINUS) // -
        return true;
    if(tt == TokenType::STAR) // *
        return true;
    if(tt == TokenType::SLASH) // /
        return true;
    if(tt == TokenType::PERCENT) // % modulo
        return true;

    return false;
}

#endif //TUPLEX_TOKENTYPE_H