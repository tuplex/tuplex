//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_TOKEN_H
#define TUPLEX_TOKEN_H

#include <TokenType.h>
#include <string>
#include <unordered_map>
#include <iostream>
#include <TypeSystem.h>

std::ostream& operator<< (std::ostream& os, const TokenType tt);

extern std::string opToString(const TokenType& tt);

extern TokenType stringToToken(const std::string& s);

class Token {
private:
    TokenType _type;
    // line number (0 based)
    long long _line;
    // column number (0 based)
    long long _col;
    std::string _token;

    // type hint field. If different than unknown, the lexer already filled it!
    python::Type _typeHint;

public:
    Token():_type(TokenType::UNKNOWN), _line(-1), _col(-1), _token(""), _typeHint(python::Type::UNKNOWN) {}
    Token(TokenType type,
            long long line,
            long long col,
            std::string token,
            python::Type typeHint = python::Type::UNKNOWN) : _type(type), _line(line), _col(col), _token(token), _typeHint(typeHint) {}

    Token(const Token& other) {
        _type = other._type;
        _line = other._line;
        _col = other._col;
        _token = other._token;
        _typeHint = other._typeHint;
    }

    TokenType getType() const           { return _type; }

    python::Type getTypeHint() const    { return _typeHint; }

    std::string getRawToken() const     { return _token; }

    int getLineNumber() const { return _line; }
    int getColumn() const { return _col; }
};

#endif //TUPLEX_TOKEN_H