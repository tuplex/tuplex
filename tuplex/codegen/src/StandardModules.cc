//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <StandardModules.h>
#include <Field.h>


// math constant definitions
// taken from https://github.com/python/cpython/blob/63298930fb531ba2bb4f23bc3b915dbf1e17e9e1/Include/pymath.h
// note that CPython developers simply copied the constants...
#ifndef Py_MATH_PI
//#define Py_MATH_PI 3.14159265358979323846
#define Py_MATH_PI M_PI
#endif
#ifndef Py_MATH_E
//#define Py_MATH_E 2.7182818284590452354
#define Py_MATH_E M_E
#endif
/* Tau (2pi) to 40 digits, taken from tauday.com/tau-digits. */
#ifndef Py_MATH_TAU
#define Py_MATH_TAU 6.2831853071795864769252867665590057683943L
#endif

namespace tuplex {
    namespace module {
        /*!
         * definitions for math module, all supported symbols and their typing.
         * @param alias
         * @return
         */
        std::shared_ptr<Symbol> mathModule(std::string alias) {
            using namespace std;
            assert(!alias.empty());

            // TODO: add the widely used constants inf, pi, ...

            auto m = make_shared<Symbol>(alias, python::Type::MODULE);
            m->qualifiedName = "math";

            // works for all numeric types
            vector<python::Type> numeric_types{python::Type::BOOLEAN, python::Type::I64, python::Type::F64};
            for(const auto& type : numeric_types) {
                vector<string> names{"sin", "cos", "sqrt", "asin", "exp", "log", "log1p", "log2", "log10", "expm1", "asinh", "sinh", "cosh", "acos", "tanh", "atan", "atanh", "radians", "degrees", "acosh"};
                for(const auto& second_type : numeric_types) {
                    m->addAttribute(make_shared<Symbol>("pow", "pow", python::Type::makeFunctionType(python::Type::makeTupleType({type, second_type}), python::Type::F64), SymbolType::FUNCTION));
                    m->addAttribute(make_shared<Symbol>("atan2", "atan2", python::Type::makeFunctionType(python::Type::makeTupleType({type, second_type}), python::Type::F64), SymbolType::FUNCTION));
                }
                for(const auto& name : names) {
                    m->addAttribute(make_shared<Symbol>(name, name, python::Type::makeFunctionType(python::Type::propagateToTupleType(type), python::Type::F64), SymbolType::FUNCTION));
                }
            }
            
            // math.isnan
            auto isnanSym = make_shared<Symbol>("isnan", "isnan", python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::F64), python::Type::BOOLEAN), SymbolType::FUNCTION);
            isnanSym->addTypeIfNotExists(python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::I64), python::Type::BOOLEAN));
            isnanSym->addTypeIfNotExists(python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::BOOLEAN), python::Type::BOOLEAN));
            m->addAttribute(isnanSym);

            // math.isinf
            auto isinfSym = make_shared<Symbol>("isinf", "isinf", python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::F64), python::Type::BOOLEAN), SymbolType::FUNCTION);
            isinfSym->addTypeIfNotExists(python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::I64), python::Type::BOOLEAN));
            isinfSym->addTypeIfNotExists(python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::BOOLEAN), python::Type::BOOLEAN));
            m->addAttribute(isinfSym);

            // math.isclose
            // typing is:
            // ({f64, i64, bool}, {f64, i64, bool}, rel_tol={f64, i64, bool}, abs_tol={f64, i64, bool}) -> bool
            vector<python::Type> isclose_types{python::Type::BOOLEAN, python::Type::I64, python::Type::F64};
            auto iscloseSym = make_shared<Symbol>("isclose", "isclose", python::Type::makeFunctionType(python::Type::makeTupleType({python::Type::F64, python::Type::F64, python::Type::F64, python::Type::F64}), python::Type::BOOLEAN), SymbolType::FUNCTION);
            iscloseSym->addTypeIfNotExists(python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::I64), python::Type::BOOLEAN));
            iscloseSym->addTypeIfNotExists(python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::BOOLEAN), python::Type::BOOLEAN));
            m->addAttribute(iscloseSym);

            // math.ceil/math.floor
            for(const auto& name : vector<string>{"ceil", "floor"}) {
                // types here are:
                // f64 -> i64
                // i64 -> i64 (identity function!)
                // boolean -> i64
                auto sym = make_shared<Symbol>(name, name, python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::F64), python::Type::I64), SymbolType::FUNCTION);
                sym->addTypeIfNotExists(python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::I64), python::Type::I64));
                sym->addTypeIfNotExists(python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::BOOLEAN), python::Type::I64));
                m->addAttribute(sym);
            }

            // Constants:
            // math.pi, math.e, math.tau, math.inf, math.nan symbols
            // inf/nan since version 3.5
            // tau since version 3.6
            // use here C constants. Note, python might have depending on version different constants.
            m->addAttribute(Symbol::makeConstant("nan", Field(NAN)));
            m->addAttribute(Symbol::makeConstant("inf", Field(INFINITY)));
            m->addAttribute(Symbol::makeConstant("pi", Field(Py_MATH_PI))); // C constant M_PI
            m->addAttribute(Symbol::makeConstant("e", Field(Py_MATH_E))); // C constant M_E
            m->addAttribute(Symbol::makeConstant("tau", Field(static_cast<double>(Py_MATH_TAU))));

            return m;
        }

        /*!
         * random module from python standard lib
         * @param alias
         * @return
         */
        std::shared_ptr<Symbol> randomModule(std::string alias) {
            using namespace std;
            assert(!alias.empty());

            auto m = make_shared<Symbol>(alias, python::Type::MODULE);
            m->qualifiedName = "random";

            // string version
            m->addAttribute(make_shared<Symbol>("choice", "choice", python::Type::makeFunctionType(python::Type::STRING, python::Type::STRING), SymbolType::FUNCTION));

            // list versions
            std::vector<python::Type> list_types = {python::Type::BOOLEAN, python::Type::I64, python::Type::F64, python::Type::STRING, python::Type::EMPTYDICT, python::Type::EMPTYTUPLE, python::Type::NULLVALUE};
            for(const auto &t : list_types) {
                m->addAttribute(make_shared<Symbol>("choice", "choice", python::Type::makeFunctionType(python::Type::makeListType(t), t), SymbolType::FUNCTION));
            }
            return m;
        }

        std::shared_ptr<Symbol> reModule(std::string alias) {
            using namespace std;
            assert(!alias.empty());

            auto re = make_shared<Symbol>(alias, python::Type::MODULE);
            re->qualifiedName = "re";
            auto re_search_sym = make_shared<Symbol>("search", "search", python::Type::makeFunctionType(python::Type::makeTupleType({python::Type::STRING, python::Type::STRING}), python::Type::makeOptionType(python::Type::MATCHOBJECT)), SymbolType::FUNCTION);
            auto re_sub_sym = make_shared<Symbol>("sub", "sub", python::Type::makeFunctionType(python::Type::makeTupleType({python::Type::STRING, python::Type::STRING, python::Type::STRING}), python::Type::STRING), SymbolType::FUNCTION);

            re->addAttribute(re_search_sym);
            re->addAttribute(re_sub_sym);

            return re;
        }

        std::shared_ptr<Symbol> stringModule(std::string alias) {
            using namespace std;
            assert(!alias.empty());

            auto m = make_shared<Symbol>(alias, python::Type::MODULE);
            m->qualifiedName = "string";

            m->addAttribute(make_shared<Symbol>("capwords", "capwords", python::Type::makeFunctionType(python::Type::propagateToTupleType(python::Type::STRING), python::Type::STRING), SymbolType::FUNCTION));

            return m;
        }
    }
}