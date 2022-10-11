//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CLOSUREENVIRONMENT_H
#define TUPLEX_CLOSUREENVIRONMENT_H

#include <TypeSystem.h>
#include <Field.h>
#include "symbols/SymbolTable.h"

#ifdef BUILD_WITH_CEREAL
#include "cereal/access.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/map.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/common.hpp"
#include "cereal/archives/binary.hpp"
#endif

namespace tuplex {

    class SymbolTable;
    class ClosureEnvironment;

    class ClosureEnvironment {
    public:
        // e.g. import matplotlib as mpl
        // => identifier = mpl
        //    original_identifier = matplotlib
        //    location = /usr/local/lib/python3.9/site-packages/matplotlib/pyplot.py
        // e.g. from os import path
        // => identifier = path
        //    original_identifier = posixpath
        //    location = /usr/local/Cellar/python@3.9/3.9.0_1/Frameworks/Python.framework/Versions/3.9/lib/python3.9/posixpath.py
        struct Module {
            std::string identifier; // what identifier it is visible as
            std::string original_identifier; // original name of module
            std::string package; // to which package it belongs to
            std::string location; // file location

            Module() {}
            Module(const Module& other) = default;
            Module(Module&& other) = default;

            Module& operator = (const Module& other) = default;

#ifdef BUILD_WITH_CEREAL
            template<class Archive> void serialize(Archive &ar) {
                ar(identifier, original_identifier, package, location);
            }
#endif
        };

        struct Constant {
            python::Type type;                  // which type
            std::string identifier;             // which name
            //std::string json_value;          // value (pickled)
            Field value;

            Constant() {}
            Constant(const Constant& other) = default;
            Constant(Constant&& other) = default;
            Constant(const python::Type& type, const std::string& identifier, const Field& value) : type(type), identifier(identifier), value(value) {}
            Constant& operator = (const Constant& other) = default;

#ifdef BUILD_WITH_CEREAL
            template<class Archive> void serialize(Archive &ar) {
                ar(type, identifier, value);
            }
#endif
        };

        struct Function {
            std::string identifier; // under which identifier is this function known
            std::string package; // which module does this function belong to?
            std::string location; // file location
            std::string qualified_name; // full path, i.e. re.search

            Function() {}
            Function(const Function& other) = default;
            Function(Function&& other) = default;

            Function& operator = (const Function& other) = default;

#ifdef BUILD_WITH_CEREAL
            template<class Archive> void serialize(Archive &ar) {
                ar(identifier, package, location, qualified_name);
            }
#endif
        };

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void serialize(Archive &ar) {
            ar(_imported_modules, _globals, _functions);
        }
#endif

        void addConstant(const Constant& c) { _globals.push_back(c); }
        void addModule(const Module& m) { _imported_modules.push_back(m); }
        void addFunction(const Function& f) { _functions.push_back(f); }

        std::vector<Constant> constants() const;

        std::vector<Function> functions() const { return _functions; }
        std::vector<Module> modules() const { return _imported_modules; }

        /*!
         * add names as if the following statement was used: from module import name
         * @param module the name of the module from where to import
         * @param name the name to import
         * @param alias an alias to give the name in the name table
         * @return the environment itself
         */
        inline ClosureEnvironment& fromModuleImportAs(const std::string& module,
                                                      const std::string& name,
                                                      const std::string& alias) {
            // special case is name * ?
            // => then import everything!
            if(name == "*") {
                Module m;
                m.identifier = "*";
                m.location = "unknown";
                m.package = module;
                m.original_identifier = module;
                _imported_modules.emplace_back(m);
                return *this;
            }

            Function f;
            f.identifier = alias;
            f.qualified_name = name;
            f.package = module;
            f.location = "unknown";
            addFunction(f);
            return *this;
        }

        inline ClosureEnvironment& fromModuleImport(const std::string& module, const std::string& name) {
            return fromModuleImportAs(module, name, name);
        }

        ClosureEnvironment& addGlobal(const std::string& identifier, const std::string& value);
        ClosureEnvironment& addGlobal(const std::string& identifier, const char* value) {
            return addGlobal(identifier, std::string(value));
        }
        ClosureEnvironment& addGlobal(const std::string& identifier, const int64_t& value);
        // only define append for long when long and int64_t are not the same to avoid overload error
        template<class T=long>
        typename std::enable_if<!std::is_same<int64_t, T>::value, ClosureEnvironment&>::type addGlobal(const std::string& identifier, const T& value) {
            return addGlobal(identifier, static_cast<int64_t>(value));
        }
        ClosureEnvironment& addGlobal(const std::string& identifier, const double& value);

        void importModuleAs(const std::string& identifier, const std::string& original_identifier);

        inline void importModule(const std::string& identifier) {
            importModuleAs(identifier, identifier);
        }

        std::string desc() const;

        /*!
         * add all dependencies of this closure environment to the symbol table.
         * @param table
         * @return true if everything could been added. False, if entries were not supported. I.e., fallback mode must then be used!
         */
        bool addToTable(SymbolTable& table) const;

    private:
        std::vector<Module> _imported_modules;
        std::vector<Constant> _globals;
        std::vector<Function> _functions;

    public:
        ClosureEnvironment() {}
        ClosureEnvironment(const ClosureEnvironment& other) : _imported_modules(other._imported_modules), _globals(other._globals), _functions(other._functions) {}
        ClosureEnvironment(ClosureEnvironment&& other) : _imported_modules(other._imported_modules), _globals(other._globals), _functions(other._functions) {}

        ClosureEnvironment& operator = (const ClosureEnvironment& other) {
            _imported_modules = other._imported_modules;
            _globals = other._globals;
            _functions = other._functions;
            return *this;
        }
    };
}


#endif //TUPLEX_CLOSUREENVIRONMENT_H
