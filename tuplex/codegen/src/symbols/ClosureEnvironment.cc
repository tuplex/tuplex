//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <symbols/ClosureEnvironment.h>
#include <string>
#include <sstream>
#include <memory>
#include <symbols/StandardModules.h>

namespace tuplex {

    ClosureEnvironment& ClosureEnvironment::addGlobal(const std::string &identifier, const double &value) {
        Constant c;
        c.type = python::Type::F64;
        c.identifier = identifier;
        c.value = Field(value);
        // c.json_value = cJSON_to_string(cJSON_CreateNumber(value));
        _globals.emplace_back(c);
        return *this;
    }

    ClosureEnvironment& ClosureEnvironment::addGlobal(const std::string &identifier, const int64_t &value) {
        Constant c;
        c.type = python::Type::I64;
        c.identifier = identifier;
        c.value = Field(value);
        // c.json_value = cJSON_to_string(cJSON_CreateInt64(value));
        _globals.emplace_back(c);
        return *this;
    }

    ClosureEnvironment& ClosureEnvironment::addGlobal(const std::string &identifier, const std::string &value) {
        Constant c;
        c.type = python::Type::STRING;
        c.identifier = identifier;
        c.value = Field(value);
        //c.json_value = cJSON_to_string(cJSON_CreateString(value.c_str()));
        _globals.emplace_back(c);
        return *this;
    }

    void ClosureEnvironment::importModuleAs(const std::string &identifier, const std::string &original_identifier) {
        assert(identifier != "*"); // TODO: more comprehensive checks here...

        Module m;
        m.identifier = identifier;
        m.original_identifier = original_identifier;
        // leave the others out??
        _imported_modules.emplace_back(m);
    }

    std::string ClosureEnvironment::desc() const {
        using namespace std;
        stringstream ss;

        ss << "imported modules:\n"
           << "---------------\n";
        for (const auto& m : _imported_modules) {
            ss << m.identifier << " (qual_name=" << m.original_identifier
               << ", loc="<<m.location<<", pkg="<<m.package<<")"<<endl;
        }

        ss << "globals:\n"
           << "--------\n";
        for (const auto& c : _globals)
            ss << c.identifier << " = " << c.value.desc() << endl;

        ss<<"functions:\n"
          <<"----------\n";
        for(const auto& f : _functions) {
            ss << f.identifier << " (qual_name=" << f.qualified_name
               << ", loc=" << f.location << ", pkg=" << f.package << ")\n";
        }

        return ss.str();
    }

    static std::shared_ptr<Symbol> getModule(std::string identifier, std::string alias) {
        // Note: If you want to add support for a new built-in module to accelerate, start here.
        if(identifier == "re")
            return module::reModule(alias);
        if(identifier == "math")
            return module::mathModule(alias);
        if(identifier == "random")
            return module::randomModule(alias);
        if(identifier == "string")
            return module::stringModule(alias);
        return nullptr;
    }

    bool ClosureEnvironment::addToTable(SymbolTable &table) const {
        auto& logger = Logger::instance().defaultLogger();
        // add first modules
        for(const auto& m : _imported_modules) {

            // special case: identifier = *, i.e. import everything!
            if(m.identifier == "*") { // syntax not allowed in Python!
                auto modSymbol = getModule(m.original_identifier, m.original_identifier);
                if(!modSymbol) {
                    logger.warn("function requires module " + m.original_identifier + " but Tuplex does not provide support for it yet.");
                } else {
                    // get all attributes..
                    for(auto attr : modSymbol->attributes()) {
                        table.addSymbol(attr);
                    }
                }
                continue;
            }

            // check which (standard-library) modules are supported, else failure!
            auto modSymbol = getModule(m.original_identifier, m.identifier);
            if(!modSymbol) {
                std::string module_desc = m.identifier;
                if(m.original_identifier != m.identifier && !m.original_identifier.empty())
                    module_desc = m.original_identifier + " as " + m.identifier;

                logger.warn("function requires module " + module_desc + " for which Tuplex does not provide support yet. Will use fallback mode.");

                // add a dummy element with unknown type
                auto dummy = std::make_shared<Symbol>(m.identifier, python::Type::MODULE);
                dummy->qualifiedName = m.original_identifier;
                dummy->symbolType = SymbolType::EXTERNAL_PYTHON;
                table.addSymbol(dummy);
                // --> lookup should happen directly through interpreter
            } else {
                table.addSymbol(modSymbol);
            }
            // Note: if we wanted to compile external modules, use a local cache for them!
            //       i.e., we could (partially) compile an external module with certain typing
            //       and cache the results locally for reuse.
        }

        // directly imported functions from modules
        for(const auto& f : _functions) {

            // get module for function, then fetch symbol and alter alias!
            auto modSymbol = getModule(f.package, f.package);
            if(!modSymbol) {
                logger.warn("Module " + f.package + " native support required to compile function. Tuplex does not support it yet, using fallback.");
                // add a dummy
                // --> lookup should happen directly through interpreter
                // add a dummy element with unknown type
                auto dummy = std::make_shared<Symbol>(f.package, python::Type::MODULE);
                dummy->qualifiedName = f.package;
                dummy->symbolType = SymbolType::EXTERNAL_PYTHON;
                dummy->addAttribute(std::make_shared<Symbol>(f.qualified_name, f.qualified_name, python::Type::UNKNOWN, SymbolType::VARIABLE));
                table.addSymbol(dummy);

                auto func_symbol = dummy->findAttribute(f.qualified_name); assert(func_symbol);
                func_symbol->name = f.identifier;
                func_symbol->symbolType = SymbolType::EXTERNAL_PYTHON;
                table.addSymbol(func_symbol);
            } else {

                // it's a from ... import ... as ... statement
                // i.e. retrieve the attribute from the module!
                auto func_symbol = modSymbol->findAttribute(f.qualified_name);
                if(!func_symbol) {
                    logger.warn("Could not find function " + f.qualified_name + " in module " + f.package + " natively supported by Tuplex, using fallback.");
                    return false;
                }
                func_symbol->name = f.identifier;
                table.addSymbol(func_symbol);
            }
        }

        // overwrite with globals, replacing identifiers!
        for(const auto& var : _globals) {
            assert(var.value.getType() == var.type);
            table.addSymbol(Symbol::makeConstant(var.identifier, var.value));

            // old:
            //table.addSymbol(var.identifier, var.type);
        }

        return true;
    }

    std::vector<ClosureEnvironment::Constant> ClosureEnvironment::constants() const {
        std::vector<Constant> c = _globals;
        return c;
    }
}