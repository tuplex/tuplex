//
// Created by Leonhard Spiegelberg on 2/27/22.
//

#include <visitors/ReplaceConstantTypesVisitor.h>

namespace tuplex {
    ASTNode* ReplaceConstantTypesVisitor::replace(ASTNode *parent, ASTNode *node) {
        using namespace std;

        auto& logger = Logger::instance().logger("codegen");

        // parent must always be set
        assert(parent);

        // next may be an empty field
        if(!node)
            return nullptr;

        if(node->getInferredType().isConstantValued()) {
            // constant valued node! replace!
            auto type = node->getInferredType().underlying();
            auto value = node->getInferredType().constant();

            // is it option type? -> check what the value is! null or None?
            // special case string option: check quotes!
            if(type.isOptionType()) {
                // is it null or not?
                if(value == "null" || value == "None")
                    type = python::Type::NULLVALUE;
                else
                    type = type.elementType(); // valid entry.
            }

            if(python::Type::I64 == type || python::Type::F64 == type) {
                // replace with number node
                return new NNumber(value);
            } else if(python::Type::STRING == type) {
                // check that string is quoted with ''!
                if(value.empty() || value.front() != '\'' || value.back() != '\'')
                    throw std::runtime_error("make sure string constants are escaped using '' ");
                return new NString(value);
            } else if(python::Type::NULLVALUE == type) {
                return new NNone;
            } else if(python::Type::BOOLEAN == type) {
                return new NBoolean(parseBoolString(value));
            } else if(python::Type::EMPTYTUPLE == type) {
                return new NTuple();
            } else if(python::Type::EMPTYLIST == type) {
                return new NList();
            } else if(python::Type::EMPTYDICT == type) {
                return new NDictionary();
            } else {
                logger.warn("Found constant valued type " + node->getInferredType().desc() +
                " , but no optimization for it implemented yet.");
            }
        }

        return node; // no replacement, return node as is
    }
}