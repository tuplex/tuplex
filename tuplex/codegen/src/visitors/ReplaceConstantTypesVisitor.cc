//
// Created by Leonhard Spiegelberg on 2/27/22.
//

#include <visitors/ReplaceConstantTypesVisitor.h>

namespace tuplex {

    ASTNode* astNodeFromConstType(const python::Type& t) {
        auto& logger = Logger::instance().logger("codegen");

        if(!t.isConstantValued())
            return nullptr;

        // constant valued node! replace!
        auto type = t.underlying();
        auto value = t.constant();

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
            auto num = new NNumber(value);
            num->setInferredType(t);
            return num;
        } else if(python::Type::STRING == type) {

            // hack
            if(!is_encoded_python_str(value))
                value = escape_to_python_str(value);

            // check that string is quoted with ''!
            if(value.empty() || value.front() != '\'' || value.back() != '\'')
                throw std::runtime_error("make sure string constants are escaped using '' ");
            auto s = new NString(value);
            s->setInferredType(t);
        } else if(python::Type::NULLVALUE == type) {
            return new NNone;
        } else if(python::Type::BOOLEAN == type) {
            auto b = new NBoolean(parseBoolString(value));
            b->setInferredType(t);
            return b;
        } else if(python::Type::EMPTYTUPLE == type) {
            return new NTuple();
        } else if(python::Type::EMPTYLIST == type) {
            return new NList();
        } else if(python::Type::EMPTYDICT == type) {
            return new NDictionary();
        } else {
            logger.warn("Found constant valued type " + t.desc() +
                        " , but no optimization for it implemented yet.");
            return nullptr;
        }
        return nullptr;
    }

    ASTNode* ReplaceConstantTypesVisitor::replace(ASTNode *parent, ASTNode *node) {
        using namespace std;

        auto& logger = Logger::instance().logger("codegen");

        // parent must always be set
        assert(parent);

        // next may be an empty field
        if(!node)
            return nullptr;

        if(node->getInferredType().isConstantValued()) {

            auto constant_type = node->getInferredType();

            // special case: if node is a parameter - DO NOT REPLACE
            if(node->type() == ASTNodeType::Identifier) {
                if(parent && parent->type() == ASTNodeType::Parameter)
                    return node;
            }

            // same true for parameter
            if(node->type() == ASTNodeType::Parameter) {
                return node;
            }

            auto replacement_node = astNodeFromConstType(constant_type);
            if(replacement_node)
                return replacement_node;
        }

        return node; // no replacement, return node as is
    }
}