//
// Created by Leonhard Spiegelberg on 2/27/22.
//

#include <ReplaceConstantTypesVisitor.h>

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

            if(python::Type::I64 == type || python::Type::F64 == type) {
                // replace with number node
                return new NNumber(value);
            } else if(python::Type::STRING == type) {
                return new NString(value);
            } else {
                logger.info("Found constant valued type " + node->getInferredType().desc() +
                " , but no optimization for it implemented yet.");
            }
        }


        return node; // no replacement, return node as is
    }
}