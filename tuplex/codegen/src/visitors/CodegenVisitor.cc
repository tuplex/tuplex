//
// Created by leonhards on 4/16/23.
//

#include <visitors/CodegenVisitor.h>

namespace tuplex {
    std::string CodegenVisitor::python_code() const {
        assert(_stack.size() == 1);
        return _stack.front();
    }

    // visit all the nodes & emit python code
    void CodegenVisitor::visit(NNone* node) {
        _stack.push_back("None");
    }

    void CodegenVisitor::visit(NNumber* node) {
        _stack.push_back(node->_value);
    }

    void CodegenVisitor::visit(NIdentifier* id) {
        _stack.push_back(id->_name);
    }

    void CodegenVisitor::visit(NBoolean* b) {
        _stack.push_back(b->_value ? "True" : "False");
    }

    void CodegenVisitor::visit(NEllipsis* node) {
        _stack.push_back("...");
    }

    void CodegenVisitor::visit(NString * str) {
        _stack.push_back(str->raw_value());
    }

    void CodegenVisitor::visit(NParameter* p) {
        ApatheticVisitor::visit(p);

    }

    // @TODO: fill in the rest here... -> that will take forever...

}