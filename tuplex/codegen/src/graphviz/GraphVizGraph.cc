//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <graphviz/GraphVizGraph.h>
#include <regex>
#include <TSet.h>

namespace tuplex {
    // helper function
    std::string setToString(const TSet<python::Type>& S) {
        std::string s = "\\{";

        for(int i = 0; i < S.size(); i++) {
            s += S[i].desc();
            if(i != S.size() - 1)
                s += ", ";
        }

        return s + "\\}";
    }

// glue code logic
    void GraphVizGraph::createFromAST(ASTNode *root, bool withTypes) {
        _astVisitor.showTypes(withTypes);
        root->accept(_astVisitor);
        _astGraphCreated = true;
    }

    bool GraphVizGraph::saveAsDot(const std::string &path) {
        if(_astGraphCreated) {
            return _astVisitor.getBuilder()->saveToDotFile(path);
        }
            // log here...
        else return false;
    }

    bool GraphVizGraph::saveAsPDF(const std::string &path) {
        if(_astGraphCreated) {
            return _astVisitor.getBuilder()->saveToPDF(path);
        }
            // log here...
        else return false;
    }

// note: B.c. here a visitor pattern is used
// the usual recursive tree walking is replaced with a stack and a returnval
// i.e. _ids is the call stack of ids
// and lastid hold the "return value" of the lastly evaluated "visit" function

    std::string GraphVizGraph::GraphVizVisitor::typeStr(ASTNode *node) {
        if(!_showTypes)
            return "";

        //// inferred type
        //// need to escape strings for html nodes...

        std::string str = " <" + node->getInferredType().desc() + ">";

        // escape chars
        str = regex_replace(regex_replace(str, std::regex("<"), "&lt;"), std::regex(">"), "&gt;");

        return str;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NNumber* number) {
        int id = _builder->addNode("Number : " + number->_value + typeStr(number));
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NIdentifier *identifier) {

        int id = _builder->addNode("Identifier : " + identifier->_name + typeStr(identifier));
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NBoolean *boolean) {
        int iParent = getParent();

        std::string value = "";
        if(boolean->_value)value = "True";
        else value = "False";

        int id = _builder->addNode("Boolean : " + value + typeStr(boolean));
        if(iParent >= 0)
            _builder->addEdge(iParent, id);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NEllipsis *ellipsis) {
        int iParent = getParent();
        int id = _builder->addNode("..." + typeStr(ellipsis));
        if(iParent >= 0)
            _builder->addEdge(iParent, id);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NNone* none) {
        int iParent = getParent();
        int id = _builder->addNode("None" + typeStr(none));
        if(iParent >= 0)
            _builder->addEdge(iParent, id);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NBreak* node) {
        int iParent = getParent();
        int id = _builder->addNode("break");
        if(iParent >= 0)
            _builder->addEdge(iParent, id);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NContinue* node) {
        int iParent = getParent();
        int id = _builder->addNode("continue");
        if(iParent >= 0)
            _builder->addEdge(iParent, id);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NString *string) {
        int iParent = getParent();
        int id = _builder->addNode("String : " + string->value() + typeStr(string));
        if(iParent >= 0)
            _builder->addEdge(iParent, id);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NBinaryOp *binop) {
        int id = _builder->addNode("BinaryOp: " +opToString(binop->_op) + typeStr(binop));
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        // recursively call nodes
        assert(binop->_left);
        assert(binop->_right);

        // note: order matters here, b.c. whatever node is added first is most left
        // call left to right
        // note that id needs to be pushed twice on stack, b.c. getParent removes it!
        _ids.push(id);
        binop->_left->accept(*this);
        _ids.push(id);
        binop->_right->accept(*this);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NUnaryOp *unop) {
        int id = _builder->addNode("UnaryOp: " + opToString(unop->_op) + typeStr(unop));
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        // recursively call nodes
        assert(unop->_operand);
        _ids.push(id);
        unop->_operand->accept(*this);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NSuite *suite) {
        int id = _builder->addNode("suite" + typeStr(suite));
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        // recursively call nodes
        // note: order matters here, b.c. whatever node is added first is most left
        // call left to right
        for(auto it = suite->_statements.begin(); it != suite->_statements.end(); ++it) {
            assert(*it);

            _ids.push(id);
            (*it)->accept(*this);
        }
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NModule *module) {
        int id = _builder->addNode("module" + typeStr(module));
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        assert(module->_suite);

        // push id before recursive call
        _ids.push(id);
        module->_suite->accept(*this);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NParameter *param) {
        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"#48B887\" COLSPAN=\"3\">parameter";
        html += typeStr(param);
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"arg\">name</TD><TD PORT=\"def\">default value</TD><TD PORT=\"ann\">annotation</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        assert(param->_identifier);
        _ids.push(-1);
        param->_identifier->accept(*this);
        int idArg = _lastId;
        _builder->addEdge(id, idArg, "arg");

        if(param->_default) {
            _ids.push(-1);
            param->_default->accept(*this);
            int idDefault = _lastId;
            _builder->addEdge(id, idDefault, "def");
        }

        if(param->_annotation) {
            _ids.push(-1);
            param->_annotation->accept(*this);
            int idAnnotation = _lastId;
            _builder->addEdge(id, idAnnotation, "ann");
        }
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NParameterList *pl) {
        int id = _builder->addNode("parameter list" + typeStr(pl));
        int iParent = getParent();
        if (iParent >= 0)
            _builder->addEdge(iParent, id);

        for(auto it = pl->_args.begin(); it != pl->_args.end(); ++it) {
            assert(*it);

            _ids.push(id);
            (*it)->accept(*this);
        }
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NLambda *lambda) {
        int id = _builder->addNode("lambda" + typeStr(lambda));
        int iParent = getParent();
        if (iParent >= 0)
            _builder->addEdge(iParent, id);

        if(lambda->_arguments) {
            _ids.push(id);
            lambda->_arguments->accept(*this);
        }

        assert(lambda->_expression);
        _ids.push(id);
        lambda->_expression->accept(*this);

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NAwait *await) {
        int id = _builder->addNode("await" + typeStr(await));
        int iParent = getParent();
        if (iParent >= 0)
            _builder->addEdge(iParent, id);

        assert(await->_target);
        _ids.push(id);
        await->_target->accept(*this);

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NStarExpression *se) {
        int id = _builder->addNode("*(...)" + typeStr(se));
        int iParent = getParent();
        if (iParent >= 0)
            _builder->addEdge(iParent, id);

        assert(se->_target);
        _ids.push(id);
        se->_target->accept(*this);

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NCompare *cmp) {

        int colspan = 1 + cmp->_comps.size() + cmp->_ops.size();
        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"#CD5C5C\" COLSPAN=\"" + std::to_string(colspan) + "\">cmp";
        html += typeStr(cmp);
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n";

        // add as many ports as necessary (+1 for the left)
        html += "<TD PORT=\"op0\">op0</TD>";
        for(int i = 0; i < cmp->_ops.size(); ++i) {

            auto op = cmp->_ops.at(i);
            html +="<TD PORT=\"cmp" + std::to_string(i) + "\">" + escapeHTML(opToString(op)) + "</TD>";
            html +="<TD PORT=\"op" + std::to_string(i + 1) + "\">op" +  std::to_string(i + 1) + "</TD>";
        }

        html += "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);

        // add edge to parent
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);


        // connect left
        _ids.push(-1);
        cmp->_left->accept(*this);
        int idName = _lastId;
        _builder->addEdge(id, idName, "op0");

        for(int i = 0; i < cmp->_comps.size(); ++i) {
            auto c = cmp->_comps[i];
            _ids.push(-1);
            c->accept(*this);
            int idParams = _lastId;
            _builder->addEdge(id, idParams, "op" + std::to_string(i + 1));
        }

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NIfElse *ifelse) {
        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"#90EE90\" COLSPAN=\"3\">if";

        // only include typestr for expression if
        if(ifelse->isExpression())
            html += typeStr(ifelse);
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"cond\">condition</TD><TD PORT=\"then\">then</TD><TD PORT=\"else\">else</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        assert(ifelse->_expression);
        assert(ifelse->_then);

        _ids.push(-1);
        ifelse->_expression->accept(*this);
        int idCond = _lastId;
        _builder->addEdge(id, idCond, "cond");

        _ids.push(-1);
        ifelse->_then->accept(*this);
        int idThen = _lastId;
        _builder->addEdge(id, idThen, "then");

        if(ifelse->_else) {
            _ids.push(-1);
            ifelse->_else->accept(*this);
            int idElse = _lastId;
            _builder->addEdge(id, idElse, "else");
        }

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NFunction *func) {
        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightblue\" COLSPAN=\"4\">function";
        html += typeStr(func);
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"name\">name</TD><TD PORT=\"params\">parameters</TD><TD PORT=\"ann\">annotation</TD><TD PORT=\"suite\">body</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        assert(func->_name);
        assert(func->_suite);

        _ids.push(-1);
        func->_name->accept(*this);
        int idName = _lastId;
        _builder->addEdge(id, idName, "name");

        if(func->_parameters) {
            _ids.push(-1);
            func->_parameters->accept(*this);
            int idParams = _lastId;
            _builder->addEdge(id, idParams, "params");
        }

        if(func->_annotation) {
            _ids.push(-1);
            func->_annotation->accept(*this);
            int idAnnotation = _lastId;
            _builder->addEdge(id, idAnnotation, "ann");
        }

        _ids.push(-1);
        func->_suite->accept(*this);
        int idSuite = _lastId;
        _builder->addEdge(id, idSuite, "suite");

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NTuple *tuple) {
        int id = _builder->addNode("tuple" + typeStr(tuple));
        int iParent = getParent();
        if (iParent >= 0)
            _builder->addEdge(iParent, id);

        for(auto it = tuple->_elements.begin(); it != tuple->_elements.end(); ++it) {
            assert(*it);

            _ids.push(id);
            (*it)->accept(*this);
        }
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NList *list) {
        int id = _builder->addNode("list" + typeStr(list));
        int iParent = getParent();
        if (iParent >= 0)
            _builder->addEdge(iParent, id);

        for(auto it = list->_elements.begin(); it != list->_elements.end(); ++it) {
            assert(*it);

            _ids.push(id);
            (*it)->accept(*this);
        }
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NDictionary *dict) {
        int id = _builder->addNode("dictionary" + typeStr(dict));
        int iParent = getParent();
        if (iParent >= 0)
            _builder->addEdge(iParent, id);

        for(auto it = dict->_pairs.begin(); it != dict->_pairs.end(); ++it) {
            assert(it->first);
            _ids.push(id);
            (it->first)->accept(*this);

            assert(it->second);
            _ids.push(id);
            (it->second)->accept(*this);
        }
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NReturn* ret) {

        // @Todo: verify this here is correct...
        int id = _builder->addNode("return " + typeStr(ret));
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        // recursively call nodes
        if(ret->_expression) {
            _ids.push(id);
            ret->_expression->accept(*this);
        }
        _lastId = id;
    }

    std::string GraphVizGraph::GraphVizVisitor::escapeHTML(const std::string &s) {
        // from https://stackoverflow.com/questions/5665231/most-efficient-way-to-escape-xml-html-in-c-string/5665377#5665377
        std::string buffer;
        buffer.reserve(s.size());
        for(size_t pos = 0; pos != s.size(); ++pos) {
            switch(s[pos]) {
                case '&':  buffer.append("&amp;");       break;
                case '\"': buffer.append("&quot;");      break;
                case '\'': buffer.append("&apos;");      break;
                case '<':  buffer.append("&lt;");        break;
                case '>':  buffer.append("&gt;");        break;
                default:   buffer.append(&s[pos], 1); break;
            }
        }
        return buffer;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NSubscription *sub) {
        assert(sub->_value);
        assert(sub->_expression);

        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightblue\" COLSPAN=\"2\">[]";
        html += typeStr(sub);
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"value\">value</TD><TD PORT=\"index\">index</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        _ids.push(-1);
        sub->_value->accept(*this);
        int idValue = _lastId;
        _builder->addEdge(id, idValue, "value");

        _ids.push(-1);
        sub->_expression->accept(*this);
        int idIndex = _lastId;
        _builder->addEdge(id, idIndex, "index");

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NAssign* as) {
#warning "change this later when adding full support for assignment statement"

        int id = _builder->addNode("Assign(=): " + typeStr(as));
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        // recursively call nodes
        assert(as->_target);
        assert(as->_value);

        // note: order matters here, b.c. whatever node is added first is most left
        // call left to right
        // note that id needs to be pushed twice on stack, b.c. getParent removes it!
        _ids.push(id);
        as->_target->accept(*this);
        _ids.push(id);
        as->_value->accept(*this);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NCall* call) {
#warning "order of args displayed is not correct..."

        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightblue\" COLSPAN=\"4\">call";
        html += typeStr(call);
        html += "</TD></TR><TR>";

        // use Python 3.5 layout

        html += "<TD PORT=\"function\">function</TD><TD PORT=\"args\">args</TD><TD PORT=\"keywords\">keywords</TD></TR></TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);



        _ids.push(-1);
        call->_func->accept(*this);
        int idName = _lastId;
        _builder->addEdge(id, idName, "function");

        if(!call->_positionalArguments.empty()) {
            for(auto it = call->_positionalArguments.begin();
                it != call->_positionalArguments.end();
                ++it) {
                _ids.push(-1);
                auto arg = *it;
                arg->accept(*this);
                int idArg = _lastId;
                _builder->addEdge(id, idArg, "args");
            }
        }

        // no support for keyword args yet...

        // ret id
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NAttribute* attr) {
        int id = _builder->addNode("Attribute(.): " + typeStr(attr));
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        // recursively call nodes
        assert(attr->_value);
        assert(attr->_attribute);

        // note: order matters here, b.c. whatever node is added first is most left
        // call left to right
        // note that id needs to be pushed twice on stack, b.c. getParent removes it!
        _ids.push(id);
        attr->_value->accept(*this);
        _ids.push(id);
        attr->_attribute->accept(*this);
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NSlice *slicing) {
        assert(slicing->_value);

        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightblue\" COLSPAN=\"2\">[]";
        html += typeStr(slicing);
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"value\">value</TD><TD PORT=\"slices\">slices</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        _ids.push(-1);
        slicing->_value->accept(*this);
        int idValue = _lastId;
        _builder->addEdge(id, idValue, "value");

        _ids.push(-1);
        int sliceID = _builder->addNode("slices");
        _builder->addEdge(id, sliceID, "slices");

        for (int i = 0; i < slicing->_slices.size(); ++i) {
            assert(slicing->_slices[i]);

            _ids.push(-1);
            (slicing->_slices[i])->accept(*this);
            idValue = _lastId;
            _builder->addEdge(sliceID, idValue);
        }

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NSliceItem *slicingItem) {
        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightblue\" COLSPAN=\"3\">[]";
        html += typeStr(slicingItem);
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"start\">start</TD><TD PORT=\"end\">end</TD><TD PORT=\"stride\">stride</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        if (slicingItem->_start) {
            _ids.push(-1);
            slicingItem->_start->accept(*this);
            int idValue = _lastId;
            _builder->addEdge(id, idValue, "start");
        }

        if (slicingItem->_end) {
            _ids.push(-1);
            slicingItem->_end->accept(*this);
            int idValue = _lastId;
            _builder->addEdge(id, idValue, "end");
        }

        if (slicingItem->_stride) {
            _ids.push(-1);
            slicingItem->_stride->accept(*this);
            int idValue = _lastId;
            _builder->addEdge(id, idValue, "stride");
        }

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NRange* range) {
#warning "order of args displayed is not correct..."

        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightblue\" COLSPAN=\"4\">range";
        html += typeStr(range);
        html += "</TD></TR><TR>";

        // use Python 3.5 layout

        html += "<TD PORT=\"function\">function</TD><TD PORT=\"args\">args</TD><TD PORT=\"keywords\">keywords</TD></TR></TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        if(!range->_positionalArguments.empty()) {
            for(auto it = range->_positionalArguments.begin();
                it != range->_positionalArguments.end();
                ++it) {
                _ids.push(-1);
                auto arg = *it;
                arg->accept(*this);
                int idArg = _lastId;
                _builder->addEdge(id, idArg, "args");
            }
        }

        // ret id
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NComprehension *comprehension) {
        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightblue\" COLSPAN=\"4\">comprehension";
        html += typeStr(comprehension);
        html += "</TD></TR><TR>";

        // use Python 3.5 layout

        html += "<TD PORT=\"target\">target</TD><TD PORT=\"iter\">iter</TD><TD PORT=\"if_conditions\">if_conditions</TD></TR></TABLE>";

        int id = _builder->addHTMLNode(html);
//    int iParent = getParent();
//    if (iParent >= 0)
//        _builder->addEdge(iParent, id);

        if (comprehension->target) {
            _ids.push(-1);
            comprehension->target->accept(*this);
            int idValue = _lastId;
            _builder->addEdge(id, idValue, "target");
        }

        if (comprehension->iter) {
            _ids.push(-1);
            comprehension->iter->accept(*this);
            int idValue = _lastId;
            _builder->addEdge(id, idValue, "iter");
        }

        for(auto it = comprehension->if_conditions.begin(); it != comprehension->if_conditions.end(); ++it) {
            assert(*it);

            _ids.push(id);
            (*it)->accept(*this);
            int idArg = _lastId;
            _builder->addEdge(id, idArg, "if_conditions");
        }
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NListComprehension *listComprehension) {
        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightblue\" COLSPAN=\"3\">list comprehension";
        html += typeStr(listComprehension);
        html += "</TD></TR><TR>";

        // use Python 3.5 layout

        html += "<TD PORT=\"expression\">expression</TD><TD PORT=\"generators\">generators</TD></TR></TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if (iParent >= 0)
            _builder->addEdge(iParent, id);

        if (listComprehension->expression) {
            _ids.push(-1);
            listComprehension->expression->accept(*this);
            int idValue = _lastId;
            _builder->addEdge(id, idValue, "expression");
        }

        for(auto it = listComprehension->generators.begin(); it != listComprehension->generators.end(); ++it) {
            assert(*it);

            _ids.push(id);
            (*it)->accept(*this);
            int idArg = _lastId;
            _builder->addEdge(id, idArg, "generators");
        }
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NAssert *as) {

        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightgrey\" COLSPAN=\"2\">assert";
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"expression\">expression</TD><TD PORT=\"errorExpression\">errorExpression</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);


        if(as->_expression) {
            _ids.push(-1);
            as->_expression->accept(*this);
            int idExp = _lastId;
            _builder->addEdge(id, idExp, "expression");
        }

        if(as->_errorExpression) {
            _ids.push(-1);
            as->_errorExpression->accept(*this);
            int idErrExp = _lastId;
            _builder->addEdge(id, idErrExp, "errorExpression");
        }


        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NRaise *rs) {

        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"lightgrey\" COLSPAN=\"2\">raise ";
        html += typeStr(rs->_expression);
        if(rs->_fromExpression)
            html += " from " + typeStr(rs->_fromExpression);
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"expression\">expression</TD><TD PORT=\"fromExpression\">fromExpression</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        if(rs->_expression) {
            _ids.push(-1);
            rs->_expression->accept(*this);
            int idExp = _lastId;
            _builder->addEdge(id, idExp, "expression");
        }

        if(rs->_fromExpression) {
            _ids.push(-1);
            rs->_fromExpression->accept(*this);
            int idErrExp = _lastId;
            _builder->addEdge(id, idErrExp, "fromExpression");
        }

        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NWhile *node) {
        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"#48B887\" COLSPAN=\"3\">parameter";
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"expression\">expression</TD><TD PORT=\"body\">body</TD><TD PORT=\"else\">else</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        assert(node->expression);
        _ids.push(-1);
        node->expression->accept(*this);
        int idExpression = _lastId;
        _builder->addEdge(id, idExpression, "expression");

        assert(node->suite_body);
        _ids.push(-1);
        node->suite_body->accept(*this);
        int idBody = _lastId;
        _builder->addEdge(id, idBody, "body");

        if(node->suite_else) {
            _ids.push(-1);
            node->suite_else->accept(*this);
            int idElse = _lastId;
            _builder->addEdge(id, idElse, "else");
        }
        _lastId = id;
    }

    void GraphVizGraph::GraphVizVisitor::visit(NFor *node) {
        std::string html = "<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\">\n"
                           "   <TR>\n"
                           "    <TD BGCOLOR=\"#48B887\" COLSPAN=\"4\">parameter";
        html += "</TD>\n"
                "   </TR>\n"
                "   <TR>\n"
                "    <TD PORT=\"target\">target</TD><TD PORT=\"expression\">expression</TD><TD PORT=\"body\">body</TD><TD PORT=\"else\">else</TD>\n"
                "   </TR>\n"
                "  </TABLE>";

        int id = _builder->addHTMLNode(html);
        int iParent = getParent();
        if(iParent >= 0)
            _builder->addEdge(iParent, id);

        assert(node->target);
        _ids.push(-1);
        node->target->accept(*this);
        int idTarget = _lastId;
        _builder->addEdge(id, idTarget, "target");

        assert(node->expression);
        _ids.push(-1);
        node->expression->accept(*this);
        int idExpression = _lastId;
        _builder->addEdge(id, idExpression, "expression");

        assert(node->suite_body);
        _ids.push(-1);
        node->suite_body->accept(*this);
        int idBody = _lastId;
        _builder->addEdge(id, idBody, "body");

        if(node->suite_else) {
            _ids.push(-1);
            node->suite_else->accept(*this);
            int idElse = _lastId;
            _builder->addEdge(id, idElse, "else");
        }
        _lastId = id;
    }
}