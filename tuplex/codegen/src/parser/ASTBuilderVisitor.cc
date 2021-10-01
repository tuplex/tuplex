//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <parser/ASTBuilderVisitor.h>
#include <Token.h>

namespace tuplex {

    using namespace antlr4;

    antlrcpp::Any ASTBuilderVisitor::visitAtom(Python3Parser::AtomContext *ctx) {
        auto res = visitChildren(ctx);

        // fetch for debug info start/end for each token!
        // -> this helps to print problems out better!!!
        ctx->getStart()->getStartIndex();
        ctx->getStop()->getStopIndex();
        // get interval https://stackoverflow.com/questions/16343288/how-do-i-get-the-original-text-that-an-antlr4-rule-matched

        using namespace std;

        // check what type we have
        // NAME | NUMBER | STRING+ | '...' | 'None' | 'True' | 'False');
        if (ctx->NAME()) {
            pushNode(new NIdentifier(ctx->NAME()->getText()));
        } else if (ctx->NUMBER()) {
            pushNode(new NNumber(ctx->NUMBER()->getText()));
        } else if (!ctx->STRING().empty()) {
            std::string str = "";
            for (auto s : ctx->STRING()) {
                str += s->getText();
            }

            pushNode(new NString(str));
        } else if (ctx->ELLIPSIS()) {
            error("ellipsis not yet supported");
            return nullptr;
        } else if (ctx->NONE()) {
            pushNode(new NNone());
        } else if (ctx->TRUE()) {
            pushNode(new NBoolean(true));
        } else if (ctx->FALSE()) {
            pushNode(new NBoolean(false));
        } else if (ctx->OPEN_PAREN()) {
            // tuple
            if (ctx->yield_expr()) {
                error("yield expression not supported");
                return nullptr;
            }

            // all ok, there is a test
            // --> empty tuple or not?
            if (ctx->testlist_comp()) {
                // need to know how many elements
                assert(ctx->testlist_comp()->star_expr().size() == 0);
                assert(!ctx->testlist_comp()->comp_for());
                auto num_elements = ctx->testlist_comp()->test().size();

                // create tuple only IFF , is present!
                if(!ctx->testlist_comp()->COMMA().empty()) {
                    // pop from stack
                    assert(nodes.size() >= num_elements);

                    std::vector<ASTNode *> v;
                    v.reserve(num_elements);
                    for (unsigned i = 0; i < num_elements; ++i)
                        v.emplace_back(popNode());
                    std::reverse(v.begin(), v.end());
                    auto tuple = new NTuple();
                    tuple->_elements = v;

                    pushNode(tuple);
                } else {
                    // nothing todo...
                    // result is just an expression in (...)
                }
            } else {
                // empty tuple
                pushNode(new NTuple());
            }

        } else if (ctx->OPEN_BRACE()) {
            // dictionary or set
            if (ctx->yield_expr()) {
                error("yield expression not supported");
                return nullptr;
            }
            if(ctx->dictorsetmaker()) {
                // quick checks
                assert(ctx->dictorsetmaker()->POWER().empty());
                assert(!ctx->dictorsetmaker()->comp_for());
                assert(ctx->dictorsetmaker()->star_expr().empty());
                auto num_elements = ctx->dictorsetmaker()->test().size();
                if(!ctx->dictorsetmaker()->COLON().empty()) {
                    // pop from stack
                    assert(num_elements % 2 == 0);
                    assert(nodes.size() >= num_elements);

                    std::vector<pair<ASTNode *, ASTNode *> > pairs;
                    pairs.reserve(num_elements/2);
                    for (unsigned i = 0; i < num_elements/2; ++i) {
                        auto val = popNode();
                        auto key = popNode();
                        pairs.emplace_back(key, val);
                    }
                    auto dict = new NDictionary();
                    std::reverse(pairs.begin(), pairs.end());
                    dict->_pairs = pairs;

                    pushNode(dict);
                } else {
                    error("Sets not supported yet.");
                    return nullptr;
                }
            }
            else {
                pushNode(new NDictionary());
            }
        } else if(ctx->OPEN_BRACK()) {
            if(ctx->testlist_comp()) {
                if(ctx->testlist_comp()->comp_for()) {
                    assert(ctx->testlist_comp()->star_expr().empty());
                    auto num_elements = ctx->testlist_comp()->test().size();
                    if(num_elements > 1) {
                        error("for comprehension only supports one expression");
                    }
                    assert(nodes.size() >= 2);
                    auto generator = dynamic_cast<NComprehension*>(popNode());
                    auto expr = popNode();
                    auto lComp = new NListComprehension(expr);
                    lComp->generators.push_back(generator);

                    pushNode(lComp);
                } else {
                    // need to know how many elements
                    assert(ctx->testlist_comp()->star_expr().empty());
                    assert(!ctx->testlist_comp()->comp_for());
                    auto num_elements = ctx->testlist_comp()->test().size();

                    // pop from stack
                    assert(nodes.size() >= num_elements);

                    std::vector<ASTNode *> v;
                    v.reserve(num_elements);
                    for (unsigned i = 0; i < num_elements; ++i)
                        v.emplace_back(popNode());
                    std::reverse(v.begin(), v.end());
                    auto list = new NList();
                    list->_elements = v;

                    pushNode(list);
                }
            } else {
                pushNode(new NList());
            }
        } else {
            error("atom not yet supported");
        }

        return nullptr;
    }

    // unary operations +, -, ~
    antlrcpp::Any ASTBuilderVisitor::visitFactor(Python3Parser::FactorContext *ctx) {

        visitChildren(ctx);

        if (ctx->factor()) {
            // pop whatever is on the stack & add with unary operation back
            auto n = popNode();
            if (ctx->ADD())
                pushNode(new NUnaryOp(TokenType::PLUS, n));
            if (ctx->MINUS())
                pushNode(new NUnaryOp(TokenType::MINUS, n));
            if (ctx->NOT_OP())
                pushNode(new NUnaryOp(TokenType::TILDE, n));
        } else {
            assert(ctx->power());
            // do nothing, power already handled
        }

        return nullptr;
    }

    void ASTBuilderVisitor::constructBinaryOperation(const std::vector<antlr4::tree::ParseTree *> &children) {
        if (children.size() > 1) {
            // more than one child, must be odd and > 3
            // add operations
            assert(children.size() & 0x1);
            // @TODO: could also construct MapReduce tree here...
            // for now stick with simple solution, basically simple recursive thing

            // maybe a weird order at first hand, works though!
            assert(nodes.size() >= 2);

            // need to reverse order
            auto num_terms = (children.size() + 1) / 2;
            assert(nodes.size() >= num_terms);

            std::vector<ASTNode *> vn;
            for (unsigned i = 0; i < num_terms; ++i)
                vn.emplace_back(popNode());
            // reverse (stack effect)
            std::reverse(vn.begin(), vn.end());

            auto left = vn[0];
            auto right = vn[1];
            pushNode(new NBinaryOp(left, stringToToken(children[1]->getText()), right));

            for (unsigned i = 2; i < vn.size(); ++i) {
                auto left = popNode();
                auto right = vn[i];
                pushNode(new NBinaryOp(left, stringToToken(children[2 * i - 1]->getText()), right));
            }

        } // else, nothing todo, it is a simple factor
    }

    // binary operations *, @,
    antlrcpp::Any ASTBuilderVisitor::visitTerm(Python3Parser::TermContext *ctx) {
        visitChildren(ctx);
        constructBinaryOperation(ctx->children);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitArith_expr(Python3Parser::Arith_exprContext *ctx) {
        visitChildren(ctx);
        constructBinaryOperation(ctx->children);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitShift_expr(Python3Parser::Shift_exprContext *ctx) {
        visitChildren(ctx);
        constructBinaryOperation(ctx->children);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitExpr(Python3Parser::ExprContext *ctx) {

        visitChildren(ctx);
        constructBinaryOperation(ctx->children);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitXor_expr(Python3Parser::Xor_exprContext *ctx) {

        visitChildren(ctx);
        constructBinaryOperation(ctx->children);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitAnd_expr(Python3Parser::And_exprContext *ctx) {

        visitChildren(ctx);
        constructBinaryOperation(ctx->children);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitTrailer(Python3Parser::TrailerContext *ctx) {

        visitChildren(ctx);

        // only one special case, for .NAME rule emit identifier
        if (ctx->DOT()) {
            assert(ctx->NAME());
            pushNode(new NIdentifier(ctx->NAME()->getText()));
        }

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitAtom_expr(Python3Parser::Atom_exprContext *ctx) {

        // this can have AWAIT & trailer.
        // if encountered, break...
        if (ctx->AWAIT()) {
            error("await not supported");
            return nullptr;
        }

        visitChildren(ctx);

        if (ctx->trailer().size() > 0) {
            // subcript, arguments, ...

            // need to go through multiple trailers
            auto trailers = ctx->trailer();

            // how many nodes to pull from stack?
            auto num_to_pop = 0;
            for (auto t : trailers) {
                // .attribute or subscript via [...]
                if (t->DOT() || t->subscriptlist())
                    num_to_pop++;
                if (t->arglist())
                    num_to_pop += t->arglist()->argument().size();
            }

            // pop nodes
            assert(nodes.size() >= num_to_pop + 1);

            std::vector<ASTNode *> v;
            for (unsigned i = 0; i < num_to_pop + 1; ++i)
                v.emplace_back(popNode());

            ASTNode *atom = nullptr;
            ASTNode *trailer = nullptr;
            if (num_to_pop > 0) {
                atom = v.back();
                v.pop_back();
            }

            for (unsigned i = 0; i < trailers.size(); ++i) {
                auto t = trailers[i];


                if (t->DOT() || t->subscriptlist()) {
                    // update trailer
                    assert(!v.empty());
                    trailer = v.back();
                    v.pop_back();
                }

                if (t->DOT()) {
                    // argument lookup
                    assert(trailer->type() == ASTNodeType::Identifier);
                    atom = new NAttribute(atom, (NIdentifier *) trailer);
                }

                if (t->subscriptlist()) {
                    // subscript
                    // on the stack is the atom + trailer (i.e. the subscript)

                    // two options here:
                    // either it is a slice or a subscript
                    if (trailer->type() == ASTNodeType::SliceItem) {
                        // NSlicing!
                        // --> currently only a single one supported, future could be more...
                        atom = new NSlice(atom, {trailer});
                    } else {
                        // subscript
                        atom = new NSubscription(atom, trailer);
                    }
                }

                if (t->OPEN_PAREN()) {
                    // call with potential arglist
                    if (t->arglist()) {
                        //.. call with args...
                        auto num_args = t->arglist()->argument().size();

                        auto atom_id = dynamic_cast<NIdentifier*>(atom);
                        if(atom_id && atom_id->_name == "range") {
                            auto range = new NRange();
                            for(unsigned i = 0; i < num_args; ++i) {
                                assert(!v.empty());
                                range->_positionalArguments.emplace_back(v.back());
                                v.pop_back();
                            }
                            atom = range;
                        } else {
                            auto call = new NCall(atom);
                            for (unsigned i = 0; i < num_args; ++i) {
                                assert(!v.empty());
                                call->_positionalArguments.emplace_back(v.back());
                                v.pop_back();
                            }
                            atom = call;
                        }
                    } else {
                        atom = new NCall(atom);
                    }
                }
            }

            pushNode(atom);
        }

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitArgument(Python3Parser::ArgumentContext *ctx) {

        // only positional args are yet supported...
        if (ctx->STAR() || ctx->ASSIGN() || ctx->POWER()) {
            error("only positional arguments supported yet");
            return nullptr;
        } else {
            // nothing todo
        }

        visitChildren(ctx);


        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitSubscriptlist(Python3Parser::SubscriptlistContext *ctx) {

        // only single subscriptlist so far supported
        if (ctx->subscript().size() != 1) {
            error("only a single subscript list is yet supported.");
            return nullptr;
        }

        visitChildren(ctx);

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitSliceop(Python3Parser::SliceopContext *ctx) {

        // always push something here to make logic in subscript easier
        if (!ctx->test())
            pushNode(nullptr);


        visitChildren(ctx);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitSubscript(Python3Parser::SubscriptContext *ctx) {

        visitChildren(ctx);

        // could be either script OR slice operator
        // ==> support only script so far
        if (ctx->COLON()) {

            // this will result in a slice item node...
            // (test)? ':' (test)? (sliceop)?;
            // need to gover nodes
            // check if first is test

            // check if first one is test context
            if (ctx->children.size() == 4) {
                // easy, all 4 must hold
                auto stride = popNode();
                auto end = popNode();
                auto start = popNode();
                pushNode(new NSliceItem(start, end, stride));
            } else if (ctx->children.size() == 3) {

                // Python3Parser::TestContext
                // Python3Parser::SliceopContext
                // options: test : test
                //          test : sliceop
                //          : test sliceop

                // check
                if (dynamic_cast<Python3Parser::TestContext *>(ctx->children.front())) {
                    if (dynamic_cast<Python3Parser::SliceopContext *>(ctx->children.back())) {
                        // test : sliceop
                        auto stride = popNode();
                        auto start = popNode();
                        pushNode(new NSliceItem(start, nullptr, stride));
                    } else {
                        // test : test
                        // i.e. start to end
                        auto end = popNode();
                        auto start = popNode();
                        pushNode(new NSliceItem(start, end, nullptr));
                    }
                } else {
                    // : test sliceop case
                    auto stride = popNode();
                    auto end = popNode();
                    pushNode(new NSliceItem(nullptr, end, stride));
                }
            } else if (ctx->children.size() == 2) {
                // there must be a colon amongst this...
                // because the colon must be present, there are only 3 options
                // test :
                // : test
                // : sliceop

                if (dynamic_cast<Python3Parser::TestContext *>(ctx->children.front())) {
                    // test :
                    auto start = popNode();
                    pushNode(new NSliceItem(start, nullptr, nullptr));
                } else {
                    if (dynamic_cast<Python3Parser::SliceopContext *>(ctx->children.back())) {
                        // : sliceop
                        auto stride = popNode();
                        pushNode(new NSliceItem(nullptr, nullptr, stride));
                    } else {
                        // : test
                        auto end = popNode();
                        pushNode(new NSliceItem(nullptr, end, nullptr));
                    }
                }
            }
        } else {
            assert(ctx->test().size() == 1);
            // nothing todo, just a single test
        }

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitStar_expr(Python3Parser::Star_exprContext *ctx) {

        error("Star (*) expression not yet supported");
        // visitChildren(ctx);

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitComparison(Python3Parser::ComparisonContext *ctx) {
        visitChildren(ctx);

        auto children = ctx->children;
        if (children.size() > 1) {
            // more than one child, must be odd and > 3
            // add operations
            assert(children.size() & 0x1);
            assert(nodes.size() >= 2);

            // need to reverse order
            auto num_terms = (children.size() + 1) / 2;
            assert(nodes.size() >= num_terms);

            std::vector<ASTNode *> vn;
            for (unsigned i = 0; i < num_terms; ++i)
                vn.emplace_back(popNode());
            // reverse (stack effect)
            std::reverse(vn.begin(), vn.end());

            auto cmp = new NCompare();
            cmp->setLeft(vn[0]);

            for (unsigned i = 1; i < vn.size(); ++i) {
                auto right = vn[i];
                assert(dynamic_cast<Python3Parser::Comp_opContext *>(children[2 * i - 1]));
                cmp->addCmp(stringToToken(children[2 * i - 1]->getText()), right);
            }
            pushNode(cmp);
        }

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitOr_test(Python3Parser::Or_testContext *ctx) {
        visitChildren(ctx);
        constructBinaryOperation(ctx->children);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitAnd_test(Python3Parser::And_testContext *ctx) {

        visitChildren(ctx);
        constructBinaryOperation(ctx->children);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitNot_test(Python3Parser::Not_testContext *ctx) {

        visitChildren(ctx);


        // two options: 'not' not_test | comparison
        if (ctx->NOT()) {
            auto n = popNode();
            pushNode(new NUnaryOp(TokenType::NOT, n));
        }// else, nothing todo

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitTest(Python3Parser::TestContext *ctx) {

        visitChildren(ctx);
        //test: or_test ('if' or_test 'else' test)? | lambdef;
        // check if 'if' is present, then new node, else nothing todo
        if (ctx->IF()) {
            assert(nodes.size() >= 3);
            // pop 3 nodes
            auto _else = popNode();
            auto _cond = popNode();
            auto _then = popNode();

            // not a stmt, but an expression
            pushNode(new NIfElse(_cond, _then, _else, true));
        }


        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitLambdef(Python3Parser::LambdefContext *ctx) {

        visitChildren(ctx);

        // arguments?
        if (ctx->varargslist()) {
            auto expr = popNode();
            auto paramList = popNode();
            assert(paramList->type() == ASTNodeType::ParameterList);
            pushNode(new NLambda((NParameterList *) paramList, expr));
        } else {
            // no args, test must be present though
            // i.e. pop node & push
            assert(nodes.size() >= 1);
            assert(ctx->test());

            auto n = popNode();
            pushNode(new NLambda(new NParameterList(), n));
        }

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitLambdef_nocond(Python3Parser::Lambdef_nocondContext *ctx) {

        error("lambdef_nocond not yet supported");
        // visitChildren(ctx);
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitTypedargslist(Python3Parser::TypedargslistContext *ctx) {
        visitChildren(ctx);

        // this is similar to vararg list, however this time there are parameters & possible default values
        // because of stack go from back
        ASTNode *lastDefault = nullptr; // last default value encountered


        std::vector<NParameter *> parameters;

        for (int i = ctx->children.size() - 1; i >= 0; --i) {
            auto child = ctx->children[i];

            // check what type of child we do have
            if (dynamic_cast<Python3Parser::TfpdefContext *>(child)) {
                // pop identifier
                auto n = popNode();
                assert(n->type() == ASTNodeType::Parameter);

                NParameter *param = (NParameter *) n;
                param->setDefault(lastDefault);
                // no annotation for vararg list...
                parameters.emplace_back(param);

                // clear lastdefault
                lastDefault = nullptr;
            } else {
                // ignore ,
                // do not ignore * and **!
                // they are not supported
                if (child->getText() == ",") {
                    // ignore
                }

                if (child->getText() == "*") {
                    error("no support for starred args yet");
                }

                if (child->getText() == "**") {
                    error("no support for kwargs yet");
                }

                // check if =, meaning default
                if (child->getText() == "=") {
                    // set last default value to succesor
                    assert(i + 1 < ctx->children.size());
                    lastDefault = popNode();
                }
            }

        }


        // reverse parameters
        std::reverse(parameters.begin(), parameters.end());
        // create NParamList
        auto pl = new NParameterList();
        for (auto p : parameters)
            pl->addParam(p);

        pushNode(pl);

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitTfpdef(Python3Parser::TfpdefContext *ctx) {
        visitChildren(ctx);


        // function parameters can have an annotation
        // hence, create parameter here!
        //tfpdef: NAME (':' test)?;
        assert(ctx->NAME());
        if (ctx->test()) {
            // pop test from stack
            auto ann = popNode();
            auto param = new NParameter(new NIdentifier(ctx->NAME()->getText()));
            param->setAnnotation(ann);
            pushNode(param);
        } else {
            // simple identifier
            pushNode(new NParameter(new NIdentifier(ctx->NAME()->getText())));
        }

        return nullptr;
    }


    // this here is the most complex one because of all the weird options in python...
    antlrcpp::Any ASTBuilderVisitor::visitVarargslist(Python3Parser::VarargslistContext *ctx) {
        visitChildren(ctx);

        // because of stack go from back
        ASTNode *lastDefault = nullptr; // last default value encountered


        std::vector<NParameter *> parameters;

        for (int i = ctx->children.size() - 1; i >= 0; --i) {
            auto child = ctx->children[i];

            // check what type of child we do have
            if (dynamic_cast<Python3Parser::VfpdefContext *>(child)) {
                // pop identifier
                auto id = popNode();
                assert(id->type() == ASTNodeType::Identifier);

                auto param = new NParameter((NIdentifier *) id);
                param->setDefault(lastDefault);
                // no annotation for vararg list...
                parameters.emplace_back(param);

                // clear lastdefault
                lastDefault = nullptr;
            } else {
                // ignore ,
                // do not ignore * and **!
                // they are not supported
                if (child->getText() == ",") {
                    // ignore
                }

                if (child->getText() == "*") {
                    error("no support for starred args yet");
                    return nullptr;
                }

                if (child->getText() == "**") {
                    error("no support for kwargs yet");
                    return nullptr;
                }

                // check if =, meaning default
                if (child->getText() == "=") {
                    // set last default value to succesor
                    assert(i + 1 < ctx->children.size());
                    lastDefault = popNode();
                }
            }

        }

        // reverse parameters
        std::reverse(parameters.begin(), parameters.end());
        // create NParamList
        auto pl = new NParameterList();
        for (auto p : parameters)
            pl->addParam(p);

        pushNode(pl);

        return nullptr;
    }


    antlrcpp::Any ASTBuilderVisitor::visitFuncdef(Python3Parser::FuncdefContext *ctx) {

        visitChildren(ctx);
        // funcdef: 'def' NAME parameters ('->' test)? ':' suite;


        // no support for -> annotation
        if (ctx->ARROW()) {
            error("no support for error annotations");
            return nullptr;
        }

        // normal parsing
        NIdentifier *name = new NIdentifier(ctx->NAME()->getText());

        // on stack there should be suite & params
        assert(nodes.size() >= 2);

        auto suite = (NSuite *) popNode();
        auto parameters = (NParameterList *) popNode();

        assert(suite->type() == ASTNodeType::Suite);
        assert(parameters->type() == ASTNodeType::ParameterList);

        auto func = new NFunction(name, suite);
        func->setParams(parameters);
        pushNode(func);

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitParameters(Python3Parser::ParametersContext *ctx) {

        visitChildren(ctx);

        if (ctx->typedargslist()) {
            // parameters already in place...
        } else {
            // push empty arglist
            pushNode(new NParameterList());
        }

        return nullptr;
    }


    antlrcpp::Any ASTBuilderVisitor::visitSuite(Python3Parser::SuiteContext *ctx) {

        visitChildren(ctx);

        if (ctx->simple_stmt()) {
            // a single simple stmt?
            auto simple_stmt = popNode();
            auto suite = new NSuite();

            suite->addStatement(simple_stmt);

            pushNode(suite);

        } else {
            // must have multiple statements
            assert(ctx->stmt().size() > 0);

            auto num_stmts = ctx->stmt().size();
            std::vector<ASTNode *> v;
            v.reserve(num_stmts);
            for (unsigned i = 0; i < num_stmts; ++i)
                v.emplace_back(popNode());

            // reverse
            std::reverse(v.begin(), v.end());

            // push back to suite
            auto suite = new NSuite();
            for (auto stmt : v)
                suite->addStatement(stmt);

            pushNode(suite);
        }
        return nullptr;
    }

    // simply emit identifier
    antlrcpp::Any ASTBuilderVisitor::visitVfpdef(Python3Parser::VfpdefContext *ctx) {
        visitChildren(ctx);

        assert(ctx->NAME());
        pushNode(new NIdentifier(ctx->NAME()->getText()));

        return nullptr;
    }

    // tuple construction, ...
    antlrcpp::Any ASTBuilderVisitor::visitTestlist_comp(Python3Parser::Testlist_compContext *ctx) {
        visitChildren(ctx);

        if (ctx->star_expr().size() > 0) {
            error("star expressions not yet supported");
            return nullptr;
        }

        // only test in the list, i.e. can create a tuple out of it!
        // --> nothing to do actually, because each test pushes to stack.
        // hence, in atom the tuple construction needs to happen.

        return nullptr;
    }

    // dictionary construction
    antlrcpp::Any ASTBuilderVisitor::visitDictorsetmaker(antlr4::Python3Parser::DictorsetmakerContext *ctx) {
        visitChildren(ctx);

        if(ctx->comp_for()) {
            error("for comprehension not yet supported");
            return nullptr;
        }
        if(ctx->star_expr().size() > 0) {
            error("star expressions not yet supported");
            return nullptr;
        }
        if(!ctx->POWER().empty()) {
            error("Dictionary unpacking not yet supported.");
            return nullptr;
        }
        if(!ctx->COLON().empty() && ctx->test().size() % 2 == 1) {
            error("Invalid dictionary statement");
            return nullptr;
        }
        // construct dict in atom
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitReturn_stmt(Python3Parser::Return_stmtContext *ctx) {
        visitChildren(ctx);

        // generate new return stmt
        // can be empty or not
        if (ctx->testlist()) {

            // TODO: change here to support multiple nodes.
            // => or do this in testlist.
            assert(nodes.size() >= 1);

            size_t tupleSize = ctx->testlist()->test().size();
            assert(nodes.size() > tupleSize);

            // TODO: doesn't work when returning tuples w/ function (e.g., return f(x), y)

            if (tupleSize == 1 && ctx->testlist()->COMMA().empty()) {
                pushNode(new NReturn(popNode()));
            } else {
                NTuple tuple;
                tuple._elements.resize(tupleSize);

                for (auto i = 0; i < tupleSize; i++) {
                    tuple._elements[tupleSize-i-1] = popNode();
                }
                pushNode(new NReturn(&tuple));
            }
        } else {
            pushNode(new NReturn());
        }

        return nullptr;
    }

    // i.e. part of a, b = b, a
    // we only support single variable for now...
    antlrcpp::Any ASTBuilderVisitor::visitTestlist_star_expr(Python3Parser::Testlist_star_exprContext *ctx) {
        size_t size = ctx->test().size();
        visitChildren(ctx);
        if (size == 1) {
            return nullptr;
        }

        assert(nodes.size() > size);
        NTuple expr;
        expr._elements.resize(size);

        for (auto i = 0; i < size; i++) {
            expr._elements[size-i-1] = popNode();
        }
        pushNode(new NTuple(expr));

        return nullptr;
    }

    // assign stmt
    antlrcpp::Any ASTBuilderVisitor::visitExpr_stmt(Python3Parser::Expr_stmtContext *ctx) {
        visitChildren(ctx);

        // expr_stmt: testlist_star_expr (annassign | augassign (yield_expr|testlist) |
        //                     ('=' (yield_expr|testlist_star_expr))*);

        // first error on all unsupported statements for now
        if (ctx->annassign()) {
            error("annotation assign not yet supported");
            return nullptr;
        }

        if (ctx->yield_expr().size() > 0) {
            error("yield expr not yet supported");
            return nullptr;
        }

        // multiple testlist_star_expr possible
        // i.e. a = b = c = 10

        // augassign might be also fine
        if (ctx->augassign()) {
            // note: here testlist_star_expr contains only one element

            // there should be at least 2 elements on the stack
            assert(nodes.size() >= 2);
            ASTNode *value = popNode();
            ASTNode *target = popNode();

            // hack: create assign + operation
            auto opString = ctx->augassign()->getText();
            assert(opString.back() == '=');
            auto binop = new NBinaryOp(target, stringToToken(opString.substr(0, opString.length() - 1)), value);

            pushNode(new NAssign(target, binop));

            return nullptr;
        } else {
            // no augassign...
            auto num_expr = ctx->testlist_star_expr().size();

            // more than one?
            if(num_expr >= 2) {

                assert(ctx->ASSIGN().size() == num_expr - 1);

                // pop from stack
                std::vector<ASTNode *> v;
                v.reserve(ctx->testlist_star_expr().size());
                for (int i = 0; i < ctx->testlist_star_expr().size(); ++i) {
                    v.emplace_back(popNode());
                }

                ASTNode *assign = new NAssign(v[1], v[0]);
                for (unsigned i = 2; i < v.size(); ++i) {
                    assign = new NAssign(v[i], assign);
                }

                pushNode(assign);
            } // else it is a single expr, i.e/ nothing todo

        }

        return nullptr;
    }

    // if elif else stmt
    antlrcpp::Any ASTBuilderVisitor::visitIf_stmt(Python3Parser::If_stmtContext *ctx) {
        visitChildren(ctx);

        // if_stmt: 'if' test ':' suite ('elif' test ':' suite)* ('else' ':' suite)?;

        // first case:
        // if only
        assert(ctx->IF());

        if (!ctx->ELSE() && ctx->ELIF().size() == 0) {
            // simple pop test & suite
            auto suite = popNode();
            auto test = popNode();

            assert(suite->type() == ASTNodeType::Suite);

            pushNode(new NIfElse(test, suite, nullptr, false));

        } else if (ctx->ELSE() && ctx->ELIF().size() == 0) {
            auto elseSuite = popNode();
            auto thenSuite = popNode();
            auto test = popNode();

            assert(thenSuite->type() == ASTNodeType::Suite);
            assert(thenSuite->type() == ASTNodeType::Suite);

            pushNode(new NIfElse(test, thenSuite, elseSuite, false));
        } else {
            // full elif, not yet supported...
            //error("not yet supported...");

            auto num_elif = ctx->ELIF().size();

            // final else suite?
            ASTNode *elseSuite = nullptr;
            if (ctx->ELSE())
                elseSuite = popNode();

            // pop all elif
            std::vector<ASTNode *> tests;
            std::vector<ASTNode *> suites;
            for (unsigned i = 0; i < num_elif; ++i) {
                suites.emplace_back(popNode());
                tests.emplace_back(popNode());
            }

            // construct elif & final if
            ASTNode *elif = elseSuite;
            for (unsigned i = 0; i < num_elif; ++i) {
                elif = new NIfElse(tests[i], suites[i], elif, false);
            }

            auto suite = popNode();
            auto test = popNode();

            pushNode(new NIfElse(test, suite, elif, false));
        }

        return nullptr;
    }

    // rules that not yet fully implemented...
    antlrcpp::Any ASTBuilderVisitor::visitPower(Python3Parser::PowerContext *ctx) {
        // no error, return
        visitChildren(ctx);

        // check if factor is present
        // Ruleis::         power: atom_expr ('**' factor)?;

        if(ctx->factor()) {

            // add new binary op
            auto right = popNode();
            auto left = popNode();

            assert(right && left);

            pushNode(new NBinaryOp(left, TokenType::DOUBLESTAR, right));
            return nullptr;
        }

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitExprlist(antlr4::Python3Parser::ExprlistContext *context) {
        visitChildren(context);

        // no *expr yet supported. When supporting that, need to adapt code below.
        if(!context->star_expr().empty()) {
            error("star_expr not supported yet");
            return nullptr;
        }

        // create expr list form test list
        int num_targets = context->expr().size();
        assert(nodes.size() >= num_targets);
        std::vector<ASTNode*> targets;
        for(int i = 0; i < num_targets; ++i)
            targets.push_back(popNode());
        std::reverse(targets.begin(), targets.end());

        // push tuple of identifiers or single identifier
        if(1 == num_targets) {
            pushNode(targets.front());
        } else {
            auto t = new NTuple();
            t->_elements = targets;
            pushNode(t);
        }

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitComp_for(antlr4::Python3Parser::Comp_forContext *context) {
        visitChildren(context);

        if(context->comp_iter()) {
            error("multiple generators not supported in for comprehensions");
            return nullptr;
        }

        // just exprlist and or_test -> can construct NComprehension
        auto iter = popNode();
        auto target = dynamic_cast<NIdentifier*>(popNode());
        pushNode(new NComprehension(target, iter));
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitAssert_stmt(antlr4::Python3Parser::Assert_stmtContext *ctx) {
        visitChildren(ctx);
        // two expressions or one?
        if(ctx->COMMA()) {
            // two
            auto errExpr = popNode();
            auto expr = popNode();
            pushNode(new NAssert(expr, errExpr));
        } else {
            // one
            auto expr = popNode();
            pushNode(new NAssert(expr));
        }

        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitRaise_stmt(antlr4::Python3Parser::Raise_stmtContext *ctx) {
        visitChildren(ctx);

        // 3 options.
        if(ctx->children.size() == 1) {
            pushNode(new NRaise());
        } else if(ctx->FROM()) {
            auto fromExpr = popNode();
            auto expr = popNode();
            pushNode(new NRaise(expr, fromExpr));
        } else {
            auto expr = popNode();
            pushNode(new NRaise(expr));
        }

        return nullptr;
    }
    
    antlrcpp::Any ASTBuilderVisitor::visitFor_stmt(antlr4::Python3Parser::For_stmtContext *ctx) {
        visitChildren(ctx);

        // for exprlist in testlist
        assert(ctx->FOR());
        assert(ctx->IN());

        ASTNode* suite_else = nullptr;
        if(ctx->ELSE()) {
            // for ... else ...
            suite_else = popNode();
            assert(suite_else->type() == ASTNodeType::Suite);
        }
        auto suite_body = popNode();
        assert(suite_body->type() == ASTNodeType::Suite);
        // if multiple tests in testlist, wrap them in a tuple
        auto testlist_size = ctx->testlist()->test().size();
        assert(nodes.size() >= testlist_size+1);
        std::vector<ASTNode*> testlist(testlist_size);
        for (auto i = 0; i < testlist_size; ++i) {
            testlist[testlist_size-1-i] = popNode();
        }
        auto exprlist = popNode(); // handled in visitExprlist

        if(testlist_size > 1) {
            auto tuple = new NTuple;
            tuple->_elements = testlist;
            pushNode(new NFor(exprlist, tuple, suite_body, suite_else));
        } else {
            pushNode(new NFor(exprlist, testlist.front(), suite_body, suite_else));
        }
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitWhile_stmt(antlr4::Python3Parser::While_stmtContext *ctx) {
        visitChildren(ctx);

        assert(ctx->WHILE());

        ASTNode* suite_else;

        if(ctx->ELSE()) {
            // while ... else ...
            suite_else = popNode();
            assert(suite_else->type() == ASTNodeType::Suite);
        } else {
            // while statement only
            suite_else = nullptr;
        }

        auto suite_body = popNode();
        assert(suite_body->type() == ASTNodeType::Suite);
        auto expression = popNode();

        pushNode(new NWhile(expression, suite_body, suite_else));
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitBreak_stmt(antlr4::Python3Parser::Break_stmtContext *ctx) {
        visitChildren(ctx);

        assert(ctx->BREAK());
        pushNode(new NBreak());
        return nullptr;
    }

    antlrcpp::Any ASTBuilderVisitor::visitContinue_stmt(antlr4::Python3Parser::Continue_stmtContext *ctx) {
        visitChildren(ctx);

        assert(ctx->CONTINUE());
        pushNode(new NContinue());
        return nullptr;
    }
}