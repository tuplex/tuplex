//
// Created by leonhard on 10/3/22.
//

#include <physical/codegen/JsonSourceTaskBuilder.h>
#include <experimental/StructDictHelper.h>
#include <experimental/ListHelper.h>
#include <physical/experimental/JSONParseRowGenerator.h>
#include <logical/FilterOperator.h>

namespace tuplex {
    namespace codegen {
        JsonSourceTaskBuilder::JsonSourceTaskBuilder(const std::shared_ptr<LLVMEnvironment> &env,
                                                     int64_t input_operator_id,
                                                     const python::Type &normalCaseRowType,
                                                     const python::Type &generalCaseRowType,
                                                     const std::vector<std::string>& normal_case_columns,
                                                     const std::vector<std::string>& general_case_columns,
                                                     bool unwrap_first_level,
                                                     const std::map<int, int>& normalToGeneralMapping,
                                                     const std::string &name,
                                                     const ExceptionSerializationMode& except_mode,
                                                     const std::vector<NormalCaseCheck>& checks): BlockBasedTaskBuilder(env,
                                                                                                                   normalCaseRowType,
                                                                                                                   generalCaseRowType,
                                                                                                                   normalToGeneralMapping,
                                                                                                                   name,
                                                                                                                   except_mode,
                                                                                                                   checks),
                                                     _normalCaseRowType(normalCaseRowType),
                                                     _generalCaseRowType(generalCaseRowType),
                                                     _normal_case_columns(normal_case_columns),
                                                     _general_case_columns(general_case_columns),
                                                     _unwrap_first_level(unwrap_first_level),
                                                     _functionName(name),
                                                     _inputOperatorID(input_operator_id) {
        }

        llvm::Function* JsonSourceTaskBuilder::build(bool terminateEarlyOnFailureCode) {
            using namespace llvm;

            assert(_env);
            auto& ctx = _env->getContext();
            // build a basic block based function (compatible with function signature read_block_f in CodeDefs.h
            auto arg_types = std::vector<llvm::Type*>({ctypeToLLVM<void*>(ctx),
                                                       ctypeToLLVM<uint8_t*>(ctx),
                                                       ctypeToLLVM<int64_t>(ctx),
                                                       ctypeToLLVM<int64_t*>(ctx),
                                                       ctypeToLLVM<int64_t*>(ctx),
                                                       ctypeToLLVM<int8_t>(ctx)});
            FunctionType* FT = FunctionType::get(ctypeToLLVM<int64_t>(ctx), arg_types, false);
            auto func = getOrInsertFunction(_env->getModule().get(), _functionName, FT);

            auto args = mapLLVMFunctionArgs(func, {"userData", "inPtr", "inSize", "outNormalRowCount", "outBadRowCount", "ignoreLastRow"});

            BasicBlock* bbEntry = BasicBlock::Create(env().getContext(), "entry", func);
            IRBuilder<> builder(bbEntry);

            auto bufPtr = args["inPtr"];
            auto bufSize = args["inSize"];
            auto userData = args["userData"];

            initVars(builder);

            // full parse loop now
            auto parsed_bytes = generateParseLoop(builder, bufPtr, bufSize, userData, _normal_case_columns,
                                                  _general_case_columns, _unwrap_first_level, terminateEarlyOnFailureCode);



            // // debug
            // env().debugPrint(builder, "---");
            //  env().printValue(builder, builder.CreateLoad(_rowNumberVar), "row number: ");
            //  env().printValue(builder, builder.CreateLoad(_normalRowCountVar), "normal:     ");
            //  env().printValue(builder, builder.CreateLoad(_generalRowCountVar), "general:    ");
            //  env().printValue(builder, builder.CreateLoad(_fallbackRowCountVar), "fallback:   ");
            //  env().printValue(builder, builder.CreateLoad(_badNormalRowCountVar), "bad normal: ");
            //  env().debugPrint(builder, "---");

            // store in output var info
            llvm::Value* normalRowCount = builder.CreateLoad(_normalRowCountVar);
            normalRowCount = builder.CreateSub(normalRowCount, builder.CreateLoad(_badNormalRowCountVar));
            env().storeIfNotNull(builder, normalRowCount, args["outNormalRowCount"]);
            llvm::Value* badRowCount = builder.CreateAdd(builder.CreateLoad(_generalRowCountVar), builder.CreateLoad(_fallbackRowCountVar));
            badRowCount = builder.CreateAdd(builder.CreateLoad(_badNormalRowCountVar), badRowCount);
            env().storeIfNotNull(builder, badRowCount, args["outBadRowCount"]); // <-- i.e. exceptions

            builder.CreateRet(parsed_bytes);

            return func;
        }

        std::vector<int>
        JsonSourceTaskBuilder::columns_required_for_checks(const std::vector<NormalCaseCheck> &checks) const {
            std::set<int> columns;
            for(const auto& check : checks) {
                switch(check.type) {
                    case CheckType::CHECK_CONSTANT: {
                        if(!check.isSingleColCheck())
                            throw std::runtime_error("only single col constant check yet supported.");

                        std::stringstream ss;
                        ss<<"Found normal case check (constant) for column "<<check.colNo();
                        logger().debug(ss.str());
                        columns.insert(check.colNo());
                        break;
                    }
                    case CheckType::CHECK_FILTER: {
                        throw std::runtime_error("not yet implemented");
                        break;
                    }
                    default:
                        throw std::runtime_error("Found check " + check.to_string() + " for which columns required can't be determined.");
                }
            }

            auto ret = std::vector<int>{columns.begin(), columns.end()};
            std::sort(ret.begin(), ret.end());
            return ret;
        }

        std::tuple<FlattenedTuple, std::unordered_map<int, int>>
        JsonSourceTaskBuilder::parse_selected_columns(llvm::IRBuilder<> &builder,
                                                      const std::vector<int> &columns_to_parse, llvm::Value *parser,
                                                      llvm::BasicBlock *bbBadParse) const {
            using namespace llvm;
            assert(parser);
            assert(bbBadParse);

            std::vector<python::Type> col_types_to_parse;
            std::vector<std::string> col_names_to_parse;
            std::tie(col_types_to_parse, col_names_to_parse) = get_column_types_and_names(std::vector<size_t>(columns_to_parse.begin(), columns_to_parse.end()));

            for(auto& t: col_types_to_parse)
                t = deoptimizedType(t);
            auto row_type = python::Type::makeTupleType(col_types_to_parse);

            auto tuple_row_type = row_type.isRowType() ? row_type.get_columns_as_tuple_type() : row_type;
            assert(row_type_compatible_with_columns(tuple_row_type, col_names_to_parse));
            auto ft = json_parseRow(*_env.get(), builder, tuple_row_type, col_names_to_parse, true, false, parser, bbBadParse);


            std::unordered_map<int, int> m;
            for(unsigned i = 0; i < columns_to_parse.size(); ++i) {
                m[columns_to_parse[i]] = i;
            }

            return std::make_tuple(ft, m);
        }

        void
        JsonSourceTaskBuilder::generateChecks(llvm::IRBuilder<> &builder,
                                              llvm::Value *userData,
                                              llvm::Value *rowNumber,
                                              llvm::Value* parser,
                                              llvm::BasicBlock* bbSkipRow,
                                              llvm::BasicBlock* bbBadRow) {
            using namespace llvm;

            assert(rowNumber && rowNumber->getType() == _env->i64Type());
            assert(parser && parser->getType() == _env->i8ptrType());

            // _env->printValue(builder, rowNumber, "normal-case check for row=");

            // which checks are available?
            auto checks = this->checks();

            // for all checks, determine which columns are required.
            // then, parse once the required columns
            auto required_cols_for_check = columns_required_for_checks(checks);

            {
                std::stringstream ss;
                std::vector<std::string> required_column_names;
                for(auto i : required_cols_for_check)
                    if(_normal_case_columns.empty())
                        required_column_names.push_back("<no name>");
                    else {
                        required_column_names.push_back(_normal_case_columns[i]);
                    }
                ss<<"Following columns are to be parsed for checks: "<<required_cols_for_check<<" ("<<required_column_names<<")";
                logger().debug(ss.str());
            }

            // parse here into single flattened tuple & map
            FlattenedTuple ft_for_checks(_env.get());
            std::unordered_map<int, int> col_to_check_col_map;
            std::tie(ft_for_checks, col_to_check_col_map) = parse_selected_columns(builder, required_cols_for_check, parser, bbBadRow);

            for(const auto& check : checks) {
                if(check.type == CheckType::CHECK_FILTER) {
                    // ok, this is supported
                    logger().debug("found filter check, emitting code");

                    // deserialize filter operator in order to generate check properly
                    std::shared_ptr<FilterOperator> fop = nullptr;
#ifdef BUILD_WITH_CEREAL
                    // use cereal deserialization
                    {
                        std::istringstream iss(check.data());
                        cereal::BinaryInputArchive ar(iss);
                        ar(fop);
                    }
#else
                    fop = FilterOperator::from_json(nullptr, check.data());
#endif
                    logger().debug("deserialized operator " + fop->name());

                    // which columns does filter operator require?
                    // -> perform struct type rewrite specific for THIS filter
                    auto acc_cols = fop->getUDF().getAccessedColumns(false);
                    std::stringstream ss;
                    ss<<"filter accesses following columns: "<<acc_cols<<"\n";
                    ss<<"input row type: "<<_inputRowType.desc()<<"\n";
                    ss<<"input columns: "<<_normal_case_columns<<"\n";
                    // // these are empty b.c. no parent operator for check
                    // ss<<"filter input columns: "<<fop->inputColumns()<<"\n";
                    // ss<<"filter input schema: "<<fop->getInputSchema().getRowType().desc()<<"\n";

                    // retype filter operator

                    std::vector<std::string> acc_column_names;
                    std::vector<python::Type> acc_col_types;
                    std::tie(acc_col_types, acc_column_names) = get_column_types_and_names(acc_cols);

                    // however, these here should show correct columns/types.
                    ss<<"filter input columns: "<<acc_column_names<<"\n";
                    ss<<"filter input schema: "<<python::Type::makeTupleType(acc_col_types).desc()<<"\n";

                    RetypeConfiguration conf;
                    // @TODO: when always switching over to row type, can remove the column names here.
                    conf.columns = acc_column_names;
                    conf.row_type = python::Type::makeTupleType(acc_col_types);

                    if(PARAM_USE_ROW_TYPE)
                        conf.row_type = python::Type::makeRowType(acc_col_types, acc_column_names);

                    conf.is_projected = true;

                    // create filter op from scratch (avoid retyping etc., no parent needed)
                    UDF plain_udf(fop->getUDF().getCode(),
                                  fop->getUDF().getPickledCode(),
                                  fop->getUDF().getAnnotatedAST().globals(),
                                  fop->getUDF().compilePolicy());
                    fop = FilterOperator::from_udf(plain_udf, conf.row_type, conf.columns);
                    if(!fop)
                        throw std::runtime_error("failed to create filter operator properly!");

                    // auto ret = fop->retype(conf);
                    // if(!ret) {
                    //     std::stringstream err_stream;
                    //     err_stream<<"failed to retype filter operator with columns="<<conf.columns<<", type="<<conf.row_type.desc();
                    //     logger().error(err_stream.str());
                    //     throw std::runtime_error(err_stream.str());
                    // }
                    logger().debug(ss.str());


                    // create two basic blocks: -> check passed, check failed
                    auto& ctx = builder.getContext();
                    BasicBlock* bbCheckPassed = BasicBlock::Create(ctx, "filter_check_" + std::to_string(fop->getID()) + "_passed", builder.GetInsertBlock()->getParent());
                    BasicBlock* bbCheckFailed = BasicBlock::Create(ctx, "filter_check_" + std::to_string(fop->getID()) + "_failed", builder.GetInsertBlock()->getParent());

                    if(_unwrap_first_level) {
                        // first level are columns:
                        // i.e., detect which columns are necessary and prune accordingly!
                        // => only filter specific paths should be parsed!
                        ss.str("");
                        ss<<"filter requires "<<pluralize(conf.columns.size(), "column")<<": "<<conf.columns;
                        logger().debug(ss.str());
                        BasicBlock* bbFilterBadParse = BasicBlock::Create(ctx, "filter_" + std::to_string(fop->getID()) + "_bad_parse", builder.GetInsertBlock()->getParent());
                        auto tuple_row_type = conf.row_type.isRowType() ? conf.row_type.get_columns_as_tuple_type() : conf.row_type;
                        assert(row_type_compatible_with_columns(tuple_row_type, conf.columns));
                        auto ft_filter = json_parseRow(env(), builder, tuple_row_type, conf.columns, true, false, parser, bbFilterBadParse);

                        // debug print:
#ifndef NDEBUG
                        for(unsigned i = 0; i < conf.columns.size(); ++i) {
                            auto col_type = conf.row_type.isRowType() ? conf.row_type.get_column_type(i) : conf.row_type.parameters()[i];

                            // what type?
                            if(python::Type::STRING == col_type) {
                                auto str_value = ft_filter.get(i);
                                assert(str_value && str_value->getType() == env().i8ptrType());
                                // env().printValue(builder, str_value, conf.columns[i] + "= ");
                            }
                        }
#endif
                        // now call filter & determine whether good or bad
                        if(!fop->getUDF().isCompiled())
                            throw std::runtime_error("only compilable UDFs here supported!");
                        auto cf = const_cast<UDF&>(fop->getUDF()).compile(env());
                        if(!cf.good())
                            throw std::runtime_error("failed compiling UDF for filter check");

                        if(python::Type::BOOLEAN != cf.output_python_type)
                            throw std::runtime_error("only filter with boolean output type yet supported in filter check");

                        auto resVal = env().CreateFirstBlockAlloca(builder, cf.getLLVMResultType(env()));

                        auto exceptionBlock = BasicBlock::Create(ctx, "filter_" + std::to_string(fop->getID()) + "_except", builder.GetInsertBlock()->getParent());
                        auto ecVar = env().CreateFirstBlockAlloca(builder, env().i64Type(), "ecVar_filter");
                        auto ft = cf.callWithExceptionHandler(builder, ft_filter, resVal, exceptionBlock, ecVar);
                        // check type is correct
                        auto expectedType = python::Type::BOOLEAN;
                        llvm::Value* filterCond = nullptr; // i1 filter condition
                        if(ft.getTupleType() != expectedType) {
                            // perform truthValueTest on result
                            // is it a tuple type?
                            // keep everything!
                            if(ft.numElements() > 1)
                                filterCond = env().i1Const(true);
                            else{
                                auto ret_val = SerializableValue(ft.get(0), ft.getSize(0), ft.getIsNull(0)); // single value?
                                filterCond = env().truthValueTest(builder, ret_val, cf.output_python_type);
                            }
                        } else {
                            assert(ft.numElements() ==  1);
                            auto compareVal = ft.get(0);
                            assert(compareVal->getType() == env().getBooleanType());
                            filterCond = builder.CreateICmpEQ(compareVal, env().boolConst(true));
                        }

                        // env().printValue(builder, filterCond, "promoted filter res=");
                        builder.CreateCondBr(filterCond, bbCheckPassed, bbSkipRow);

                        builder.SetInsertPoint(exceptionBlock);
                        // env().printValue(builder, builder.CreateLoad(ecVar), "filter check failed with exception of ec=");
                        builder.CreateBr(bbCheckFailed);

                        builder.SetInsertPoint(bbFilterBadParse);
                        // env().debugPrint(builder, "filter row parse failed");
                        builder.CreateBr(bbCheckFailed);

                    } else {
                        logger().debug("filter check with non-unwrap of first level, prune struct type paths");
                        throw std::runtime_error("non unwrap not yet supported for filter-promo");
                    }

                    // now what happens if filter passed or failed?

                    builder.SetInsertPoint(bbCheckFailed);
                    // env().debugPrint(builder, "promoted filter check failed.");
                    builder.CreateBr(bbBadRow);

                    // continue on normal case path!
                    builder.SetInsertPoint(bbCheckPassed);
                    // env().debugPrint(builder, "promoted filter check passed.");

                    // create reduced input type for quick parsing
                    assert(_inputRowType.isTupleType() || _inputRowType.isRowType());
                } else if(check.type == CheckType::CHECK_CONSTANT && check.isSingleColCheck()) {
                    // is column being serialized? If so - emit check.

                    {
                        std::stringstream ss;
                        ss<<"general-case requires column, emit code for constant check == "
                          <<check.constant_type().constant()
                          <<" for column "<<check.colNo();
                        logger().info(ss.str());
                    }

                    // retrieve corresponding column
                    auto val = ft_for_checks.getLoad(builder, {col_to_check_col_map[check.colNo()]});

                    python::Type elementType;
                    std::string  constant_value;
                    std::tie(elementType, constant_value) = extract_type_and_value_from_constant_check(check);
                    llvm::Value* check_cond = nullptr;
                    if(python::Type::BOOLEAN == elementType) {
                        // compare value
                        auto c_val = parseBoolString(constant_value);
                        check_cond = builder.CreateICmpEQ(_env->boolConst(c_val), val.val);
                    } else {
                        throw std::runtime_error("Check with type " + check.constant_type().desc() + " not yet supported");
                    }

                    auto& ctx = builder.getContext();
                    BasicBlock* bbCheckPassed = BasicBlock::Create(ctx, "constant_check_" + std::to_string(check.colNo()) + "_passed", builder.GetInsertBlock()->getParent());

                    // if cond is met, proceed - else go to bad parse
                    builder.CreateCondBr(check_cond, bbCheckPassed, bbBadRow);

                    builder.SetInsertPoint(bbCheckPassed);

                    // _env->debugPrint(builder, "constant check passed.");
                } else {
                    throw std::runtime_error("Check " + check.to_string() + " not supported for JsonSourceTaskBuilder");
                }
            }
        }

        std::tuple<std::vector<python::Type>, std::vector<std::string>>
        JsonSourceTaskBuilder::get_column_types_and_names(const std::vector<size_t> &acc_cols) const {
            std::vector<std::string> acc_column_names;
            std::vector<python::Type> acc_col_types;
            for(auto idx : acc_cols) {
                if(_inputRowType.isRowType()) {
                    assert(vec_equal(_normal_case_columns, _inputRowType.get_column_names()));
                    acc_column_names.push_back(_inputRowType.get_column_name(idx));
                    acc_col_types.push_back(_inputRowType.get_column_type(idx));
                } else {
                    acc_column_names.push_back(_normal_case_columns[idx]);
                    acc_col_types.push_back(_inputRowType.parameters()[idx]);
                }
            }

            return std::make_tuple(acc_col_types, acc_column_names);
        }

        void JsonSourceTaskBuilder::initVars(llvm::IRBuilder<> &builder) {
            // input vars (read from reader)
            _normalRowCountVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));
            _generalRowCountVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));
            _fallbackRowCountVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));

            // stores how many of normal rows were processed with bad result (pipeline).
            _badNormalRowCountVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));

            _normalMemorySizeVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));
            _generalMemorySizeVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));
            _fallbackMemorySizeVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0));

            _rowNumberVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0), "row_no");

            _parsedBytesVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0), "truncated_bytes");

             // init JsonItem row object var
             _row_object_var = _env->CreateFirstBlockVariable(builder, _env->i8nullptr(), "row_object");
        }

        llvm::Value *JsonSourceTaskBuilder::generateParseLoop(llvm::IRBuilder<> &builder,
                                                              llvm::Value *bufPtr,
                                                              llvm::Value *bufSize,
                                                              llvm::Value *userData,
                                                              const std::vector<std::string>& normal_case_columns,
                                                              const std::vector<std::string>& general_case_columns,
                                                              bool unwrap_first_level,
                                                              bool terminateEarlyOnLimitCode) {
            using namespace llvm;
            using namespace std;

            auto& ctx = _env->getContext();

            // this will be a loop
            auto F = builder.GetInsertBlock()->getParent();
            BasicBlock *bLoopHeader = BasicBlock::Create(ctx, "loop_header", F);
            BasicBlock *bLoopBody = BasicBlock::Create(ctx, "loop_body", F);
            BasicBlock *bLoopExit = BasicBlock::Create(ctx, "loop_exit", F);

            // init json parse
            // auto j = JsonParser_init();
            // if(!j)
            //     throw std::runtime_error("failed to initialize parser");
            // JsonParser_open(j, buf, buf_size);
            // while(JsonParser_hasNextRow(j)) {
            //     if(JsonParser_getDocType(j) != JsonParser_objectDocType()) {

            auto parser = initJsonParser(builder);

            // init row number
            _rowNumberVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0), "row_no");
            // _badParseCountVar = _env->CreateFirstBlockVariable(builder, _env->i64Const(0), "badparse_count");

            // create single free block
            _freeStart = _freeEnd = BasicBlock::Create(ctx, "free_row_objects", F);

            {
                // create here free obj block.
                llvm::IRBuilder<> b(_freeStart);

                // _env->printValue(b, rowNumber(b), "entered free row objects for row no=");

                // release the row var if required
                json_release_object(*_env, b, _row_object_var);
                _freeEnd = b.GetInsertBlock();
            }

            llvm::Value *rc = openJsonBuf(builder, parser, bufPtr, bufSize);
            llvm::Value *rc_cond = _env->i1neg(builder,
                                              builder.CreateICmpEQ(rc, _env->i64Const(ecToI64(ExceptionCode::SUCCESS))));
            exitMainFunctionWithError(builder, rc_cond, rc);
            _env->freeAll(builder);
            builder.CreateBr(bLoopHeader);


            // ---- loop condition ---
            // go from current block to header
            builder.SetInsertPoint(bLoopHeader);
            // condition (i.e. hasNextDoc)
            auto cond = hasNextRow(builder, parser);
            builder.CreateCondBr(cond, bLoopBody, bLoopExit);



            // ---- loop body ----
            // body
            builder.SetInsertPoint(bLoopBody);
            // generate here...
            // _env->debugPrint(builder, "parsed row");





#ifdef JSON_PARSER_TRACE_MEMORY
             _env->printValue(builder, rowNumber(builder), "row no= ");
#endif
            // check whether it's of object type -> parse then as object (only supported type so far!)
            cond = isDocumentOfObjectType(builder, parser);

#ifdef JSON_PARSER_TRACE_MEMORY
            _env->printValue(builder, cond, "check that document is object: ");
#endif

            BasicBlock* bbParseAsGeneralCaseRow = BasicBlock::Create(_env->getContext(), "parse_as_general_row", builder.GetInsertBlock()->getParent());
            BasicBlock* bbNormalCaseSuccess = BasicBlock::Create(_env->getContext(), "normal_row_found", builder.GetInsertBlock()->getParent());
            BasicBlock* bbGeneralCaseSuccess = BasicBlock::Create(_env->getContext(), "general_row_found", builder.GetInsertBlock()->getParent());
            BasicBlock* bbFallback = BasicBlock::Create(_env->getContext(), "fallback_row_found", builder.GetInsertBlock()->getParent());

            // // filter promotion? -> happens here.
            // if(hackyPromoteEventFilter) {
            //     BasicBlock* bbKeep = BasicBlock::Create(_env->getContext(), "keep_row_after_promo", builder.GetInsertBlock()->getParent());
            //     BasicBlock* bbSkip = BasicBlock::Create(_env->getContext(), "skip_row_after_promo", builder.GetInsertBlock()->getParent());
            //
            //     auto keep_row = emitHackyFilterPromo(builder, parser, hackyEventName, bbKeep);
            //     builder.CreateCondBr(keep_row, bbKeep, bbSkip);
            //
            //     // create skip part -> count as normal row.
            //     builder.SetInsertPoint(bbSkip);
            //     incVar(builder, _normalRowCountVar);
            //     builder.CreateBr(_freeStart);
            //
            //     builder.SetInsertPoint(bbKeep);
            // }

            if(!checks().empty()) {
                BasicBlock* bbSkipRow = BasicBlock::Create(_env->getContext(), "normal_row_skip", builder.GetInsertBlock()->getParent());
                {
                    llvm::IRBuilder<> builder(bbSkipRow);

                    // env().debugPrint(builder, "skip row b.c. of promoted filter");

                    incVar(builder, _normalRowCountVar);
                    builder.CreateBr(_freeStart);
                }

                // before parsing the full JSON, perform any checks required to speed up the process!
                generateChecks(builder, userData, rowNumber(builder), parser, bbSkipRow, bbFallback);
            }


#ifdef JSON_PARSER_TRACE_MEMORY
            _env->debugPrint(builder, "try parsing as normal row...");
#endif
            // new: within its own LLVM function
            // parse here as normal row
            auto normal_case_row = generateAndCallParseRowFunction(builder, "parse_normal_row_internal",
                                                                   _normalCaseRowType, normal_case_columns,
                                                                   unwrap_first_level, parser, bbParseAsGeneralCaseRow);
#ifdef JSON_PARSER_TRACE_MEMORY
            _env->printValue(builder, rc, "normal row parsed.");
#endif
            builder.CreateBr(bbNormalCaseSuccess);

            // // old:
            // // parse here as normal row
            // auto normal_case_row = parseRow(builder, _normalCaseRowType, normal_case_columns, unwrap_first_level, parser, bbParseAsGeneralCaseRow);
            // builder.CreateBr(bbNormalCaseSuccess);

            builder.SetInsertPoint(bbParseAsGeneralCaseRow);

            // short cut: if normal case and general case are identical -> go to fallback from here!
            FlattenedTuple general_case_row(_env.get());
            if(_normalCaseRowType == _generalCaseRowType) {
                builder.CreateBr(bbFallback);
            } else {

                // test: always emit badparse string input exceptions -> a good idea for hyper-specialization
                bool emit_only_bad_parse = true; // --> disabling this will make EVERYTHING super slow.
                if(emit_only_bad_parse) {
                    // with this below, one could avoid using the complex general-case row parse stuff.
                    // be quick, avoid the complex structure
                    builder.CreateBr(bbFallback);
                } else {
                    // new: (--> should be llvm optimizer friendlier)
                    general_case_row = generateAndCallParseRowFunction(builder, "parse_general_row_internal",
                                                                       _generalCaseRowType, general_case_columns,
                                                                       unwrap_first_level, parser, bbFallback);
#ifdef JSON_PARSER_TRACE_MEMORY
                    _env->printValue(builder, rc, "general row parsed.");
#endif
                    builder.CreateBr(bbGeneralCaseSuccess);

                    // // old, but correct with long compile times:
                    // general_case_row = parseRow(builder, _generalCaseRowType, general_case_columns,
                    //                             unwrap_first_level, parser, bbFallback);
                    // builder.CreateBr(bbGeneralCaseSuccess);
                }
            }

            // now create the blocks for each scenario
            {
                // 1. normal case
                builder.SetInsertPoint(bbNormalCaseSuccess);
#ifdef JSON_PARSER_TRACE_MEMORY
                 _env->debugPrint(builder, "processing as normal-case row...");
#endif
                // if pipeline exists, call pipeline on normal tuple!
                if(pipeline()) {
                    auto processRowFunc = pipeline()->getFunction();
                    if(!processRowFunc)
                        throw std::runtime_error("invalid function from pipeline builder in JsonSourceTaskBuilder");
                    auto row_no = rowNumber(builder);
                    auto intermediate = initIntermediate(builder);
                    // _env->debugPrint(builder, "Calling pipeline on rowno: ", row_no);
                    auto pip_res = PipelineBuilder::call(builder, processRowFunc, normal_case_row, userData, row_no, intermediate);

#ifdef JSON_PARSER_TRACE_MEMORY
                    _env->debugPrint(builder, "pipeline called.");
#endif

                    // create if based on resCode to go into exception block
                    auto ecCode = builder.CreateZExtOrTrunc(pip_res.resultCode, env().i64Type());
                    auto ecOpID = builder.CreateZExtOrTrunc(pip_res.exceptionOperatorID, env().i64Type());
                    auto numRowsCreated = builder.CreateZExtOrTrunc(pip_res.numProducedRows, env().i64Type());

                    // env().printValue(builder, ecCode, "pip ecCode= ");

                    // if ecCode != success -> inc bad normal count.
                    // do this here branchless
                    auto bad_row_cond = builder.CreateICmpNE(ecCode, env().i64Const(ecToI64(ExceptionCode::SUCCESS)));
                    // inc
                    auto bad_row_delta = builder.CreateZExt(bad_row_cond, env().i64Type());

                    incVar(builder, _badNormalRowCountVar, bad_row_delta);

                    if(terminateEarlyOnLimitCode)
                        generateTerminateEarlyOnCode(builder, ecCode, ExceptionCode::OUTPUT_LIMIT_REACHED);

                    // if there's an exception handler, serialize
                    if(hasExceptionHandler()) {


                        // does pipeline have exception handler or not?
                        // if not, then task needs to emit exception to process further down the line...
                        // -> could short circuit and save "true" exception first here.
                        BasicBlock *bNotOK = BasicBlock::Create(ctx, "pipeline_not_ok", builder.GetInsertBlock()->getParent());
                        BasicBlock *bNext = BasicBlock::Create(ctx, "pipeline_next", builder.GetInsertBlock()->getParent());
                        builder.CreateCondBr(bad_row_cond, bNotOK, bNext);
                        builder.SetInsertPoint(bNotOK);

                        _env->debugPrint(builder, "found row to serialize as exception in normal-case handler");

                        // normal case row is parsed - can it be converted to general case row?
                        // if so emit directly, if not emit fallback row.
                        auto normal_case_row_type = _normalCaseRowType;
                        auto general_case_row_type = _generalCaseRowType;


//                        // following code snippet casts it up as general_case_row exception with general_case row type.
//                        // Yet in decodeFallbackRow, the decode is done using normal-case schema?
//                        serializeAsNormalCaseException(builder, userData, _inputOperatorID, rowNumber(builder), normal_case_row);


                        if(python::canUpcastType(normal_case_row_type, general_case_row_type)) {
                            logger().debug("found exception handler in JSON source task builder, serializing exceptions in general case format.");

                            // if both are row type, check names are the same. Then because normal_case_row is given as
                            // tuple type, convert general case to tuple type.
                            if(normal_case_row_type.isRowType() && general_case_row_type.isRowType()) {
                                if(!vec_equal(normal_case_row_type.get_column_names(), general_case_row_type.get_column_names()))
                                    throw std::runtime_error("need to fix ordering in upcast.");
                                general_case_row_type = general_case_row_type.get_columns_as_tuple_type();
                            }

                            auto upcasted_row = normal_case_row.upcastTo(builder, general_case_row_type);

                            // serialize as exception --> this connects already to freeStart.
                            serializeAsNormalCaseException(builder, userData, _inputOperatorID, rowNumber(builder), upcasted_row);
                        } else {
                            logger().debug("normal case row and general case row not compatible, emitting exceptions as fallback rows.");
                            throw std::runtime_error("need to implement exception handling here");
                        }

                        // connect to next row processing.
                        builder.CreateBr(_freeStart);

                        // pipeline ok, continue with normal processing
                        builder.SetInsertPoint(bNext);
                    }
                }

                // serialized size (as is)
                auto normal_size = normal_case_row.getSize(builder);
                incVar(builder, _normalMemorySizeVar, normal_size);

                // _env->debugPrint(builder, "got normal-case row!");

                // inc by one
                incVar(builder, _normalRowCountVar);
                builder.CreateBr(_freeStart);
            }

            {
                // 2. general case
                builder.SetInsertPoint(bbGeneralCaseSuccess);

#ifdef JSON_PARSER_TRACE_MEMORY
                _env->debugPrint(builder, "got general-case row!");
#endif

                // serialized size (as is)
                auto general_size = general_case_row.getSize(builder);

#ifdef JSON_PARSER_TRACE_MEMORY
                _env->printValue(builder, general_size, "serializing general-case row of size=");
#endif
                // in order to store an exception, need 8 bytes for each: rowNumber, ecCode, opID, eSize + the size of the row
                general_size = builder.CreateAdd(general_size, _env->i64Const(4 * sizeof(int64_t)));

                serializeAsNormalCaseException(builder, userData, _inputOperatorID, rowNumber(builder), general_case_row);

                incVar(builder, _generalMemorySizeVar, general_size);

                incVar(builder, _generalRowCountVar);

#ifdef JSON_PARSER_TRACE_MEMORY
                _env->debugPrint(builder, "serialization as exception done for general-case row.");
#endif
                builder.CreateBr(_freeStart);
            }

            {
                // 3. fallback case
                builder.SetInsertPoint(bbFallback);

#ifdef JSON_PARSER_TRACE_MEMORY
                _env->debugPrint(builder, "try parsing as fallback-case row...");
#endif
                // same like in general case, i.e. stored as badStringParse exception
                // -> store here the raw json
                auto Frow = getOrInsertFunction(_env->getModule().get(), "JsonParser_getMallocedRowAndSize", _env->i8ptrType(),
                                                _env->i8ptrType(), _env->i64ptrType());
                auto size_var = _env->CreateFirstBlockAlloca(builder, _env->i64Type());
                auto line = builder.CreateCall(Frow, {parser, size_var});

                // create badParse exception
                // @TODO: throughput can be improved by using a single C++ function for all of this!
                serializeBadParseException(builder, userData, _inputOperatorID, rowNumber(builder), line, builder.CreateLoad(size_var));
#ifdef JSON_PARSER_TRACE_MEMORY
                 _env->debugPrint(builder, "got fallback-case row!");
#endif
                // should whitespace lines be skipped?
                bool skip_whitespace_lines = true;

                if(skip_whitespace_lines) {
                    // note: line could be empty line -> check whether to skip or not!
                    auto Fws = getOrInsertFunction(_env->getModule().get(), "Json_is_whitespace", ctypeToLLVM<bool>(_env->getContext()),
                                                   _env->i8ptrType(), _env->i64Type());
                    auto ws_rc = builder.CreateCall(Fws, {line, builder.CreateLoad(size_var)});
                    auto is_ws = builder.CreateICmpEQ(ws_rc, cbool_const(_env->getContext(), true));


                    // skip whitespace?
                    BasicBlock* bIsNotWhitespace = BasicBlock::Create(ctx, "not_whitespace", builder.GetInsertBlock()->getParent());
                    BasicBlock* bFallbackDone = BasicBlock::Create(ctx, "fallback_done", builder.GetInsertBlock()->getParent());

                    // decrease row count
                    incVar(builder, _rowNumberVar, -1);

                    builder.CreateCondBr(is_ws, bFallbackDone, bIsNotWhitespace);

                    // only inc for non whitespace (would serialize here!)
                    builder.SetInsertPoint(bIsNotWhitespace);
                    incVar(builder, _fallbackMemorySizeVar, builder.CreateLoad(size_var));
                    incVar(builder, _fallbackRowCountVar);
                    incVar(builder, _rowNumberVar); // --> trick, else the white line is counted as row!
                    builder.CreateBr(bFallbackDone);
                    builder.SetInsertPoint(bFallbackDone);
                }
                else {
                    incVar(builder, _fallbackMemorySizeVar, builder.CreateLoad(size_var));
                    incVar(builder, _fallbackRowCountVar);
                }


                // this is ok here, b.c. it's local.
                _env->cfree(builder, line);
                builder.CreateBr(_freeStart);
            }


            {
//                auto s = struct_dict_serialized_memory_size(_env, builder, row_var, row_type);
//                // _env->printValue(builder, s.val, "size of row materialized in bytes is: ");
//
//                // rtmalloc and serialize!
//                auto mem_ptr = _env->malloc(builder, s.val);
//                auto serialization_res = struct_dict_serialize_to_memory(_env, builder, row_var, row_type, mem_ptr);
//                // _env->printValue(builder, serialization_res.size, "realized serialization size is: ");
//
//                // inc total size with serialization size!
//                auto cur_total = builder.CreateLoad(_outTotalSerializationSize);
//                auto new_total = builder.CreateAdd(cur_total, serialization_res.size);
//                builder.CreateStore(new_total, _outTotalSerializationSize);
//
//                // now, load entries to struct type in LLVM
//                // then calculate serialized size and print it.
//                //auto v = struct_dict_load_from_values(_env, builder, row_type, entries);
            }

//            auto bbSchemaMismatch = emitBadParseInputAndMoveToNextRow(builder, parser, _env->i1neg(builder, cond));

            // go to free start
            //builder.CreateBr(_freeStart);

            // free data..
            // --> parsing will generate there free statements per row

            builder.SetInsertPoint(_freeEnd); // free is done -> now move onto next row.
            // go to next row
            moveToNextRow(builder, parser);

            // this will only work when allocating everything local!
            // -> maybe better craft a separate process row function?

            // link back to header
            _env->freeAll(builder);
            builder.CreateBr(bLoopHeader);

            // ---- post loop block ----
            // continue in loop exit.
            builder.SetInsertPoint(bLoopExit);

            // before parser is freed, query the parsed bytes!
            auto parsed_bytes = parsedBytes(builder, parser, bufSize);
            builder.CreateStore(parsed_bytes, _parsedBytesVar);

            // free JSON parse (global object)
            freeJsonParse(builder, parser);

            return builder.CreateLoad(_parsedBytesVar);
        }

        llvm::Value* json_parseRowAsStructuredDict(LLVMEnvironment& env, llvm::IRBuilder<> &builder, const python::Type &dict_type,
                llvm::Value *j, llvm::BasicBlock *bbSchemaMismatch) {
            assert(j);
            using namespace llvm;
            auto &ctx = env.getContext();

            // get initial object
            // => this is from parser
            auto Fgetobj = getOrInsertFunction(env.getModule().get(), "JsonParser_getObject", env.i64Type(),
                                               env.i8ptrType(), env.i8ptrType()->getPointerTo(0));

            auto obj_var = env.CreateFirstBlockVariable(builder, env.i8nullptr(), "row_object");

            // release and store nullptr
            json_release_object(env, builder, obj_var);

            builder.CreateCall(Fgetobj, {j, obj_var});

            // don't forget to free everything...
            // alloc variable
            auto struct_dict_type = create_structured_dict_type(env, dict_type);
            auto row_var = env.CreateFirstBlockAlloca(builder, struct_dict_type);

#ifdef JSON_PARSER_TRACE_MEMORY
            env.debugPrint(builder, "initializing struct dict");
#endif
            struct_dict_mem_zero(env, builder, row_var, dict_type); // !!! important !!!

            // create dict parser and store to row_var
            JSONParseRowGenerator gen(env, dict_type, bbSchemaMismatch);
#ifdef JSON_PARSER_TRACE_MEMORY
            env.debugPrint(builder, "parsing data to row var");
#endif
            gen.parseToVariable(builder, builder.CreateLoad(obj_var), row_var);

            // create new block (post - parse)
            BasicBlock *bPostParse = BasicBlock::Create(ctx, "post_parse", builder.GetInsertBlock()->getParent());

            builder.CreateBr(bPostParse);
            builder.SetInsertPoint(bPostParse);
#ifdef JSON_PARSER_TRACE_MEMORY
            env.debugPrint(builder, "entering post parse");
#endif
            // free obj_var...
            json_release_object(env, builder, obj_var);
        #ifndef NDEBUG
            builder.CreateStore(env.i8nullptr(), obj_var);
        #endif
#ifdef JSON_PARSER_TRACE_MEMORY
            env.debugPrint(builder, "returning row var");
#endif
            return row_var;
        }

        FlattenedTuple json_parseRow(LLVMEnvironment& env, llvm::IRBuilder<>& builder, const python::Type& row_type,
                                const std::vector<std::string>& columns,
                                bool unwrap_first_level,
                                bool fill_missing_first_level_with_null,
                                llvm::Value* parser,
                                llvm::BasicBlock *bbSchemaMismatch) {
            auto& logger = Logger::instance().logger("codegen");
            assert(row_type.isTupleType());

            FlattenedTuple ft(&env);
            ft.init(row_type);

            // should first level be unwrapped or not?
            if(unwrap_first_level) {
                // -> need to have columns info
                assert(columns.size() >= row_type.parameters().size());

                if(columns.size() < row_type.parameters().size()) {
                    builder.CreateBr(bbSchemaMismatch);
                    // maybe need to initialize with dummies?
                    return ft;
                }

                // parse using a struct type composed of columns and the data!
                std::vector<python::StructEntry> entries;
                auto num_entries = std::min(columns.size(), row_type.parameters().size());

                for(unsigned i = 0; i < num_entries; ++i) {
                    python::StructEntry entry;
                    entry.keyType = python::Type::STRING;
                    entry.key = escape_to_python_str(columns[i]);
                    entry.valueType = row_type.parameters()[i];

                    // if value type is option[...] (or null) and fill missing flag is set,
                    // then make it a maybe present element.
                    if((python::Type::NULLVALUE == entry.valueType || entry.valueType.isOptionType()) && fill_missing_first_level_with_null)
                        entry.alwaysPresent = false;

                    entries.push_back(entry);
                }
                auto dict_type = python::Type::makeStructuredDictType(entries);

                // parse dictionary
                auto dict = json_parseRowAsStructuredDict(env, builder, dict_type, parser, bbSchemaMismatch);

                // env.debugPrint(builder, "-> start parse:");

                // fetch columns from dict and assign to tuple!
                for(int i = 0; i < num_entries; ++i) {
                    SerializableValue value;

#ifdef JSON_PARSER_TRACE_MEMORY
                    env.debugPrint(builder, "fetching entry '" + columns[i] + "' type=" + row_type.parameters()[i].desc() + "  " + std::to_string(i + 1) + " of " + std::to_string(num_entries));
#endif

                    // special case: maybe element and option type
                    auto value_type = row_type.parameters()[i];
                    bool treat_missing_as_null = (python::Type::NULLVALUE == value_type || value_type.isOptionType()) && fill_missing_first_level_with_null;
                    if(treat_missing_as_null) {
                        // check if element is present, load only if present - else dummy.
                        access_path_t access_path;
                        access_path.push_back(std::make_pair(escape_to_python_str(columns[i]), python::Type::STRING));
                        auto is_present = struct_dict_load_present(env, builder, dict, dict_type, access_path);

                        llvm::BasicBlock* bColumnPresent = llvm::BasicBlock::Create(env.getContext(), "column" + std::to_string(i) + "_present", builder.GetInsertBlock()->getParent());
                        llvm::BasicBlock* bColumnDone = llvm::BasicBlock::Create(env.getContext(), "column" + std::to_string(i) + "_done", builder.GetInsertBlock()->getParent());

                        auto curBlock = builder.GetInsertBlock();
                        builder.SetInsertPoint(bColumnPresent);
                        // parse
                        // fetch value from dict!
                        value = struct_dict_get_or_except(env, builder, dict_type, escape_to_python_str(columns[i]),
                                                          python::Type::STRING, dict, bbSchemaMismatch);
                        auto bLastColumnDone = builder.GetInsertBlock();
                        builder.CreateBr(bColumnDone);

                        // create branch and dummies
                        builder.SetInsertPoint(curBlock);
                        SerializableValue dummy = CreateDummyValue(env, builder, value_type);

                        //  env.printValue(builder, is_present, "column " + std::to_string(i) + "/" + std::to_string(columns.size()) + " " + columns[i] + " present:");

                       builder.CreateCondBr(is_present, bColumnPresent, bColumnDone);
                        builder.SetInsertPoint(bColumnDone);

                        // correct for size (sometimes passed around for e.g., None)
                        if(!dummy.size)
                            value.size = nullptr;
                        if(!value.size)
                            dummy.size = nullptr;

                        // create phi nodes & store in flattened tuple
                        auto phi_val = value.val ? builder.CreatePHI(value.val->getType(), 2) : nullptr;
                        auto phi_size = value.size ? builder.CreatePHI(value.size->getType(), 2) : nullptr;
                        auto phi_is_null = value.is_null ? builder.CreatePHI(env.i1Type(), 2) : nullptr;

                        auto is_null = env.i1neg(builder, is_present); // null when not present or when retrieved value is null?

                        if(value.val) {
                            phi_val->addIncoming(value.val, bLastColumnDone);
                            phi_val->addIncoming(dummy.val, curBlock);
                        }
                        if(value.size) {
                            phi_size->addIncoming(value.size, bLastColumnDone);
                            phi_size->addIncoming(dummy.size, curBlock);
                        }

                        if(phi_is_null) {
                            phi_is_null->addIncoming(value.is_null, bLastColumnDone);
                            phi_is_null->addIncoming(env.i1Const(false), curBlock);

                            // update isnull
                            is_null = builder.CreateOr(is_null, phi_is_null);
                        }

                        ft.set(builder, {i}, phi_val, phi_size, is_null);

                        //env.debugPrint(builder, "-- set");
                    } else {
                        // fetch value from dict!
                        value = struct_dict_get_or_except(env, builder, dict_type, escape_to_python_str(columns[i]),
                                                          python::Type::STRING, dict, bbSchemaMismatch);
#ifdef JSON_PARSER_TRACE_MEMORY
                        if(value.size)
                        env.printValue(builder, value.size, "got entry " + std::to_string(i + 1) + " with size: ");
#endif
                        ft.set(builder, {i}, value.val, value.size, value.is_null);
                    }
                }

#ifdef JSON_PARSER_TRACE_MEMORY
                env.debugPrint(builder, "tuple load done.");
#endif
            } else {
                assert(row_type.parameters().size() == 1);
                auto dict_type = row_type.parameters()[0].withoutOption();
                assert(dict_type.isStructuredDictionaryType()); // <-- this should be true, or an alternative parsing strategy devised...
                if(!dict_type.isStructuredDictionaryType())
                    throw std::runtime_error("parsing JSON files with type " + dict_type.desc() + " not yet implemented.");

                // _env->debugPrint(builder, "starting to parse row...");
                // parse struct dict
                auto s = json_parseRowAsStructuredDict(env, builder, dict_type, parser, bbSchemaMismatch);
                // _env->debugPrint(builder, "row parsed.");

                // assign to tuple (for further processing)
                ft.setElement(builder, 0, s, nullptr, nullptr);
            }

            return ft;
        }

        llvm::Function* json_generateParseRowFunction(LLVMEnvironment& env,
                                                      const std::string& name,
                                                      const python::Type &row_type,
                                                      const std::vector<std::string> &columns,
                                                      bool unwrap_first_level) {
            using namespace llvm;

            auto& logger = Logger::instance().logger("codegen");

            FlattenedTuple ft(&env);
            ft.init(row_type);
            auto tuple_llvm_type = ft.getLLVMType();

            // now, create function type
            auto FT = FunctionType::get(env.i64Type(), {env.i8ptrType(), tuple_llvm_type->getPointerTo()}, false);
            auto F = Function::Create(FT, llvm::GlobalValue::LinkageTypes::InternalLinkage, name, env.getModule().get());

            BasicBlock* bEntry = BasicBlock::Create(env.getContext(), "entry", F); // <-- call first!
            BasicBlock* bMismatch = BasicBlock::Create(env.getContext(), "mismatch", F);
            IRBuilder<> builder(bMismatch);
            // free objects here

            builder.CreateRet(env.i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));

            // main and entry block.
            builder.SetInsertPoint(bEntry);

            // map function args
            auto args = mapLLVMFunctionArgs(F, {"parser", "out_tuple"});

            auto parser = args["parser"];

            if(columns.empty()) {
                logger.debug("no column specified means defaulting to no unwrap mode.");
                unwrap_first_level = false;
            }

            auto tuple_row_type = row_type.isRowType() ? row_type.get_columns_as_tuple_type() : row_type;
            assert(row_type_compatible_with_columns(tuple_row_type, columns));
            auto ft_parsed = json_parseRow(env, builder, tuple_row_type, columns, unwrap_first_level, true, parser, bMismatch);
            ft_parsed.storeTo(builder, args["out_tuple"]);
#ifdef JSON_PARSER_TRACE_MEMORY
            _env->debugPrint(builder, "tuple store to output ptr done.");
#endif

            // free temp objects here...
            builder.CreateRet(env.i64Const(0));
            return F;
        }

        void json_freeParser(LLVMEnvironment& env, llvm::IRBuilder<>& builder, llvm::Value* parser) {
            auto &ctx = env.getContext();
            auto F = getOrInsertFunction(env.getModule().get(), "JsonParser_Free", llvm::Type::getVoidTy(ctx),
                                         env.i8ptrType());
            builder.CreateCall(F, parser);
        }

        // chain for initializing parser (incl. go to code)
        llvm::Value* json_retrieveParser(LLVMEnvironment& env, llvm::IRBuilder<>& builder,
                                         llvm::Value* str, llvm::Value* str_size, llvm::BasicBlock* bError) {
            using namespace llvm;

            assert(str && str_size && bError);

            auto F_init = getOrInsertFunction(env.getModule().get(), "JsonParser_Init", env.i8ptrType());

            auto parser = builder.CreateCall(F_init, {});
            auto is_null = builder.CreateICmpEQ(parser, env.i8nullptr());

            BasicBlock* bParserInitOK = BasicBlock::Create(env.getContext(), "parser_init_ok", builder.GetInsertBlock()->getParent());
            builder.CreateCondBr(is_null, bError, bParserInitOK);
            builder.SetInsertPoint(bParserInitOK);


            // now open buffer (i.e., parse)
            auto F_parse = getOrInsertFunction(env.getModule().get(), "JsonParser_open", env.i64Type(), env.i8ptrType(),
                                         env.i8ptrType(), env.i64Type());
            auto parse_code =  builder.CreateCall(F_parse, {parser, str, str_size});

            // check if ok
            auto parse_ok = builder.CreateICmpEQ(parse_code, env.i64Const(ecToI64(ExceptionCode::SUCCESS)));

            BasicBlock* bParseOK = BasicBlock::Create(env.getContext(), "parse_ok", builder.GetInsertBlock()->getParent());
            BasicBlock* bBadParse = BasicBlock::Create(env.getContext(), "parse_failed", builder.GetInsertBlock()->getParent());

            builder.CreateCondBr(parse_ok, bParseOK, bBadParse);
            builder.SetInsertPoint(bBadParse);
            json_freeParser(env, builder, parser);
            builder.CreateBr(bError);
            builder.SetInsertPoint(bParseOK);

            return parser;
        }

        llvm::Function* json_generateParseStringFunction(LLVMEnvironment& env,
                                                         const std::string& name,
                                                         const python::Type &row_type,
                                                         const std::vector<std::string> &columns) {

            using namespace llvm;

            FlattenedTuple ft(&env); ft.init(row_type);
            auto llvm_tuple_type = ft.getLLVMType();

            // create new func
            auto FT = FunctionType::get(env.i64Type(), {llvm_tuple_type->getPointerTo(), env.i8ptrType(), env.i64Type()}, false);

            auto func = getOrInsertFunction(env.getModule().get(), name, FT);
            BasicBlock* bEntry = BasicBlock::Create(env.getContext(), "entry", func);
            IRBuilder<> builder(bEntry);


            auto args = mapLLVMFunctionArgs(func, std::vector<std::string>({"tuple_out", "str", "str_size"}));

            auto input_str = args["str"];
            auto input_str_size = args["str_size"];

            BasicBlock* bError = BasicBlock::Create(env.getContext(), "error", func);

            // init parser & open buf
            auto parser = json_retrieveParser(env, builder, input_str, input_str_size, bError);

            // now check that it is doc & row present
            auto F_parse = json_generateParseRowFunction(env, name + "_parse_row", row_type, columns, true);

            // call and check error code
            auto rc = builder.CreateCall(F_parse, {parser, args["tuple_out"]});
            // auto rc_ok = builder.CreateICmpEQ(rc, env.i64Const(ecToI64(ExceptionCode::SUCCESS)));
            json_freeParser(env, builder, parser);
            builder.CreateRet(rc);

            // fill error block (just return bad parse)
            builder.SetInsertPoint(bError);
            builder.CreateRet(env.i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT)));

            return func;
        }


        llvm::Function *JsonSourceTaskBuilder::generateParseRowFunction(const std::string& name,
                                                                        const python::Type &row_type,
                                                                        const std::vector<std::string> &columns,
                                                                        bool unwrap_first_level) {
            return json_generateParseRowFunction(*_env.get(), name, row_type, columns, unwrap_first_level);
        }

        FlattenedTuple JsonSourceTaskBuilder::generateAndCallParseRowFunction(llvm::IRBuilder<> &parent_builder,
                                                                              const std::string& name,
                                                                              const python::Type &row_type,
                                                                              const std::vector<std::string> &columns,
                                                                              bool unwrap_first_level,
                                                                              llvm::Value *parser,
                                                                              llvm::BasicBlock *bbSchemaMismatch) {
            using namespace llvm;

            auto& logger = Logger::instance().logger("codegen");

#ifdef JSON_PARSER_TRACE_MEMORY
            if(!columns.empty()) {
                std::stringstream ss; ss<<"columns for function "<<name<<": "<<columns;
                logger.debug(ss.str());
            }
#endif

            auto F = generateParseRowFunction(name, row_type, columns, unwrap_first_level);
            FlattenedTuple ft(_env.get());
            ft.init(row_type);

            BasicBlock *bbOK = BasicBlock::Create(_env->getContext(), "parse_row_ok", parent_builder.GetInsertBlock()->getParent());

            auto res_ptr = ft.alloc(parent_builder);
            auto rc = parent_builder.CreateCall(F, {parser, res_ptr});

#ifdef JSON_PARSER_TRACE_MEMORY
            _env->printValue(parent_builder, rc, "parse rc= ");
#endif

            // check rc, if not 0 goto mismatch
            auto rc_ok = parent_builder.CreateICmpEQ(rc, _env->i64Const(0));
            parent_builder.CreateCondBr(rc_ok, bbOK, bbSchemaMismatch);
            parent_builder.SetInsertPoint(bbOK);

#ifdef JSON_PARSER_TRACE_MEMORY
            _env->printValue(parent_builder, rc, "parse ok, loading flattened tuple from struct val...");
#endif

            return FlattenedTuple::fromLLVMStructVal(_env.get(), parent_builder, res_ptr, row_type);
        }

        llvm::Value *
        JsonSourceTaskBuilder::parsedBytes(llvm::IRBuilder<> &builder, llvm::Value *parser, llvm::Value *buf_size) {
            using namespace llvm;
            // call func
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_TruncatedBytes", _env->i64Type(), _env->i8ptrType());
            assert(parser && parser->getType() == _env->i8ptrType());

            auto truncated_bytes = builder.CreateCall(F, {parser});
            return builder.CreateSub(buf_size, truncated_bytes);
        }

        // json helper funcs
        llvm::Value *JsonSourceTaskBuilder::initJsonParser(llvm::IRBuilder<> &builder) {

            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_Init", _env->i8ptrType());

            auto j = builder.CreateCall(F, {});
            auto is_null = builder.CreateICmpEQ(j, _env->i8nullptr());
            exitMainFunctionWithError(builder, is_null, _env->i64Const(ecToI64(ExceptionCode::NULLERROR)));
            return j;
        }

        llvm::Value *JsonSourceTaskBuilder::openJsonBuf(llvm::IRBuilder<> &builder, llvm::Value *j, llvm::Value *buf,
                                                     llvm::Value *buf_size) {
            assert(j);
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_open", _env->i64Type(), _env->i8ptrType(),
                                         _env->i8ptrType(), _env->i64Type());
            return builder.CreateCall(F, {j, buf, buf_size});
        }

        void JsonSourceTaskBuilder::freeJsonParse(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env->getContext();
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_Free", llvm::Type::getVoidTy(ctx),
                                         _env->i8ptrType());
            builder.CreateCall(F, j);
        }

        void JsonSourceTaskBuilder::exitMainFunctionWithError(llvm::IRBuilder<> &builder, llvm::Value *exitCondition,
                                                              llvm::Value *exitCode) {
            using namespace llvm;
            auto &ctx = _env->getContext();
            auto F = builder.GetInsertBlock()->getParent();

            assert(exitCondition->getType() == _env->i1Type());
            assert(exitCode->getType() == _env->i64Type());

            // branch and exit
            BasicBlock *bbExit = BasicBlock::Create(ctx, "exit_with_error", F);
            BasicBlock *bbContinue = BasicBlock::Create(ctx, "no_error", F);
            builder.CreateCondBr(exitCondition, bbExit, bbContinue);
            builder.SetInsertPoint(bbExit);
            builder.CreateRet(builder.CreateMul(_env->i64Const(-1), exitCode)); // truncated bytes, but use here negative code.
            builder.SetInsertPoint(bbContinue);
        }

        llvm::Value *JsonSourceTaskBuilder::hasNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            auto &ctx = _env->getContext();
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_hasNextRow", ctypeToLLVM<bool>(ctx),
                                         _env->i8ptrType());

            auto v = builder.CreateCall(F, {j});
            return builder.CreateICmpEQ(v, llvm::ConstantInt::get(
                    llvm::Type::getIntNTy(ctx, ctypeToLLVM<bool>(ctx)->getIntegerBitWidth()), 1));
        }

        void JsonSourceTaskBuilder::moveToNextRow(llvm::IRBuilder<> &builder, llvm::Value *j) {
            // move
            using namespace llvm;
            auto &ctx = _env->getContext();
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_moveToNextRow", ctypeToLLVM<bool>(ctx),
                                         _env->i8ptrType());
            builder.CreateCall(F, {j});

            // update row number (inc +1)
            auto row_no = rowNumber(builder);
            builder.CreateStore(builder.CreateAdd(row_no, _env->i64Const(1)), _rowNumberVar);

            // @TODO: free everything so far??
            _env->freeAll(builder); // -> call rtfree!
        }

        llvm::Value *
        JsonSourceTaskBuilder::parseRowAsStructuredDict(llvm::IRBuilder<> &builder, const python::Type &dict_type,
                                                        llvm::Value *j, llvm::BasicBlock *bbSchemaMismatch) {
            assert(j);
            using namespace llvm;
            auto &ctx = _env->getContext();

            // get initial object
            // => this is from parser
            auto Fgetobj = getOrInsertFunction(_env->getModule().get(), "JsonParser_getObject", _env->i64Type(),
                                               _env->i8ptrType(), _env->i8ptrType()->getPointerTo(0));

            auto obj_var = _row_object_var;

            // release and store nullptr
            //builder.CreateStore(_env->i8nullptr(), obj_var);
            json_release_object(*_env, builder, obj_var);

            builder.CreateCall(Fgetobj, {j, obj_var});

            // don't forget to free everything...
            // alloc variable
            auto struct_dict_type = create_structured_dict_type(*_env.get(), dict_type);
            auto row_var = _env->CreateFirstBlockAlloca(builder, struct_dict_type);
            struct_dict_mem_zero(*_env.get(), builder, row_var, dict_type); // !!! important !!!

            // // create new block where all objects allocated for gen are freed.
            // BasicBlock* bParseFree = BasicBlock::Create(ctx, "parse_free", builder.GetInsertBlock()->getParent());

            // create dict parser and store to row_var
            JSONParseRowGenerator gen(*_env.get(), dict_type, bbSchemaMismatch);
            gen.parseToVariable(builder, builder.CreateLoad(obj_var), row_var);
//            // update free end
//            auto bParseFreeStart = bParseFree;
//            bParseFree = gen.generateFreeAllVars(bParseFree);
//
//            // jump now to parse free
//            builder.CreateBr(bParseFreeStart);
//            builder.SetInsertPoint(bParseFree);

            // create new block (post - parse)
            BasicBlock *bPostParse = BasicBlock::Create(ctx, "post_parse", builder.GetInsertBlock()->getParent());

            builder.CreateBr(bPostParse);
            builder.SetInsertPoint(bPostParse);
            // _env->debugPrint(builder, "free done.");

            // free obj_var...
            //json_freeObject(*_env.get(), builder, builder.CreateLoad(obj_var));
            json_release_object(*_env, builder, obj_var);
#ifndef NDEBUG
            builder.CreateStore(_env->i8nullptr(), obj_var);
#endif
            return row_var;
        }

        llvm::Value *JsonSourceTaskBuilder::isDocumentOfObjectType(llvm::IRBuilder<> &builder, llvm::Value *j) {
            using namespace llvm;
            auto &ctx = _env->getContext();
            auto F = getOrInsertFunction(_env->getModule().get(), "JsonParser_getDocType", _env->i64Type(),
                                         _env->i8ptrType());
            auto call_res = builder.CreateCall(F, j);
            auto cond = builder.CreateICmpEQ(call_res, _env->i64Const(JsonParser_objectDocType()));
            return cond;
        }

        FlattenedTuple JsonSourceTaskBuilder::parseRow(llvm::IRBuilder<> &builder, const python::Type &row_type,
                                                       const std::vector<std::string>& columns,
                                                       bool unwrap_first_level, llvm::Value *parser,
                                                       llvm::BasicBlock *bbSchemaMismatch) {
            auto& logger = Logger::instance().logger("codegen");
            assert(row_type.isTupleType());

#ifdef JSON_PARSER_TRACE_MEMORY
            if(!columns.empty()) {
                std::stringstream ss; ss<<"columns: "<<columns;
                logger.debug(ss.str());
            }
#endif

            FlattenedTuple ft(_env.get());
            ft.init(row_type);

            // should first level be unwrapped or not?
            if(unwrap_first_level) {
                // -> need to have columns info
                assert(columns.size() >= row_type.parameters().size());

                if(columns.size() < row_type.parameters().size()) {
                    builder.CreateBr(bbSchemaMismatch);
                    // maybe need to initialize with dummies?
                    return ft;
                }

                // parse using a struct type composed of columns and the data!
                std::vector<python::StructEntry> entries;
                auto num_entries = std::min(columns.size(), row_type.parameters().size());

#ifndef NDEBUG
                // // loading entries:
                // {
                //     std::stringstream ss;
                //     for(unsigned i = 0; i < num_entries; ++i) {
                //         ss<<i<<": "<<columns[i]<<" -> "<<row_type.parameters()[i].desc()<<std::endl;
                //     }
                //     logger.debug("columns:\n" + ss.str());
                // }
#endif

                for(unsigned i = 0; i < num_entries; ++i) {
                    python::StructEntry entry;
                    entry.keyType = python::Type::STRING;
                    entry.key = escape_to_python_str(columns[i]);
                    entry.valueType = row_type.parameters()[i];
                    entries.push_back(entry);
                }
                auto dict_type = python::Type::makeStructuredDictType(entries);

                // parse dictionary
                auto dict = parseRowAsStructuredDict(builder, dict_type, parser, bbSchemaMismatch);

                // fetch columns from dict and assign to tuple!
                for(int i = 0; i < num_entries; ++i) {
                    SerializableValue value;

                    // fetch value from dict!
                    value = struct_dict_get_or_except(*_env.get(), builder, dict_type, escape_to_python_str(columns[i]),
                                                      python::Type::STRING, dict, bbSchemaMismatch);

                    ft.set(builder, {i}, value.val, value.size, value.is_null);
                }
            } else {
                assert(row_type.parameters().size() == 1);
                auto dict_type = row_type.parameters()[0];
                assert(dict_type.isStructuredDictionaryType()); // <-- this should be true, or an alternative parsing strategy devised...
                if(!dict_type.isStructuredDictionaryType())
                    throw std::runtime_error("parsing JSON files with type " + dict_type.desc() + " not yet implemented.");

                // _env->debugPrint(builder, "starting to parse row...");
                // parse struct dict
                auto s = parseRowAsStructuredDict(builder, dict_type, parser, bbSchemaMismatch);
                // _env->debugPrint(builder, "row parsed.");

                // assign to tuple (for further processing)
                ft.setElement(builder, 0, s, nullptr, nullptr);
            }

            return ft;
        }

        void JsonSourceTaskBuilder::serializeAsNormalCaseException(llvm::IRBuilder<> &builder, llvm::Value *userData,
                                                                   int64_t operatorID, llvm::Value *row_no,
                                                                   const tuplex::codegen::FlattenedTuple &general_case_row) {
            // checks
            assert(row_no);
            assert(row_no->getType() == _env->i64Type());

#ifdef JSON_PARSER_TRACE_MEMORY
            _env->printValue(builder, row_no, "row number: ");
#endif

            // this is a regular exception -> i.e. cf. deserializeExceptionFromMemory for mem layout
            auto mem = general_case_row.serializeToMemory(builder);
            auto buf = mem.val;
            auto buf_size = mem.size;

#ifdef JSON_PARSER_TRACE_MEMORY
            _env->printValue(builder, buf_size, "emitting normal-case violation in general case format of size: ");
#endif

            // buf is now fully serialized. => call exception handler from pipeline.
            auto handler_name = exception_handler();
            if(!hasExceptionHandler() || handler_name.empty())
                throw std::runtime_error("invalid, no handler specified");

            auto eh_func = exception_handler_prototype(_env->getContext(), _env->getModule().get(), handler_name);
            llvm::Value *ecCode = _env->i64Const(ecToI64(ExceptionCode::NORMALCASEVIOLATION));
            llvm::Value *ecOpID = _env->i64Const(operatorID);
            builder.CreateCall(eh_func, {userData, ecCode, ecOpID, row_no, buf, buf_size});
        }

        void JsonSourceTaskBuilder::serializeBadParseException(llvm::IRBuilder<> &builder,
                                                               llvm::Value* userData,
                                                               int64_t operatorID,
                                                               llvm::Value *row_no,
                                                               llvm::Value *str,
                                                               llvm::Value *str_size) {

            // checks
            assert(row_no && str && str_size);
            assert(row_no->getType() == _env->i64Type());
            assert(str->getType() == _env->i8ptrType());
            assert(str_size->getType() == _env->i64Type());

            // memory layout for this is like in serializeParseException
            // yet for JSON there's only a single cell
            // buf_size = sizeof(int64_t) + numCells * sizeof(int64_t) + SUM_i sizes[i]
            // --> here:
            llvm::Value* buf_size = builder.CreateAdd(_env->i64Const(sizeof(int64_t) * 2), str_size);

            llvm::Value* buf = _env->malloc(builder, buf_size);
            llvm::Value* ptr = buf;

            // write first number of cells to serialize. ( = 1)
            builder.CreateStore(_env->i64Const(1), builder.CreatePointerCast(ptr, _env->i64ptrType()));
            ptr = builder.CreateGEP(ptr, _env->i64Const(sizeof(int64_t)));
            // write str info (size + offset)
            size_t offset = sizeof(int64_t); // trivial offset
            auto info = pack_offset_and_size(builder, _env->i64Const(offset), str_size);
            builder.CreateStore(info, builder.CreatePointerCast(ptr, _env->i64ptrType()));
            ptr = builder.CreateGEP(ptr, _env->i64Const(sizeof(int64_t)));
            // memcpy string
            builder.CreateMemCpy(ptr, 0, str, 0, str_size);

            // buf is now fully serialized. => call exception handler from pipeline.
            auto handler_name = exception_handler();
            if(!hasExceptionHandler() || handler_name.empty())
                throw std::runtime_error("invalid, no handler specified");

            auto eh_func = exception_handler_prototype(_env->getContext(), _env->getModule().get(), handler_name);
            llvm::Value *ecCode = _env->i64Const(ecToI64(ExceptionCode::BADPARSE_STRING_INPUT));
            llvm::Value *ecOpID = _env->i64Const(operatorID);
            builder.CreateCall(eh_func, {userData, ecCode, ecOpID, row_no, buf, buf_size});
        }
    }
}