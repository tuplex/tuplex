//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "physical/CSVParseRowGenerator.h"
#include <Logger.h>
#include <ExceptionCodes.h>
#include <Base.h>
#include <StringUtils.h>
#include <RuntimeInterface.h>

//#define TRACE_PARSER

namespace tuplex {

    namespace codegen {
        // new implementation of parser
        CSVParseRowGenerator &CSVParseRowGenerator::addCell(const python::Type &type, bool serialize) {
            // only primitive types so far possible!
            auto t = type.isOptionType() ? type.getReturnType() : type;
            if (!t.isPrimitiveType()) {
                Logger::instance().defaultLogger().error(
                        "only primitive types supported for parser generation yet! Type " + type.desc() +
                        " not yet supported!");
                return *this;
            }

            // lazy add
            CSVCellDesc desc;
            desc.type = type;
            desc.willBeSerialized = serialize;
            _cellDescs.push_back(desc);

            return *this;
        }

        python::Type CSVParseRowGenerator::serializedType() const {
            std::vector<python::Type> types;
            for (const auto c : _cellDescs) {
                if (c.willBeSerialized)
                    types.push_back(c.type);
            }
            return python::Type::makeTupleType(types);
        }

        llvm::Type *CSVParseRowGenerator::resultType() const {
            if (!_resultType) {
                using namespace llvm;
                auto &context = _env->getContext();
                bool packed = false;
                std::string twine = "csvparse_t";
                auto i8ptr_type = Type::getInt8PtrTy(context, 0);


                std::vector<Type *> vTypes;
                vTypes.push_back(_env->i64Type());
                vTypes.push_back(i8ptr_type); // trimmed line start
                vTypes.push_back(i8ptr_type); // trimmed line end


                // bitmap
                auto numBitmapElements = bitmapBitCount() / 64;
                assert(bitmapBitCount() % 64 == 0); // check it's a multiple of 64!
                assert(numBitmapElements > 0);

                // create fixed size array type
                vTypes.push_back(ArrayType::get(_env->i64Type(), numBitmapElements));

                // append fields for serialized values.
                // ==> for option types, ignore!
                auto stype = serializedType();
                for (const auto& t : stype.parameters()) {
                    vTypes.push_back(_env->pythonToLLVMType(t.withoutOptions()));
                    vTypes.push_back(_env->i64Type()); // size field
                }

                // optional serialized parse info in case parse fails
                // (i.e. individual cells, might be necessary for null-value opt)
                vTypes.push_back(_env->i64Type());      // buf_length
                vTypes.push_back(_env->i8ptrType());    // buf

                // create struct type (lazily?)
                llvm::ArrayRef<llvm::Type *> members(vTypes);
                llvm::Type *structType = llvm::StructType::create(context, members, twine, packed);

                // bad hack here...
                const_cast<CSVParseRowGenerator *>(this)->_resultType = structType;
            }
            return _resultType;
        }

        void CSVParseRowGenerator::updateLookAhead(IRBuilder& builder) {
            auto ptr = builder.CreateLoad(_currentPtrVar);
            auto lessThanEnd = builder.CreateICmpULT(ptr, _endPtr);
            auto la = builder.CreateSelect(lessThanEnd, builder.CreateLoad(builder.CreateGEP(ptr, _env->i32Const(1))),
                                           _env->i8Const(_escapechar));
            builder.CreateStore(la, _currentLookAheadVar);

        }

        llvm::Value *CSVParseRowGenerator::newlineCondition(IRBuilder& builder, llvm::Value *curChar) {
            assert(curChar->getType() == llvm::Type::getInt8Ty(_env->getContext()));
            auto left = builder.CreateICmpEQ(curChar, _env->i8Const('\n'));
            auto right = builder.CreateICmpEQ(curChar, _env->i8Const('\r'));
            return builder.CreateOr(left, right);
        }

        llvm::Value *
        CSVParseRowGenerator::generateCellSpannerCode(IRBuilder& builder, char c1, char c2, char c3, char c4) {
            auto &context = _env->getContext();
            using namespace llvm;

            // look into godbolt
            // for following code...
            //  char c1 = ',';
            //  char c2 = '\r';
            //  char c3 = '\n';
            //  char c4 = '\0';
            //  __v16qi vq = {c1, c2, c3, c4};
            //  __m128i _v = (__m128i)vq;
            // const char *buf = "Hello world";
            // size_t pos = _mm_cmpistri(_v, _mm_loadu_si128((__m128i*)buf), 0);
            auto v16qi_val = builder.CreateAlloca(v16qi_type(context));
            uint64_t idx = 0ul;
            llvm::Value *whereToStore = builder.CreateLoad(v16qi_val);
            whereToStore = builder.CreateInsertElement(whereToStore, _env->i8Const(c1), idx++);
            whereToStore = builder.CreateInsertElement(whereToStore, _env->i8Const(c2), idx++);
            whereToStore = builder.CreateInsertElement(whereToStore, _env->i8Const(c3), idx++);
            whereToStore = builder.CreateInsertElement(whereToStore, _env->i8Const(c4), idx++);
            for (int i = 4; i < 16; ++i)
                whereToStore = builder.CreateInsertElement(whereToStore, _env->i8Const(0), idx++);

            builder.CreateStore(whereToStore, v16qi_val);
            return v16qi_val;
        }


        llvm::Value *
        CSVParseRowGenerator::executeSpanner(IRBuilder& builder, llvm::Value *spanner, llvm::Value *ptr) {
            auto &context = _env->getContext();
            using namespace llvm;

            assert(ptr->getType() == Type::getInt8PtrTy(context, 0));



            // unsafe version: this requires that there are 15 zeroed bytes after endptr at least
            auto val = builder.CreateLoad(spanner);
            auto casted_ptr = builder.CreateBitCast(ptr, v16qi_type(context)->getPointerTo(0));

            Function *pcmpistri128func = Intrinsic::getDeclaration(_env->getModule().get(),
                                                                   LLVMIntrinsic::x86_sse42_pcmpistri128);
            auto res = builder.CreateCall(pcmpistri128func, {val, builder.CreateLoad(casted_ptr), _env->i8Const(0)});
            return res;

            //  // safe version, i.e. when 16 byte border is not guaranteed.
            //  Function* pcmpistri128func = Intrinsic::getDeclaration(_env->getModule().get(), Intrinsic::x86_sse42_pcmpistri128);
            //  BasicBlock* bEnoughBytesLeft = BasicBlock::Create(context, "execute_spanner", _func);
            //  BasicBlock* bAtEndOfFile = BasicBlock::Create(context, "spanner_at_end_of_file", _func);
            //  BasicBlock* bSpannerDone = BasicBlock::Create(context, "spanner_done", _func);
            //
            //  auto val = builder.CreateLoad(spanner);
            //  auto bytesLeft = builder.CreateSub(builder.CreatePtrToInt(_endPtr, _env->i64Type()),
            //                                     builder.CreatePtrToInt(ptr, _env->i64Type()));
            //  _env->printValue(builder, bytesLeft, "bytes left: ");
            //  auto enoughBytesLeftCond = builder.CreateICmpUGE(bytesLeft, _env->i64Const(16));
            //  llvm::Value* resVar = builder.CreateAlloca(_env->i32Type());
            //  builder.CreateCondBr(enoughBytesLeftCond, bEnoughBytesLeft, bAtEndOfFile);
            //
            //
            //  builder.SetInsertPoint(bEnoughBytesLeft);
            //  auto v16qi_type = llvm::VectorType::get(llvm::Type::getInt8Ty(context), 16);
            //  auto casted_ptr = builder.CreateBitCast(ptr, v16qi_type->getPointerTo(0));
            //
            //
            //  builder.CreateStore(builder.CreateCall(pcmpistri128func, {val, builder.CreateLoad(casted_ptr), _env->i8Const(0)}), resVar);
            //  builder.CreateBr(bSpannerDone);
            //
            //  // more complicated, fill values based on how many bytes are left.
            //  builder.SetInsertPoint(bAtEndOfFile);
            //
            //  _env->printValue(builder, bytesLeft, "in at end of file for spanner, there are bytes left: ");
            //
            //  auto v16qi_val = builder.CreateAlloca(v16qi_type);
            //  uint64_t idx = 0ul;
            //  llvm::Value* whereToStore = builder.CreateLoad(v16qi_val);
            //
            //  llvm::Value* curPtr = ptr;
            //  for(int i = 0; i < 16; ++i) {
            //      auto value = builder.CreateSelect(builder.CreateICmpULT(curPtr, _endPtr),
            //              builder.CreateLoad(curPtr), _env->i8Const(_escapechar));
            //      curPtr = builder.CreateGEP(curPtr, _env->i32Const(1));
            //      whereToStore = builder.CreateInsertElement(whereToStore, value, idx++);
            //  }
            //  builder.CreateStore(whereToStore, v16qi_val);
            //
            //  // spanner with v16qi_val
            //  auto spanner_result = builder.CreateCall(pcmpistri128func, {val, builder.CreateLoad(v16qi_val), _env->i8Const(0)});
            //
            //  // minimum with bytes left (spanner may be 16 or so)
            //  auto i32BytesLeft = builder.CreateSExtOrTrunc(bytesLeft, _env->i32Type());
            //  auto eofRes = builder.CreateSelect(builder.CreateICmpULT(i32BytesLeft,
            //          spanner_result), i32BytesLeft, spanner_result);
            //  builder.CreateStore(eofRes, resVar);
            //  builder.CreateBr(bSpannerDone);
            //
            //  // continue code gen
            //  builder.SetInsertPoint(bSpannerDone);
            //  auto res = builder.CreateLoad(resVar);
            //  return res;
        }

        void CSVParseRowGenerator::buildUnquotedCellBlocks(llvm::BasicBlock *bUnquotedCellBegin,
                                                           llvm::BasicBlock *bCellDone) {
            using namespace llvm;
            auto &context = _env->getContext();

            BasicBlock *bUnquotedCellEnd = BasicBlock::Create(context, "unquoted_cell_end", _func);
            BasicBlock *bUnquotedCellBeginSkipEntry = BasicBlock::Create(context, "unquoted_cell_begin_skip", _func);


            IRBuilder builder(bUnquotedCellBegin);
            //_env->debugPrint(builder, "entering unquoted cell begin", _env->i64Const(0));
            // save cell begin ptr
            saveCellBegin(builder);

            builder.CreateBr(bUnquotedCellBeginSkipEntry);

            builder.SetInsertPoint(bUnquotedCellBeginSkipEntry);

            // use fallback or SSE4.2.? change this here...
#ifdef SSE42_MODE
            // call spanner
            auto spannerResult = executeSpanner(builder, _unquotedSpanner, currentPtr(builder));
#else
#error "backup solution needs to be added."
#endif

            consume(builder, spannerResult);
            auto curChar = currentChar(builder);// safe version

            // check what current char is:
            // if ',', then this cell is done --> go to cell done!
            // if '\0' or spannerResult=0, then end of file is reached.
            // if '\r' or '\n is encountered, parse is done.
            // else, skip again
            BasicBlock *bNextUnquoted = BasicBlock::Create(context, "unquoted_shortcircuit", _func);
            auto lookAheadIsDelimiter = builder.CreateICmpEQ(curChar, _env->i8Const(_delimiter));
            builder.CreateCondBr(lookAheadIsDelimiter, bUnquotedCellEnd, bNextUnquoted);


            builder.SetInsertPoint(bNextUnquoted);
            auto isEndOfFile = builder.CreateICmpEQ(curChar, _env->i8Const(_escapechar));

            auto isEndOfFileOrNewline = builder.CreateOr(isEndOfFile, newlineCondition(builder, curChar));
            builder.CreateCondBr(isEndOfFileOrNewline, bUnquotedCellEnd, bUnquotedCellBeginSkipEntry);


            builder.SetInsertPoint(bUnquotedCellEnd);
            saveCellEnd(builder, 0);
            builder.CreateBr(bCellDone);
        }

        void CSVParseRowGenerator::buildQuotedCellBlocks(llvm::BasicBlock *bQuotedCellBegin,
                                                         llvm::BasicBlock *bCellDone) {
            using namespace llvm;
            auto &context = _env->getContext();


            BasicBlock *bQuotedCellEnd = BasicBlock::Create(context, "quoted_cell_end", _func);
            BasicBlock *bQuotedCellBeginSkipEntry = BasicBlock::Create(context, "quoted_cell_begin_skip", _func);
            BasicBlock *bQuotedCellDQError = BasicBlock::Create(context, "quoted_cell_double_quote_error", _func);
            BasicBlock *bQuotedCellDQCheck = BasicBlock::Create(context, "quoted_cell_double_quote_check", _func);
            BasicBlock *bQuotedCellEndCheck = BasicBlock::Create(context, "quoted_cell_end_reached_check", _func);
            IRBuilder builder(bQuotedCellBegin);

            // (1) ------------------------------------------------------------------------
            //     Quoted Cell begin block [consume ", save cell start]
            //     ------------------------------------------------------------------------
            builder.SetInsertPoint(bQuotedCellBegin);
            // consume 1 char, i.e. the quotechar
            consume(builder, 1);
            // and save cell begin ptr with one offset (i.e. ignore the quote!)
            saveCellBegin(builder);

            builder.CreateBr(bQuotedCellBeginSkipEntry);


            // (2) ------------------------------------------------------------------------
            //     Quoted Cell skip entry block [execute spanner till " or \0 is found]
            //     ------------------------------------------------------------------------
            builder.SetInsertPoint(bQuotedCellBeginSkipEntry);
            // use fallback or SSE4.2.? change this here...
#ifdef SSE42_MODE
            // call spanner
            auto spannerResult = executeSpanner(builder, _quotedSpanner, currentPtr(builder));
#else
#error "fallback needs to be implemented"
#endif

            // consume result
            consume(builder, spannerResult);


            // now need to check what the current char is
            // two options:
            // (1) " quotechar
            // (2) end of file.
            //     => this is actually illegal because there is no closing character then!
            //        thus return doublequote error here
            // (3) else:
            //     => continue skipping
            auto curChar = builder.CreateLoad(currentPtr(builder));

            auto isEndOfFile = builder.CreateICmpEQ(curChar, _env->i8Const(_escapechar));
            builder.CreateCondBr(isEndOfFile, bQuotedCellDQError, bQuotedCellDQCheck);

            // (3) ------------------------------------------------------------------------
            //     Double quote error. I.e. file ended but no closing " was found
            //     ------------------------------------------------------------------------
            builder.SetInsertPoint(bQuotedCellDQError);
            // save cell ptr
            saveCellEnd(builder);
            saveLineEnd(builder);
            fillResultCode(builder, true);
            builder.CreateRet(_env->i32Const(ecToI32(ExceptionCode::DOUBLEQUOTEERROR)));

            // (4) ------------------------------------------------------------------------
            //     check whether result is quotechar ". If so go to special block in order
            //     to check whether to continue parsing OR to stop. Else, loop.
            //     ------------------------------------------------------------------------
            builder.SetInsertPoint(bQuotedCellDQCheck);
            auto isQuoteChar = builder.CreateICmpEQ(curChar, _env->i8Const(_quotechar));
            builder.CreateCondBr(isQuoteChar, bQuotedCellEndCheck, bQuotedCellBeginSkipEntry);

            // (5) ------------------------------------------------------------------------
            //     check for end: the lastChar must be " here. what is the next char?
            //     if it is ", then there is a double quote. ->skip one char and then loop again
            //     if it is , or \n or \r or \0, then the cell ended.
            //     if it something else, then we have a single " in a double quoted field.
            //     best thing todo is then to continue parsing.
            //     i.e. condition used here is to check whether next char is in {',', '\n', '\r', '\0'}
            //     ------------------------------------------------------------------------
            builder.SetInsertPoint(bQuotedCellEndCheck);
            auto lastChar = builder.CreateLoad(currentPtr(builder));
            auto nextChar = lookahead(builder);

            auto isNewLine = newlineCondition(builder, nextChar);
            auto cellDoneCond = builder.CreateOr(builder.CreateICmpEQ(nextChar, _env->i8Const(_escapechar)),
                                                 builder.CreateOr(
                                                         builder.CreateICmpEQ(nextChar, _env->i8Const(_delimiter)),
                                                         isNewLine));
            // always consume here one character
            consume(builder, 1);
            builder.CreateCondBr(cellDoneCond, bQuotedCellEnd, bQuotedCellBeginSkipEntry);

            // (6) ------------------------------------------------------------------------
            //     quoted cell ends. Save here cellend. Because only predecessor is (5)
            //     and there in any case a character is consumed, save currenptr -1 position
            //     to cell end
            //     ------------------------------------------------------------------------
            builder.SetInsertPoint(bQuotedCellEnd);
            saveCellEnd(builder, -1);
            builder.CreateBr(bCellDone);
        }

        void CSVParseRowGenerator::build(bool internalOnly) {
            using namespace llvm;
            auto &context = _env->getContext();
            auto linkage = internalOnly ? Function::InternalLinkage : Function::ExternalLinkage;
            auto func_name = "parse_row";
            auto i8ptr_type = Type::getInt8PtrTy(context, 0);

            createFunction(internalOnly);

            // start with BasicBlock
            assert(_func);
            BasicBlock *bEntry = BasicBlock::Create(context, "entry",
                                                    _func); // entry block, first one to be entered with all variables being setup here
            BasicBlock *bSetup = BasicBlock::Create(context, "setup_variables", _func);
            BasicBlock *bEmptyInput = BasicBlock::Create(context, "empty_input", _func);
            BasicBlock *bNewlineSkipCond = BasicBlock::Create(context, "newlineskip_cond",
                                                              _func); // new line skip fun loop
            BasicBlock *bNewlineSkipBody = BasicBlock::Create(context, "newlineskip_body", _func);
            BasicBlock *bNewCell = BasicBlock::Create(context, "newcell", _func); // cell begin
            BasicBlock *bNewLine = BasicBlock::Create(context, "newline", _func); // line begin
            BasicBlock *bQuotedCellBegin = BasicBlock::Create(context, "quoted_cell_begin", _func);
            BasicBlock *bUnquotedCellBegin = BasicBlock::Create(context, "unquoted_cell_begin", _func);

            BasicBlock *bCellDone = BasicBlock::Create(context, "cell_done", _func);
            BasicBlock *bParseDone = BasicBlock::Create(context, "parse_done", _func);
            IRBuilder builder(bEntry);
            _lineBeginVar = builder.CreateAlloca(i8ptr_type);
            _lineEndVar = builder.CreateAlloca(i8ptr_type);

            // check whether startptr=endptr
            assert(_inputPtr && _endPtr);
            auto emptyString = builder.CreateICmpUGE(_inputPtr, _endPtr); // us ge to make it a little more safe
            builder.CreateCondBr(emptyString, bEmptyInput, bSetup);

            // bEmptyInput
            builder.SetInsertPoint(bEmptyInput);
            // fill result code
            assert(_resultPtr);
            auto idx0 = builder.CreateGEP(_resultPtr, {_env->i32Const(0), _env->i32Const(0)});
            auto idx1 = builder.CreateGEP(_resultPtr, {_env->i32Const(0), _env->i32Const(1)});
            auto idx2 = builder.CreateGEP(_resultPtr, {_env->i32Const(0), _env->i32Const(2)});
            builder.CreateStore(_env->i64Const(0), idx0);
            builder.CreateStore(llvm::ConstantPointerNull::get(Type::getInt8PtrTy(context, 0)), idx1);
            builder.CreateStore(llvm::ConstantPointerNull::get(Type::getInt8PtrTy(context, 0)), idx2);
            builder.CreateRet(_env->i32Const(ecToI32(ExceptionCode::SUCCESS)));

            // continue setup
            builder.SetInsertPoint(bSetup);

            _currentPtrVar = builder.CreateAlloca(i8ptr_type);
            _currentLookAheadVar = builder.CreateAlloca(Type::getInt8Ty(context));
            _cellBeginVar = builder.CreateAlloca(i8ptr_type);
            _cellEndVar = builder.CreateAlloca(i8ptr_type);
            _cellNoVar = builder.CreateAlloca(_env->i32Type());
            _ecVar = builder.CreateAlloca(_env->i32Type());

            // where to store the stuff
            _storeIndexVar = builder.CreateAlloca(_env->i32Type());
            _storedCellBeginsVar = builder.CreateAlloca(i8ptr_type, 0, _env->i32Const(numCellsToSerialize()));
            _storedCellEndsVar = builder.CreateAlloca(i8ptr_type, 0, _env->i32Const(numCellsToSerialize()));

#ifdef SSE42_MODE
            _quotedSpanner = generateCellSpannerCode(builder, _quotechar, _escapechar);
            _unquotedSpanner = generateCellSpannerCode(builder, _delimiter, '\r', '\n', _escapechar);
#else
#error "fallback missing here"
#endif

            // setup current ptr and look ahead
            builder.CreateStore(_inputPtr, _currentPtrVar);
            updateLookAhead(builder);

            builder.CreateStore(_env->i32Const(0), _cellNoVar);
            builder.CreateStore(_env->i32Const(0), _ecVar);

            builder.CreateStore(_env->i32Const(0), _storeIndexVar);

            // go to newline skip
            builder.CreateBr(bNewlineSkipCond);

            // newline setup
            builder.SetInsertPoint(bNewlineSkipCond);
            auto isNewline = newlineCondition(builder, builder.CreateLoad(currentPtr(builder)));
            builder.CreateCondBr(isNewline, bNewlineSkipBody, bNewLine);

            // newline skip
            builder.SetInsertPoint(bNewlineSkipBody);
            // consume 1 character
            consume(builder, 1);
            builder.CreateBr(bNewlineSkipCond);


            // new line
            builder.SetInsertPoint(bNewLine);
            // save line begin
            saveLineBegin(builder);
            // start with new cell
            builder.CreateBr(bNewCell);


            // now at cell begin
            builder.SetInsertPoint(bNewCell);

            // check lookahead and decide whether to parse unquoted or quoted cell!
            auto isQuote = builder.CreateICmpEQ(builder.CreateLoad(currentPtr(builder)), _env->i8Const(_quotechar));
            builder.CreateCondBr(isQuote, bQuotedCellBegin, bUnquotedCellBegin);



            //  vars to use
            llvm::Value *spannerResult = nullptr;
            llvm::Value *lookAheadIsDelimiter = nullptr;
            llvm::Value *isEndOfFile = nullptr;
            llvm::Value *isEndOfFileOrNewline = nullptr;
            llvm::Value *curChar = nullptr;

            // ------------------------------------------------------------
            // quoted cell
            buildQuotedCellBlocks(bQuotedCellBegin, bCellDone);
            // ------------------------------------------------------------


            // ------------------------------------------------------------
            // unquoted cell
            buildUnquotedCellBlocks(bUnquotedCellBegin, bCellDone);



            // ------------------------------------------------------------
            // Other logic
            // ------------------------------------------------------------
            builder.SetInsertPoint(bCellDone);
            curChar = currentChar(builder);
            // serialize here...
            // logic is: if cellNo <= numCells, then store it in prepared vector
            saveCurrentCell(builder);
            // update cell counter
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(_cellNoVar), _env->i32Const(1)), _cellNoVar);
            // serialize end...


            // now check what curChar is:
            // is it ',' ? --> consume char and go to cell begin
            // is it '\n' or '\r' --> stop parsing, line is done.
            // is it '\0' --> stop parsing, line is done
            BasicBlock *bCellDone_SC_I = BasicBlock::Create(context, "celldone_shortcircuit_I", _func);
            BasicBlock *bCellDone_SC_II = BasicBlock::Create(context, "celldone_shortcircuit_II", _func);
            BasicBlock *bFatalError = BasicBlock::Create(context, "fatal_error", _func);
            BasicBlock *bNextCell = BasicBlock::Create(context, "goto_nextcell", _func);

            lookAheadIsDelimiter = builder.CreateICmpEQ(curChar, _env->i8Const(_delimiter));
            builder.CreateCondBr(lookAheadIsDelimiter, bNextCell, bCellDone_SC_I);

            builder.SetInsertPoint(bNextCell);
            consume(builder, 1);
            builder.CreateBr(bNewCell);

            builder.SetInsertPoint(bCellDone_SC_I);
            builder.CreateCondBr(newlineCondition(builder, curChar), bParseDone, bCellDone_SC_II);

            builder.SetInsertPoint(bCellDone_SC_II);
            builder.CreateCondBr(builder.CreateICmpEQ(curChar, _env->i8Const(_escapechar)), bParseDone, bFatalError);

            builder.SetInsertPoint(bFatalError);
            builder.CreateRet(_env->i32Const(-1));

            createParseDoneBlocks(bParseDone);
        }

        void CSVParseRowGenerator::createParseDoneBlocks(llvm::BasicBlock *bParseDone) {
            using namespace llvm;
            auto &context = _env->getContext();

            IRBuilder builder(bParseDone);
            saveLineEnd(builder); // depending


            // do here now first part of exception generation.
            // check whether number of exception fits
            BasicBlock *bCorrectNoOfCells = BasicBlock::Create(context, "correct_no_of_cells", _func);
            BasicBlock *bWrongNoOfCells = BasicBlock::Create(context, "wrong_no_of_cells", _func);

            auto correctNoOfCellCond = builder.CreateICmpEQ(_env->i32Const(numCells()), builder.CreateLoad(_cellNoVar));
            builder.CreateCondBr(correctNoOfCellCond, bCorrectNoOfCells, bWrongNoOfCells);


            // Wrong number of cells...
            builder.SetInsertPoint(bWrongNoOfCells);

            fillResultCode(builder, true);

            // select return code
            auto retCode = builder.CreateSelect(
                    builder.CreateICmpULT(builder.CreateLoad(_cellNoVar), _env->i32Const(numCells())),
                    _env->i32Const(ecToI32(ExceptionCode::CSV_UNDERRUN)),
                    _env->i32Const(ecToI32(ExceptionCode::CSV_OVERRUN)));
            builder.CreateRet(retCode);


            builder.SetInsertPoint(bCorrectNoOfCells);
            // success here, so fill result with what it needs
            fillResultCode(builder, false);
        }

        void CSVParseRowGenerator::saveCurrentCell(IRBuilder& builder) {
            using namespace llvm;
            auto &context = _env->getContext();

            // get current cellNo
            auto curCellNo = builder.CreateLoad(_cellNoVar);

            // check if less than equal number of saved cells
            auto canStore = builder.CreateICmpUGE(_env->i32Const(numCells()), curCellNo);

            // note: also add condition which cells shall be stored:
            // this is to subselect what cells to store
            canStore = builder.CreateAnd(canStore, storageCondition(builder, curCellNo));

            BasicBlock *bCanStore = BasicBlock::Create(context, "saveCell", _func);
            BasicBlock *bDone = BasicBlock::Create(context, "savedCell", _func);
            builder.CreateCondBr(canStore, bCanStore, bDone);

            builder.SetInsertPoint(bCanStore);

            // make sure indexvar is not larger than the rest!!!
            auto curIdx = builder.CreateLoad(_storeIndexVar);
            // set to vector
            auto idxBegin = builder.CreateGEP(_storedCellBeginsVar, curIdx);
            auto idxEnd = builder.CreateGEP(_storedCellEndsVar, curIdx);

            builder.CreateStore(builder.CreateLoad(_cellBeginVar), idxBegin);
            builder.CreateStore(builder.CreateLoad(_cellEndVar), idxEnd);
            builder.CreateStore(builder.CreateAdd(curIdx, _env->i32Const(1)), _storeIndexVar);
            builder.CreateBr(bDone);

            // update for new commands
            builder.SetInsertPoint(bDone);
        }


        void
        CSVParseRowGenerator::storeParseInfo(IRBuilder& builder, llvm::Value *lineStart, llvm::Value *lineEnd,
                                             llvm::Value *numParsedBytes) {
            assert(_resultPtr);
            assert(_resultPtr->getType() == resultType()->getPointerTo(0));

            assert(lineStart && lineEnd && numParsedBytes);

            assert(lineStart->getType() == _env->i8ptrType());
            assert(lineEnd->getType() == _env->i8ptrType());
            assert(numParsedBytes->getType() == _env->i64Type());

            // in any case, fill how many bytes have been parsed + line start/line end
            auto idx0 = builder.CreateGEP(_resultPtr, {_env->i32Const(0), _env->i32Const(0)});
            auto idx1 = builder.CreateGEP(_resultPtr, {_env->i32Const(0), _env->i32Const(1)});
            auto idx2 = builder.CreateGEP(_resultPtr, {_env->i32Const(0), _env->i32Const(2)});

            builder.CreateStore(numParsedBytes, idx0);
            builder.CreateStore(lineStart, idx1);
            builder.CreateStore(lineEnd, idx2);

            // store all 0s in bitmap
            auto numBitmapElements = bitmapBitCount() / 64;

            for (int i = 0; i < numBitmapElements; ++i) {
                auto idx = builder.CreateGEP(_resultPtr, {_env->i32Const(0), _env->i32Const(3), _env->i32Const(i)});
                builder.CreateStore(_env->i64Const(0), idx);
            }

            // store nullptr, 0 in error buf
            auto num_struct_elements = resultType()->getStructNumElements();
            auto idx_buf_length = _env->CreateStructGEP(builder, _resultPtr, num_struct_elements -2);
            auto idx_buf = _env->CreateStructGEP(builder, _resultPtr, num_struct_elements - 1);
            assert(idx_buf_length->getType() == _env->i64ptrType());
            assert(idx_buf->getType() == _env->i8ptrType()->getPointerTo());
            _env->storeNULL(builder, idx_buf_length);
            _env->storeNULL(builder, idx_buf);
        }


        void
        CSVParseRowGenerator::storeValue(IRBuilder& builder, int column, llvm::Value *val, llvm::Value *size,
                                         llvm::Value *isnull) {
            assert(0 <= column && column < _cellDescs.size());

            if (val) {
                auto idxVal = builder.CreateGEP(_resultPtr, {_env->i32Const(0), _env->i32Const(3 + 1 + 2 * column)});
                builder.CreateStore(val, idxVal);
            }

            if (size) {
                auto idxSize = builder.CreateGEP(_resultPtr,
                                                 {_env->i32Const(0), _env->i32Const(3 + 1 + 2 * column + 1)});
                builder.CreateStore(size, idxSize);
            }

            // store bit in bitmap
            if (isnull) {
                // fetch byte, load val
                auto idxQword = builder.CreateGEP(_resultPtr,
                                                  {_env->i32Const(0), _env->i32Const(3), _env->i32Const(column / 64)});
                auto qword = builder.CreateLoad(idxQword);
                auto new_qword = builder.CreateOr(qword, builder.CreateShl(builder.CreateZExt(isnull, _env->i64Type()),
                                                                           _env->i64Const(column % 64)));

                builder.CreateStore(new_qword, idxQword);
            }
        }


        codegen::SerializableValue
        CSVParseRowGenerator::getColumnResult(IRBuilder& builder, int column, llvm::Value *result) const {

            // make sure column is within range!
            assert(0 <= column && column < serializedType().parameters().size());


            // cast result type if necessary
            if(result->getType() != resultType()->getPointerTo(0) && result->getType() == _env->i8ptrType())
                throw std::runtime_error("result is not pointer of resulttype in " __FILE__);

            auto t = serializedType().parameters()[column]; // Note: this here is accessing only serialized cells!

            llvm::Value *val = builder.CreateLoad(
                    builder.CreateGEP(result, {_env->i32Const(0), _env->i32Const(3 + 1 + 2 * column)}));
            llvm::Value *size = builder.CreateLoad(
                    builder.CreateGEP(result, {_env->i32Const(0), _env->i32Const(3 + 1 + 2 * column + 1)}));

            llvm::Value *isnull = nullptr;

            if (python::Type::STRING == t || python::Type::makeOptionType(python::Type::STRING) == t)
                // safely zero terminate strings before further processing...
                // this will lead to some copies that are unavoidable...
                val = _env->zeroTerminateString(builder, val, size);

            // option type?
            if (t.isOptionType()) {
                // extract bitmap bit!
                // fetch byte, load val
                auto idxQword = builder.CreateGEP(result,
                                                  {_env->i32Const(0), _env->i32Const(3), _env->i32Const(column / 64)});
                auto qword = builder.CreateLoad(idxQword);

                isnull = builder.CreateICmpNE(builder.CreateAnd(qword, _env->i64Const(1UL << (static_cast<uint64_t>(column) % 64))),
                                              _env->i64Const(0));
            }

            return codegen::SerializableValue(val, size, isnull);
        }


        llvm::Function* CSVParseRowGenerator::getCSVNormalizeFunc() {
            using namespace llvm;

            auto& context = _env->getContext();

            // normalize/dequote func
            FunctionType *normFT = FunctionType::get(ctypeToLLVM<char *>(context),
                                                     {ctypeToLLVM<char>(context), ctypeToLLVM<char *>(context),
                                                      ctypeToLLVM<char *>(context),
                                                      ctypeToLLVM<int64_t *>(context)}, false);


#if LLVM_VERSION_MAJOR < 9
            // compatibility
            Function* normalizeFunc = cast<Function>(_env->getModule()->getOrInsertFunction("csvNormalize", normFT));
#else
            Function* normalizeFunc = cast<Function>(_env->getModule()->getOrInsertFunction("csvNormalize", normFT).getCallee());

#endif
            return normalizeFunc;
        }

        // @Todo: maybe rename this
        void CSVParseRowGenerator::fillResultCode(IRBuilder& builder, bool errorOccured) {
            using namespace llvm;
            auto &context = _env->getContext();
            auto i8ptr_type = Type::getInt8PtrTy(context, 0);

            auto lineStart = builder.CreateLoad(_lineBeginVar);
            auto lineEnd = builder.CreateLoad(_lineEndVar);

            auto ret_size_ptr = _env->CreateFirstBlockAlloca(builder, _env->i64Type());

            storeParseInfo(builder, lineStart, lineEnd, numParsedBytes(builder));


            // create block for special error codes
            BasicBlock* bbValueError = BasicBlock::Create(context, "null_schema_mismatch", builder.GetInsertBlock()->getParent());
            BasicBlock* bbNullError = BasicBlock::Create(context, "null_schema_mismatch", builder.GetInsertBlock()->getParent());
            IRBuilder errBuilder(bbValueError);
            storeBadParseInfo(errBuilder);
            errbuilder.CreateRet(_env->i32Const(ecToI32(ExceptionCode::VALUEERROR))); // i.e. raised for bad number parse
            errBuilder.SetInsertPoint(bbNullError);
            storeBadParseInfo(errBuilder);
            errbuilder.CreateRet(_env->i32Const(ecToI32(ExceptionCode::NULLERROR))); // i.e. raised for null value

            auto normalizeFunc = getCSVNormalizeFunc();

            // in the case of no error, generate serialization code with short circuit error handling
            size_t pos = 0;
            if (!errorOccured) {
                for (unsigned i = 0; i < _cellDescs.size(); ++i) {
                    auto desc = _cellDescs[i];

                    // should cell be serialized?
                    if (desc.willBeSerialized) {

                        //BasicBlock *bIsNullValue = BasicBlock::Create(context, "cell" + std::to_string(i) + "_is_null", _func);
                        //BasicBlock *bNotNull = BasicBlock::Create(context, "cell" + std::to_string(i) + "_not_null", _func);

                        llvm::Value *cellBegin = builder.CreateLoad(
                                builder.CreateGEP(_storedCellBeginsVar, _env->i32Const(pos)));
                        llvm::Value *cellEnd = builder.CreateLoad(
                                builder.CreateGEP(_storedCellEndsVar, _env->i32Const(pos)));
                        auto cellEndIncl = cellEnd;
                        // cellEnd is the char included. Many functions need though the one without the end.
                        auto cellEndExcl = builder.CreateGEP(cellEnd, _env->i32Const(1));

                        // special case: single digit/single char values.
                        // i.e. we know it is not a null value. Hence, add +1 to cellEnd to allow for conversion
                        cellEnd = builder.CreateSelect(builder.CreateICmpEQ(cellBegin, cellEnd),
                                                       clampWithEndPtr(builder,
                                                                       builder.CreateGEP(cellEnd, _env->i32Const(1))),
                                                       cellEnd);

                        // // uncomment following lines to display which cell is saved
                        // // debug:
                        //  _env->debugPrint(builder, "cell ", _env->i64Const(i));
                        //  _env->debugCellPrint(builder, cellBegin, cellEnd);
                        auto normalizedStr = builder.CreateCall(normalizeFunc, {_env->i8Const(_quotechar), cellBegin, cellEndIncl, ret_size_ptr});

                        //_env->debugPrint(builder, "column " + std::to_string(i) + " normalized str: ", normalizedStr);
                        //_env->debugPrint(builder, "column " + std::to_string(i) + " normalized str isnull: ", _env->compareToNullValues(builder, normalizedStr, _null_values));

                        auto type = desc.type;

                        // NEW, faster code:
                        // first, check whether option type ==> i.e. nullable will NOT raise a null error, for others fail here early ...

                        // Note: we could save copying over the string. However, for now we always call the csvNormalize func
                        //       which internally creates a copy. Therefore, strings will be null-terminated.
                        auto valueIsNull = _env->compareToNullValues(builder, normalizedStr, _null_values, true);

                        // allocate vars where to store parse result or dummy
                        Value* valPtr = _env->CreateFirstBlockAlloca(builder, _env->pythonToLLVMType(type.withoutOptions()), "col" + std::to_string(pos));
                        Value* sizePtr = _env->CreateFirstBlockAlloca(builder, _env->i64Type(), "col" + std::to_string(pos) + "_size");
                        // null them
                        _env->storeNULL(builder, valPtr);
                        _env->storeNULL(builder, sizePtr);

                        // hack: nullable string, store empty string!
                        if(type.withoutOptions() == python::Type::STRING) {
                            builder.CreateStore(_env->strConst(builder, ""), valPtr);
                        }

                        // if option type, null is ok. I.e. only parse if not null
                        BasicBlock* bbParseDone = BasicBlock::Create(context, "parse_done_col" + std::to_string(pos), _func);
                        if(type.isOptionType()) {
                            BasicBlock* bbParseField = BasicBlock::Create(context, "parse_value_col" + std::to_string(pos), _func);
                            builder.CreateCondBr(valueIsNull, bbParseDone, bbParseField);
                            builder.SetInsertPoint(bbParseField);
#ifdef TRACE_PARSER
                            _env->debugPrint(builder, "value is not null, so parsing field...");
#endif
                        }

                        // parse here according to type and raise error for failure...
                        if(python::Type::BOOLEAN == type.withoutOptions()) {
                            // call fast_atob(...)
                            std::vector<Type *> argtypes{i8ptr_type, i8ptr_type,
                                                         Type::getInt8PtrTy(context, 0)}; // bool is implemented as i8*
                            FunctionType *FT = FunctionType::get(Type::getInt32Ty(context), argtypes, false);
                            auto func = _env->getModule()->getOrInsertFunction("fast_atob", FT);
                            auto resCode = builder.CreateCall(func, {cellBegin, cellEnd, valPtr});
                            builder.CreateStore(_env->i64Const(sizeof(int64_t)), sizePtr);
                            auto parseOK = builder.CreateICmpEQ(resCode, _env->i32Const(ecToI32(ExceptionCode::SUCCESS)));
                            builder.CreateCondBr(parseOK, bbParseDone, bbValueError);

                        } else if(python::Type::I64 == type.withoutOptions()) {
                            // call fast_atoi64(...)
                            std::vector<Type *> argtypes{i8ptr_type, i8ptr_type, _env->i64Type()->getPointerTo(0)};
                            FunctionType *FT = FunctionType::get(Type::getInt32Ty(context), argtypes, false);
                            auto func = _env->getModule()->getOrInsertFunction("fast_atoi64", FT);
                            auto resCode = builder.CreateCall(func, {cellBegin, cellEnd, valPtr});
                            builder.CreateStore(_env->i64Const(sizeof(int64_t)), sizePtr);
                            auto parseOK = builder.CreateICmpEQ(resCode, _env->i32Const(ecToI32(ExceptionCode::SUCCESS)));
                            builder.CreateCondBr(parseOK, bbParseDone, bbValueError);

                        } else if(python::Type::F64 == type.withoutOptions()) {
                            // call fast_atod(...)
                            std::vector<Type *> argtypes{i8ptr_type, i8ptr_type, _env->doubleType()->getPointerTo(0)};
                            FunctionType *FT = FunctionType::get(Type::getInt32Ty(context), argtypes, false);
                            auto func = _env->getModule()->getOrInsertFunction("fast_atod", FT);
                            auto resCode = builder.CreateCall(func, {cellBegin, cellEnd, valPtr});
                            builder.CreateStore(_env->i64Const(sizeof(double)), sizePtr);
                            auto parseOK = builder.CreateICmpEQ(resCode, _env->i32Const(ecToI32(ExceptionCode::SUCCESS)));
                            builder.CreateCondBr(parseOK, bbParseDone, bbValueError);

                        } else if(python::Type::STRING == type.withoutOptions()) {
                            // super simple, just store result!
                            builder.CreateStore(normalizedStr, valPtr);
                            builder.CreateStore(builder.CreateLoad(ret_size_ptr), sizePtr);
                            builder.CreateBr(bbParseDone);
                        } else if(python::Type::NULLVALUE == type.withoutOptions()) {

                            // trivial, do not need to store anything, just go to parse done
                            builder.CreateBr(bbParseDone);
                        }
                        else throw std::runtime_error("type " + type.desc() + " not yet supported in parse row generator!");

                        // parse done, store values, then next thing
                        builder.SetInsertPoint(bbParseDone);
#ifdef TRACE_PARSER
                        // debug
                        _env->debugPrint(builder, "column " + std::to_string(i) + " normalized str: ", normalizedStr);
                        _env->debugPrint(builder, "column " + std::to_string(i) + " value: ", builder.CreateLoad(valPtr));
                        _env->debugPrint(builder, "column " + std::to_string(i) + " size: ", builder.CreateLoad(sizePtr));
                        _env->debugPrint(builder, "column " + std::to_string(i) + " isnull: ", valueIsNull);
#endif
                        storeValue(builder, pos, builder.CreateLoad(valPtr), builder.CreateLoad(sizePtr), valueIsNull);

                        pos++;
                    }
                }


                // add serialization of parse info for NULL and ValueError case
                // @TODO

                // for error blocks, check whether they have predecessors. If not, remove them from the function!
#if LLVM_VERSION_MAJOR < 9
                if(!hasPredecessor(bbNullError))
                    bbNullError->eraseFromParent();
                if(!hasPredecessor(bbValueError))
                    bbValueError->eraseFromParent();
#else
                if(!bbNullError->hasNPredecessorsOrMore(0))
                    bbNullError->eraseFromParent();
                if(!bbValueError->hasNPredecessorsOrMore(0))
                    bbValueError->eraseFromParent();
#endif

                // all, ok
                builder.CreateRet(_env->i32Const(ecToI32(ExceptionCode::SUCCESS)));
            }
        }

        void CSVParseRowGenerator::createFunction(bool internalOnly) {
            using namespace llvm;
            using namespace std;
            auto &context = _env->getContext();
            auto i8ptr_type = Type::getInt8PtrTy(context, 0);

            // create function
            // takes the following arguments:
            // returntype*, i8* ptr, i8* endptr
            // function returns i32 (i.e. the exception code)
            vector<llvm::Type *> param_types = {resultType()->getPointerTo(0), i8ptr_type, i8ptr_type};

            llvm::FunctionType *func_type = llvm::FunctionType::get(_env->i32Type(), param_types, false);

            _func = llvm::Function::Create(func_type,
                                           internalOnly ? llvm::GlobalValue::InternalLinkage
                                                        : llvm::GlobalValue::ExternalLinkage,
                                           "parse_row",
                                           _env->getModule().get());


//
//            AttrBuilder ab;
//
//            // deactivate to lower compilation time?
//            // ab.addAttribute(Attribute::AlwaysInline);
//            _func->addAttributes(llvm::AttributeList::FunctionIndex, ab);

            vector<llvm::Value *> args;
            int counter = 0;
            for (auto &arg : _func->args()) {
                args.push_back(&arg);
                counter++;
            }

            // setup internal variables to this
            assert(args.size() == 3);
            _resultPtr = args[0];
            _inputPtr = args[1];
            _endPtr = args[2];
        }

        void CSVParseRowGenerator::storeBadParseInfo(const IRBuilder& builder) {
            using namespace llvm;
            using namespace std;

            auto normalizeFunc = getCSVNormalizeFunc();
            auto ret_size_ptr = _env->CreateFirstBlockAlloca(builder, _env->i64Type());

            // calc total size & rtmalloc
            // => use only if correct num of cells... no partial restore...
            // this is for null value optimization
            // super simple, just store result!


            vector<Value*> cells; // dequoted i8*
            vector<Value*> cell_sizes; // i64

            int pos = 0;
            for (unsigned i = 0; i < _cellDescs.size(); ++i) {
                auto desc = _cellDescs[i];

                // should cell be serialized?
                if (desc.willBeSerialized) {
                    llvm::Value *cellBegin = builder.CreateLoad(
                            builder.CreateGEP(_storedCellBeginsVar, _env->i32Const(pos)));
                    llvm::Value *cellEnd = builder.CreateLoad(
                            builder.CreateGEP(_storedCellEndsVar, _env->i32Const(pos)));
                    auto cellEndIncl = cellEnd;

                    auto normalizedStr = builder.CreateCall(normalizeFunc,
                                                            {_env->i8Const(_quotechar), cellBegin, cellEndIncl,
                                                             ret_size_ptr});
                    cells.push_back(normalizedStr);
                    cell_sizes.push_back(builder.CreateLoad(ret_size_ptr));
                    pos++;
                }
            }

            // encode now very easily incl. offsets & Co
            // C++ code
            // auto buf_ptr = (char*)allocator(buf_size);
            // auto buf = buf_ptr;
            // *(int64_t*)buf = numCells;
            // buf += sizeof(int64_t);
            //
            // // write size info
            // size_t acc_size = 0;
            // for(int i = 0; i < numCells; ++i) {
            //     uint64_t info = (uint64_t)sizes[i] & 0xFFFFFFFF;
            //
            //     // offset = jump + acc size
            //     uint64_t offset = (numCells - i) * sizeof(int64_t) + acc_size;
            //     *(uint64_t*)buf = (info << 32u) | offset;
            //     memcpy(buf_ptr + sizeof(int64_t) * (numCells + 1) + acc_size, cells[i], sizes[i]);
            //
            //     // memcmp check?
            //     assert(memcmp(buf + offset, cells[i], sizes[i]) == 0);
            //
            //     buf += sizeof(int64_t);
            //     acc_size += sizes[i];
            // }
            Value* buf_size = _env->i64Const(sizeof(int64_t) * (cells.size() + 1));
            for(auto s: cell_sizes)buf_size = builder.CreateAdd(buf_size, s);

            Value* buf = _env->malloc(builder, buf_size);
            auto lastPtr = buf;
            // store num_cells!
            builder.CreateStore(_env->i64Const(cells.size()), builder.CreateBitCast(lastPtr, _env->i64ptrType()));
            lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)));
            Value* acc_size = _env->i64Const(0);
            for(int i = 0; i < cells.size(); ++i) {

#ifndef NDEBUG
                // _env->debugPrint(builder, "cell(" + std::to_string(i) + "): ", cells[i]);
#endif


                //     uint64_t offset = (numCells - i) * sizeof(int64_t) + acc_size;
                Value* offset = builder.CreateAdd(acc_size, _env->i64Const((cells.size() - i) * sizeof(int64_t)));

                //     info = (size << 32u) | offset;
                Value* info = builder.CreateOr(offset, builder.CreateShl(cell_sizes[i], _env->i64Const(32)));

                //     *(uint64_t*)buf = info
                builder.CreateStore(info, builder.CreateBitCast(lastPtr, _env->i64ptrType()));

                // copy cell content
                //     memcpy(buf_ptr + sizeof(int64_t) * (numCells + 1) + acc_size, cells[i], sizes[i]);
                auto cell_idx = builder.CreateGEP(buf, builder.CreateAdd(acc_size, _env->i64Const(sizeof(int64_t) * (cells.size() + 1))));
                builder.CreateMemCpy(cell_idx, 0, cells[i], 0, cell_sizes[i]);

                //     buf += sizeof(int64_t);
                //     acc_size += sizes[i];
                lastPtr = builder.CreateGEP(lastPtr, _env->i32Const(sizeof(int64_t)));
                acc_size = builder.CreateAdd(acc_size, cell_sizes[i]);
            }


            // store buf + buf_size into ret struct
            auto num_struct_elements = resultType()->getStructNumElements();
            auto idx_buf_length = _env->CreateStructGEP(builder, _resultPtr, num_struct_elements -2);
            auto idx_buf = _env->CreateStructGEP(builder, _resultPtr, num_struct_elements - 1);
            assert(idx_buf_length->getType() == _env->i64ptrType());
            assert(idx_buf->getType() == _env->i8ptrType()->getPointerTo());
            builder.CreateStore(buf, idx_buf);
            builder.CreateStore(buf_size, idx_buf_length);
        }

        SerializableValue CSVParseRowGenerator::getCellInfo(IRBuilder& builder, llvm::Value *result) const {
            using namespace llvm;

            // cast result type if necessary
            if(result->getType() != resultType()->getPointerTo(0) && result->getType() == _env->i8ptrType())
                throw std::runtime_error("result is not pointer of resulttype in " __FILE__);

            auto num_struct_elements = resultType()->getStructNumElements();
            auto idx_buf_length = _env->CreateStructGEP(builder, result, num_struct_elements -2);
            auto idx_buf = _env->CreateStructGEP(builder, result, num_struct_elements - 1);
            assert(idx_buf_length->getType() == _env->i64ptrType());
            assert(idx_buf->getType() == _env->i8ptrType()->getPointerTo());
            return SerializableValue(builder.CreateLoad(idx_buf), builder.CreateLoad(idx_buf_length));
        }
    }
}