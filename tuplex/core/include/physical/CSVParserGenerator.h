//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_CSVPARSERGENERATOR_H
#define TUPLEX_CSVPARSERGENERATOR_H

#include <LLVMEnvironment.h>
#include <TypeSystem.h>
#include <CodegenHelper.h>
#include <ExceptionCodes.h>
#include "CSVParseRowGenerator.h"
#include "IExceptionableTaskGenerator.h"
#include "CodeDefs.h"

namespace tuplex {

    namespace codegen {
        class CSVParserGenerator : public IExceptionableTaskGenerator {
        private:
            // generator class for a single parseRow statement
            CSVParseRowGenerator _rowGenerator;

            bool _skipHeader;

            // exception handling
            IExceptionableTaskGenerator::exceptionHandler_f _handler; //! function to be called in exception case
            int64_t _operatorID; //! the operator ID to be associated with the handler

            // llvm variables
            llvm::Value *_resStructVar;
            llvm::Value *_currentPtrVar;
            llvm::Value *_endPtr;

        public:
            CSVParserGenerator(const std::shared_ptr<LLVMEnvironment> &env, bool skipHeader,
                               const std::vector<std::string> &null_values,
                               char quotechar = '"',
                               char delimiter = ',',
                               char escapechar = '\0') : IExceptionableTaskGenerator(env), _skipHeader(skipHeader),
                                                         _handler(nullptr),
                                                         _operatorID(-1),
                                                         _resStructVar(nullptr),
                                                         _currentPtrVar(nullptr),
                                                         _endPtr(nullptr),
                                                         _rowGenerator(env.get(), null_values, quotechar, delimiter,
                                                                       escapechar) {
            }

            /*!
             * adds code to parse a cell. If serialize if specified to be true, cell contents will be automatically serialized/converted.
             * @param type
             * @param serialize
             * @return
             */
            CSVParserGenerator &addCell(const python::Type &type, bool serialize);


            void
            addExceptionHandler(IExceptionableTaskGenerator::exceptionHandler_f handler, int64_t exceptionOperatorID) {
                // lazy init. stuff will be build in Build function
                _handler = handler;
                _operatorID = exceptionOperatorID;
            }

            void build(IExceptionableTaskGenerator::reqMemory_f requestOutputMemory);

            /*!
             * returns the row type of the data that will be serialized through this parser in memory. (i.e. allows to skip certain cells)
             * @return
             */
            python::Type serializedType() const { return _rowGenerator.serializedType(); }
        };
    }
}
#endif //TUPLEX_CSVPARSERGENERATOR_H