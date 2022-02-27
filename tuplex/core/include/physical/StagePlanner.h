//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_STAGEBUILDER_H
#define TUPLEX_STAGEBUILDER_H

#include "TransformStage.h"

//// this here is the class to create a specialized version of a stage.
//namespace tuplex {
//    namespace codegen {
//        class StagePlanner {
//        public:
//            StagePlanner(LogicalOperator* inputNode, const std::vector<LogicalOperator*>& operators);
//
//
//
//            void enableAll() {
//                enableNullValueOptimization();
//                enableConstantFoldingOptimization();
//            }
//        private:
//            bool _useNVO;
//            bool _
//
//
//        };
//    }
//}