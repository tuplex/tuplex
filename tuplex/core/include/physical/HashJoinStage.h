//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_HASHJOINSTAGE_H
#define TUPLEX_HASHJOINSTAGE_H

#include "PhysicalStage.h"
#include <logical/JoinOperator.h>

namespace tuplex {
    class HashJoinStage : public PhysicalStage {
    public:
        HashJoinStage() = delete;
        HashJoinStage(PhysicalPlan *plan,
                      IBackend *backend,
                      PhysicalStage* left,
                      PhysicalStage* right,
                      int64_t leftKeyIndex,
                      const python::Type& leftRowType,
                      int64_t rightKeyIndex,
                      const python::Type& rightRowType,
                      const JoinType& jt,
                      int64_t stage_number,
                      int64_t outputDataSetID);

        /*!
        * fetch data into resultset
        * @return resultset of this stage
        */
        std::shared_ptr<ResultSet> resultSet() const override { return _rs; }

        PhysicalStage* left() const { return predecessors()[0]; }
        PhysicalStage* right() const { return predecessors()[1]; }

        int64_t leftKeyIndex() const {return _leftKeyIndex;}
        int64_t rightKeyIndex() const {return _rightKeyIndex;}
        python::Type leftType() const {return _leftRowType;}
        python::Type rightType() const {return _rightRowType;}

        int64_t outputDataSetID() const { return _outputDataSetID; } // TODO change to the real thing

        void setResultSet(const std::shared_ptr<ResultSet>& rs) { _rs = rs;}

        std::string generateCode();

        std::string probeFunctionName() const { return  "stage" + std::to_string(number()) + "_hash_probe"; }

        std::string writeRowFunctionName() const { return "stage" + std::to_string(number()) + "_hash_write_row"; }

        python::Type combinedType() const {

            // @TODO: should come from operator, no?

            // combined schema from row type
            std::vector<python::Type> combinedTypes;
            for(int i = 0; i < leftType().parameters().size(); ++i) {
                auto t = leftType().parameters()[i];
                if(i != leftKeyIndex())
                    combinedTypes.push_back(t);
            }
            combinedTypes.push_back(leftType().parameters()[leftKeyIndex()]);
            for(int i = 0; i < rightType().parameters().size(); ++i) {
                auto t = rightType().parameters()[i];
                if(i != rightKeyIndex()) {
                    // important to make option type (nullable in left join)
                    switch(_joinType) {
                        case JoinType::LEFT: {
                            combinedTypes.push_back(python::Type::makeOptionType(t));
                            break;
                        }
                        case JoinType::INNER: {
                            combinedTypes.push_back(t);
                            break;
                        }
                        default: {
                            throw std::runtime_error("join type not implemented");
                        }
                    }
                }
            }
            return python::Type::makeTupleType(combinedTypes);
        }

    private:
        std::shared_ptr<ResultSet> _rs;
        int64_t _leftKeyIndex;
        int64_t _rightKeyIndex;
        python::Type _rightRowType;
        python::Type _leftRowType;
        JoinType _joinType;
        int64_t _outputDataSetID;

        void generateProbingCode(std::shared_ptr<codegen::LLVMEnvironment>& env,
                llvm::IRBuilder<>& builder,
                                 llvm::Value *userData,
                                 llvm::Value *hashMap,
                llvm::Value* ptrVar,
                llvm::Value* hashedValueVar,
                const python::Type& buildType,
                int buildKeyIndex,
                const python::Type& probeType,
                int probeKeyIndex,
                const JoinType& jt);

        llvm::Value* makeKey(std::shared_ptr<codegen::LLVMEnvironment>& env,
                llvm::IRBuilder<>& builder, const python::Type& type, const codegen::SerializableValue& key);

        void writeJoinResult(std::shared_ptr<codegen::LLVMEnvironment>& env,
                             llvm::IRBuilder<>& builder,
                             llvm::Value* userData,
                             llvm::Value* bucketPtr,
                             const python::Type& buildType, int buildKeyIndex,
                             const codegen::FlattenedTuple& ftProbe, int probeKeyIndex);
        void writeBuildNullResult(std::shared_ptr<codegen::LLVMEnvironment>& env,
                             llvm::IRBuilder<>& builder,
                             llvm::Value* userData,
                             const python::Type& buildType, int buildKeyIndex,
                             const codegen::FlattenedTuple& ftProbe, int probeKeyIndex);
    };
}

#endif //TUPLEX_HASHJOINSTAGE_H