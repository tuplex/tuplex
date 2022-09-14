//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_MAPCOLUMNOPERATOR_H
#define TUPLEX_MAPCOLUMNOPERATOR_H

#include "LogicalOperator.h"
#include "UDFOperator.h"

namespace tuplex {

    class MapColumnOperator : public UDFOperator {
    private:
        std::string _columnToMap;
        int         _columnToMapIndex;
    public:
        std::shared_ptr<LogicalOperator> clone(bool cloneParents) override;

#ifdef BUILD_WITH_CEREAL
        // cereal serialization functions
        template<class Archive> void save(Archive &ar) const {
            ar(::cereal::base_class<UDFOperator>(this), _columnToMap, _columnToMapIndex);
        }
        template<class Archive> void load(Archive &ar) {
            ar(::cereal::base_class<UDFOperator>(this), _columnToMap, _columnToMapIndex);
        }
#endif
    protected:
        Schema inferSchema(Schema parentSchema) override;
    public:
        // required by cereal
        MapColumnOperator() = default;

        MapColumnOperator(const std::shared_ptr<LogicalOperator>& parent,
                          const std::string& columnName,
                          const std::vector<std::string>& columns,
                          const UDF& udf,
                          const std::unordered_map<size_t, size_t>& rewriteMap={});
        // needs a parent

        std::string name() override { return "mapColumn"; }
        LogicalOperatorType type() const override { return LogicalOperatorType::MAPCOLUMN; }

        bool isActionable() override { return false; }
        bool isDataSource() override { return false; }

        bool good() const override { return _columnToMapIndex >= 0; }

        void setDataSet(DataSet* dsptr) override;

        std::vector<Row> getSample(const size_t num) const override;

        int getColumnIndex() const { assert(_columnToMapIndex >= 0); return _columnToMapIndex; }
        std::string columnToMap() const { return _columnToMap; }

        void rewriteParametersInAST(const std::unordered_map<size_t, size_t>& rewriteMap) override;

        Schema getInputSchema() const override {
            return parent()->getOutputSchema(); // overwrite here, because UDFOperator always returns the UDF's input schema. However, for mapColumn it's not a row but an element!
        }

        bool retype(const python::Type& input_row_type, bool is_projected_row_type) override;
    };

}

#ifdef BUILD_WITH_CEREAL
CEREAL_REGISTER_TYPE(tuplex::MapColumnOperator);
#endif

#endif //TUPLEX_MAPCOLUMNOPERATOR_H