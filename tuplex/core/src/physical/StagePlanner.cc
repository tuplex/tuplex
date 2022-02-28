//
// Created by Leonhard Spiegelberg on 2/27/22.
//

#include <physical/StagePlanner.h>
#include <iostream>
#include <sstream>

namespace tuplex {
    namespace codegen {
        std::vector<LogicalOperator*> StagePlanner::optimize() {
            using namespace std;

            auto& logger = Logger::instance().logger("specializing stage optimizer");
            // step 1: retrieve sample from inputnode!
            std::vector<Row> sample = fetchInputSample();

            if(_useConstantFolding && sample.size() >= 100) { // should have at least 100 samples to determine this...
                // check which columns could be constants and if so propagate that information!
                logger.info("Performing constant folding optimization");

                DetectionStats ds;
                ds.detect(sample);

                // print info
                cout<<"Following columns detected to be constant: "<<ds.constant_column_indices()<<endl;
                // print out which rows are considered constant (and with which values!)
                for(auto idx : ds.constant_column_indices()) {
                    string column_name;
                    if(_inputNode && !_inputNode->inputColumns().empty())
                        column_name = _inputNode->inputColumns()[idx];
                    cout<<" - "<<column_name<<": "<<ds.constant_row.get(idx).desc()<<" : "<<ds.constant_row.get(idx).getType().desc()<<endl;
                }

                // for now simplify life (HACK!) just one operator
                if(_operators.size() == 1) {
                    // apply the constant folding operation!

                    // works now only for map operator
                    auto op = _operators.front();
                    if(op->type() == LogicalOperatorType::MAP) {
                        auto mop = dynamic_cast<MapOperator*>(op);
                        assert(mop);

                        // do opt only if input cols are valid...!

                        // retype UDF
                       cout<<"input type before: "<<mop->getInputSchema().getRowType().desc()<<endl;
                       cout<<"output type before: "<<mop->getOutputSchema().getRowType().desc()<<endl;
                       cout<<"num input columns required: "<<mop->inputColumns().size()<<endl;
                       // retype
                       auto input_cols = mop->inputColumns(); // HACK! won't work if no input cols are specified.
                       auto input_type = mop->getInputSchema().getRowType();
                       if(input_cols.empty()) {
                            logger.debug("skipping, only for input cols now working...");
                            return _operators;
                       }
                       // for all constants detected, add type there & use that for folding!
                       // if(input_type.parameters().size() == 1 && input_type.parameters().front().isTupleType())
                       auto tuple_mode = input_type.parameters().size() == 1 && input_type.parameters().front().isTupleType();
                       if(!tuple_mode) {
                           logger.debug("only tuple/dict mode supported! skipping for now");
                           return _operators;
                       }

                       auto param_types = input_type.parameters()[0].parameters();
                       if(param_types.size() != input_cols.size()) {
                           logger.warn("Something wrong, numbers do not match up.");
                           return _operators;
                       }

                       // now update these vars with whatever is possible
                       std::unordered_map<std::string, python::Type> constant_types;
                       // HACK! do not change column names, else this will fail...!
                        for(auto idx : ds.constant_column_indices()) {
                            string column_name;
                            if(_inputNode && !_inputNode->inputColumns().empty()) {
                                column_name = _inputNode->inputColumns()[idx];
                                constant_types[column_name] = python::Type::makeConstantValuedType(ds.constant_row.get(idx).getType(), ds.constant_row.get(idx).desc()); // HACK
                            }
                        }
                        // lookup column names (NOTE: this should be done using integers & properly propagated through op graph)
                        for(unsigned i = 0; i < input_cols.size(); ++i) {
                            auto name = input_cols[i];
                            auto it = constant_types.find(name);
                            if(it != constant_types.end())
                                param_types[i] = it->second;
                        }

                        // now update specialized type with constant if possible!
                       auto specialized_type = tuple_mode ? python::Type::makeTupleType({python::Type::makeTupleType(param_types)}) : python::Type::makeTupleType(param_types);
                       if(specialized_type != input_type) {
                           cout<<"specialized type "<<input_type.desc()<<endl;
                           cout<<"  - to - "<<endl;
                           cout<<specialized_type.desc()<<endl;
                       } else {
                           cout<<"no specialization possible, same type";
                           // @TODO: can skip THIS optimization, continue with the next one!
                       }

                       mop->retype({specialized_type});

                       // now check again what columns are required from input, if different count -> push down!
                       // @TODO: this could get difficult for general graphs...
                       auto accCols = mop->getUDF().getAccessedColumns();
                        // Note: this works ONLY for now, because no other op after this...


                       // check again
                        cout<<"input type after: "<<mop->getInputSchema().getRowType().desc()<<endl;
                        cout<<"output type after: "<<mop->getOutputSchema().getRowType().desc()<<endl;
                        cout<<"num input columns required after opt: "<<accCols.size()<<endl;
                    }

                }


            }

            // check accesses -> i.e. need to check for all funcs till first map or end of stage is reached.
            // why? b.c. map destroys structure. The other require analysis though...!
            // i.e. trace using sample... (this could get expensive!)




            return _operators;
        }

        std::vector<Row> StagePlanner::fetchInputSample() {
            if(!_inputNode)
                return {};
            return _inputNode->getSample(1000);
        }
    }
}