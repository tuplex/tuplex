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

            }


            return _operators;
        }

        std::vector<Row> StagePlanner::fetchInputSample() {
            if(!_inputNode)
                return {};
            return _inputNode->getSample(1000);
        }
    }
}