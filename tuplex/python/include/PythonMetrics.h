//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_PYTHONMETRICS_H
#define TUPLEX_PYTHONMETRICS_H

#include <utils/JobMetrics.h>

namespace tuplex {

    /*!
    * metrics abstraction of C++ class which provides python bindings
    */
    class PythonMetrics {

        private:
            std::shared_ptr<JobMetrics> _metrics;
            friend class PythonContext;

        public:
            PythonMetrics(): _metrics(nullptr)  {}
            /*!
            * wraps JobMetrics object in PythonMetrics object
            * @param metrics pointer to JobMetrics object
            */
            void wrap(std::shared_ptr<JobMetrics> metrics) {
                assert(metrics);
                _metrics = metrics;
            }
            /*!
            * getter for total exception count
            * @returns a size_t representing total exception count
            */            
            size_t getTotalExceptionCount() {
                return _metrics->totalExceptionCount;
            }

            /*!
            * getter for logical optimization time
            * @returns a double representing logical optimization time
            */            
            double getLogicalOptimizationTime() {
                return _metrics->getLogicalOptimizationTime();
            }
            /*!
            * getter for llvm optimization time
            * @returns a double representing llvm optimization time
            */            
            double getLLVMOptimizationTime() {
                return _metrics->getLLVMOptimizationTime();
            }
            /*!
            * getter for compilation time via llvm
            * @returns a double representing compilation time via llvm
            */            
            double getLLVMCompilationTime() {
                return _metrics->getLLVMCompilationTime();
            }
            /*!
            * getter for total compilation time
            * @returns a double representing total compilation time
            */            
            double getTotalCompilationTime() {
                return _metrics->getTotalCompilationTime();
            }
            /*!
            * getter for total time it takes to generate LLVM time (i.e.
            * for StageBuilder::build to run)
            * @returns a double representing time in seconds
            */
            double getGenerateLLVMTime() {
                return _metrics->getGenerateLLVMTime();
            }
            
            /*!
             * returns metrics as json string
             * @return string object with all metrics encoded
             */
            std::string getJSONString() const {
                return _metrics->to_json();
            }
            
    };

}
#endif //TUPLEX_PYTHONMETRICS_H