//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_JOBMETRICS_H
#define TUPLEX_JOBMETRICS_H

#include <Base.h>
#include <unordered_map>
#include <ExceptionCodes.h>

namespace tuplex {
    // a collection of metrics, easily accessible
    // and updateable
    // When adding a new member variable, make sure to update to_json as well,
    // and the python metrics object!
    class JobMetrics {
    private:
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> _exception_counts;
        double _logical_optimization_time_s = 0.0;
        double _llvm_optimization_time_s = 0.0;
        double _llvm_compilation_time_s = 0.0;
        double _total_compilation_time_s = 0.0;
        double _sampling_time_s = 0.0;

        // numbers per stage, can get combined in case.
        struct StageMetrics {
            int stageNo;
            double fast_path_wall_time_s = 0.0;
            double fast_path_time_s = 0.0;
            double slow_path_wall_time_s = 0.0;
            double slow_path_time_s = 0.0;
            double fast_path_per_row_time_ns = 0.0;
            double slow_path_per_row_time_ns = 0.0;

             size_t fast_path_input_row_count = 0;
             size_t fast_path_output_row_count = 0;
             size_t slow_path_input_row_count = 0;
             size_t slow_path_output_row_count = 0;

             double write_output_wall_time_s = 0.0;

            // disk spilling metrics
            int partitions_swapin_count = 0;
            int partitions_swapout_count = 0;
            size_t partitions_bytes_swapped_out = 0;
            size_t partitions_bytes_swapped_in = 0;
        };
        std::vector<StageMetrics> _stage_metrics;

        inline std::vector<StageMetrics>::iterator get_or_create_stage_metrics(int stageNo) {
            auto it = std::find_if(_stage_metrics.begin(), _stage_metrics.end(),
                                   [stageNo](const StageMetrics& m) { return m.stageNo == stageNo; });
            if(it == _stage_metrics.end()) {
                _stage_metrics.push_back(StageMetrics());
                _stage_metrics.back().stageNo = stageNo;
                it = std::find_if(_stage_metrics.begin(), _stage_metrics.end(),
                                  [stageNo](const StageMetrics& m) { return m.stageNo == stageNo; });
                assert(it != _stage_metrics.end());
            }
            return it;
        }

    public:
        size_t totalExceptionCount; //! how many exceptions occured in total for the job?

        inline void setExceptionCounts(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts) {
            _exception_counts = ecounts;
        }

        // set back to neutral values
        inline void reset() {
            totalExceptionCount = 0;
        }

        inline std::unordered_map<std::string, size_t> getOperatorExceptionCounts(int64_t operatorID) const {
            std::unordered_map<std::string, size_t> counts;
            for(const auto& keyval : _exception_counts) {
                auto opID = std::get<0>(keyval.first);
                auto ec = std::get<1>(keyval.first);
                auto c = keyval.second;
                if(opID == operatorID)
                    counts[exceptionCodeToPythonClass(ec)] = c;
            }
            return counts;
        }
        /*!
        * setter for logical optimization time
        * @param time a double representing logical optimization time in s
        */  
        void setLogicalOptimizationTime(double time) {
            _logical_optimization_time_s = time;
        }
        /*!
        * setter for llvm optimization time
        * @param time a double representing llvm optimization time in s
        */ 
        void setLLVMOptimizationTime(double time) {
            _llvm_optimization_time_s = time;
        }
        /*!
        * setter for compilation time via llvm
        * @param time a double representing compilation time via llvm in s
        */  
        void setLLVMCompilationTime(double time) {
            _llvm_compilation_time_s = time;
        }
        /*!
        * setter for total compilation time
        * @param time a double representing total compilation time in s
        */
        void setTotalCompilationTime(double time) {
            _total_compilation_time_s = time;
        }
        /*!
        * getter for logical optimization time
        * @returns a double representing logical optimization time in s
        */    
        double getLogicalOptimizationTime() {
            return _logical_optimization_time_s;
        }
        /*!
        * getter for llvm optimization time
        * @returns a double representing llvm optimization time in s
        */ 
        double getLLVMOptimizationTime() {
            return _llvm_optimization_time_s;
        }
        /*!
        * getter for compilation time via llvm
        * @returns a double representing compilation time via llvm in s
        */   
        double getLLVMCompilationTime() {
            return _llvm_compilation_time_s;
        }
        /*!
        * getter for total compilation time
        * @returns a double representing total compilation time in s
        */   
        double getTotalCompilationTime() {
            return _total_compilation_time_s;
        }

        /*!
         * set slow path timing info
         * @param stageNo
         * @param slow_path_wall_time_s
         * @param slow_path_time_s
         * @param slow_path_per_row_time_ns
         */
        void setSlowPathTimes(int stageNo,
                          double slow_path_wall_time_s,
                          double slow_path_time_s,
                          double slow_path_per_row_time_ns) {
            auto it = get_or_create_stage_metrics(stageNo);
            it->slow_path_wall_time_s = slow_path_wall_time_s;
            it->slow_path_time_s = slow_path_time_s;
            it->slow_path_per_row_time_ns = slow_path_per_row_time_ns;
        }

        void setFastPathRowCount(int stageNo, size_t inputRows, size_t outputRows) {
            auto it = get_or_create_stage_metrics(stageNo);
            it->fast_path_input_row_count = inputRows;
            it->fast_path_output_row_count = outputRows;
        }

        void setSlowPathRowCount(int stageNo, size_t inputRows, size_t outputRows) {
            auto it = get_or_create_stage_metrics(stageNo);
            it->slow_path_input_row_count = inputRows;
            it->slow_path_output_row_count = outputRows;
        }

        void setWriteOutputTimes(int stageNo, double wallTime) {
            auto it = get_or_create_stage_metrics(stageNo);
            it->write_output_wall_time_s = wallTime;
        }

        /*!
         * set fast path timing info
         * @param stageNo
         * @param fast_path_wall_time_s
         * @param fast_path_time_s
         * @param fast_path_per_row_time_ns
         */
        void setFastPathTimes(int stageNo,
                          double fast_path_wall_time_s,
                          double fast_path_time_s,
                          double fast_path_per_row_time_ns) {
            auto it = get_or_create_stage_metrics(stageNo);
            it->fast_path_wall_time_s = fast_path_wall_time_s;
            it->fast_path_time_s = fast_path_time_s;
            it->fast_path_per_row_time_ns = fast_path_per_row_time_ns;
        }

        /*!
         * add statistics for disk spilling (per stage)
         * @param stageNo stage
         * @param partitions_swapin_count how often partitions were swapped in
         * @param partitions_bytes_swapped_in how many bytes were swapped in
         * @param partitions_swapout_count how often partitions were swapped out
         * @param partitions_bytes_swapped_out how many bytes were swapped out
         */
        void setDiskSpillStatistics(int stageNo,
                                    int partitions_swapin_count,
                                    size_t partitions_bytes_swapped_in,
                                    int partitions_swapout_count,
                                    size_t partitions_bytes_swapped_out) {
            auto it = get_or_create_stage_metrics(stageNo);
            it->partitions_swapin_count = partitions_swapin_count;
            it->partitions_swapout_count = partitions_swapout_count;
            it->partitions_bytes_swapped_in = partitions_bytes_swapped_in;
            it->partitions_bytes_swapped_out = partitions_bytes_swapped_out;
        }

        /*!
         * set sampling time in s for specific operator
         * @param time
         */
        void setSamplingTime(double time) {
            _sampling_time_s = time;
        }

        /*!
         * create json representation of all datapoints
         * @return string in json format
         */
        std::string to_json() const {
            using namespace std;
            stringstream ss;
            ss<<"{";
            ss<<"\"logical_optimization_time_s\":"<<_logical_optimization_time_s<<",";
            ss<<"\"llvm_optimization_time_s\":"<<_llvm_optimization_time_s<<",";
            ss<<"\"llvm_compilation_time_s\":"<<_llvm_compilation_time_s<<",";
            ss<<"\"total_compilation_time_s\":"<<_total_compilation_time_s<<",";
            ss<<"\"sampling_time_s\":"<<_sampling_time_s<<",";

            // per stage numbers
            ss<<"\"stages\":[";
            for(const auto& s : _stage_metrics) {
                ss<<"{";
                ss<<"\"stage_no\":"<<s.stageNo<<",";
                ss<<"\"fast_path_wall_time_s\":"<<s.fast_path_wall_time_s<<",";
                ss<<"\"fast_path_time_s\":"<<s.fast_path_time_s<<",";
                ss<<"\"fast_path_per_row_time_ns\":"<<s.fast_path_per_row_time_ns<<",";
                ss<<"\"slow_path_wall_time_s\":"<<s.slow_path_wall_time_s<<",";
                ss<<"\"slow_path_time_s\":"<<s.slow_path_time_s<<",";
                ss<<"\"slow_path_per_row_time_ns\":"<<s.slow_path_per_row_time_ns<<",";
                ss<<"\"partitions_swapin_count\":"<<s.partitions_swapin_count<<",";
                ss<<"\"partitions_swapout_count\":"<<s.partitions_swapout_count<<",";
                ss<<"\"partitions_bytes_swapped_in\":"<<s.partitions_bytes_swapped_in<<",";
                ss<<"\"partitions_bytes_swapped_out\":"<<s.partitions_bytes_swapped_out;
                ss<<"\"fast_path_input_row_count\":"<<s.fast_path_input_row_count<<",";
                ss<<"\"fast_path_output_row_count\":"<<s.fast_path_output_row_count<<",";
                ss<<"\"slow_path_input_row_count\":"<<s.slow_path_input_row_count<<",";
                ss<<"\"slow_path_output_row_count\":"<<s.slow_path_output_row_count<<",";
                ss<<"\"write_output_wall_time_s\":"<<s.write_output_wall_time_s;
                ss<<"}";
                if(s.stageNo != _stage_metrics.back().stageNo)
                    ss<<",";
            }
            ss<<"]";

            ss<<"}";
            return ss.str();
        }

    };
}
#endif //TUPLEX_JOBMETRICS_H