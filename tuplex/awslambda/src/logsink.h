//
// Created by Leonhard Spiegelberg on 7/8/22.
//

#ifndef TUPLEX_LOGSINK_H
#define TUPLEX_LOGSINK_H

#include <nlohmann/json.hpp>
#include <google/protobuf/util/json_util.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/base_sink.h>
#include <sstream>
#include <utils/Messages.h>
#include <Utils.h>

namespace tuplex {
    // custom spdlog sink that can be reset and used to append to a (protobuf) message
    template<typename Mutex>
    class memory_sink : public spdlog::sinks::base_sink <Mutex> {
    public:
        inline std::string get_messages() const {
            return _msg_stream.str();
        }

        inline void add_to_proto_message(messages::InvocationResponse& response, const std::string& id = "") const {
            // zip message (logs can be large)
            auto compressed_messages = compress_string(get_messages());

            auto r = response.add_resources();
            if(!r)
                return;
            r->set_type(ResourceType::LOG);
            r->set_payload(compressed_messages);
            r->set_id(id);
        }

        inline void reset() {
            _msg_stream.str("");
            _msg_stream.clear();
            _msg_stream.seekg(0, std::ios::beg);
        }

    protected:
        void sink_it_(const spdlog::details::log_msg& msg) override {

            // log_msg is a struct containing the log entry info like level, timestamp, thread id etc.
            // msg.raw contains pre formatted log

            // If needed (very likely but not mandatory), the sink formats the message before sending it to its final destination:
            spdlog::memory_buf_t formatted;
            spdlog::sinks::base_sink<Mutex>::formatter_->format(msg, formatted);
            _msg_stream << fmt::to_string(formatted);
        }

        void flush_() override {
            _msg_stream << std::flush;
        }
    private:
        std::stringstream _msg_stream; // newline delimited messages
    };

    #include "spdlog/details/null_mutex.h"
    #include <mutex>
    using my_sink_mt = memory_sink<std::mutex>;
    using my_sink_st = memory_sink<spdlog::details::null_mutex>;
}

#endif //TUPLEX_LOGSINK_H
