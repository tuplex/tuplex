//
// Created by Leonhard Spiegelberg on 1/31/22.
//

#include <ee/aws/RequestInfo.h>
#include <AWSCommon.h>
#include <aws/core/utils/HashingUtils.h>

#include <regex>

namespace tuplex {
    RequestInfo RequestInfo::parseFromLog(const std::string& log) {
        RequestInfo info;

        // empty log?
        if(log.empty())
            return info;

        std::stringstream ss;
        // Decode the result header to see requested log information
        auto byteLogResult = Aws::Utils::HashingUtils::Base64Decode(log.c_str());
        for (unsigned i = 0; i < byteLogResult.GetLength(); i++)
            ss << byteLogResult.GetItem(i);
        auto logTail =  ss.str();

        // fetch RequestID, Duration, BilledDuration, MemorySize, MaxMemoryUsed from last line
        auto reportLine = logTail.substr(logTail.rfind("\nREPORT") + strlen("\nREPORT"), logTail.rfind('\n'));

        std::vector<std::string> tabCols;
        splitString(reportLine, '\t', [&](const std::string& s) { tabCols.emplace_back(s); });

        // extract parts
        for(auto col : tabCols) {
            trim(col);

            if (info.requestId.empty() && strStartsWith(col, "RequestId: ")) {
                // extract ID and store it
                info.requestId = col.substr(strlen("RequestId: "));
            }

            if (strStartsWith(col, "Duration: ") && strEndsWith(col, " ms")) {
                // extract ID and store it
                auto r = col.substr(strlen("Duration: "), col.length() - 3 - strlen("Duration: "));
                info.durationInMs = std::stod(r);
            }

            if (strStartsWith(col, "Billed Duration: ") && strEndsWith(col, " ms")) {
                // extract ID and store it
                auto r = col.substr(strlen("Billed Duration: "), col.length() - 3 - strlen("Billed Duration: "));
                info.billedDurationInMs = std::stoi(r);
            }

            if (strStartsWith(col, "Memory Size: ") && strEndsWith(col, " MB")) {
                // extract ID and store it
                auto r = col.substr(strlen("Memory Size: "), col.length() - 3 - strlen("Memory Size: "));
                info.memorySizeInMb = std::stoi(r);
            }

            if (strStartsWith(col, "Max Memory Used: ") && strEndsWith(col, " MB")) {
                // extract ID and store it
                auto r = col.substr(strlen("Max Memory Used: "), col.length() - 3 - strlen("Max Memory Used: "));
                info.maxMemoryUsedInMb = std::stoi(r);
            }

            // error message is formatted using RequestId: .... Error: ...
            // i.e., this here is the regex: RequestId:\s+([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})\s+Error:.*
            if(strStartsWith(col, "RequestId: ") && col.find("Error: ") != std::string::npos) {
                // extract error message
                info.errorMessage = col.substr(col.find("Error: "));

                // per default assign retcode -1
                info.returnCode = -1;

                // find exit status via regex
                // exit status (\d+)
                std::regex re_exit_status("exit status (\\d+)");
                std::smatch base_match;
                if(regex_search(col, base_match, re_exit_status)) {
                    // sub_match is the first parenthesized expression.
                    if (base_match.size() == 2) {
                        std::ssub_match base_sub_match = base_match[1];
                        std::string base = base_sub_match.str();
                        info.returnCode = std::stoi(base);
                    }
                }
            }
        }
        return info;
    }
}