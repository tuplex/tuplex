//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Utils.h>
#include "Logger.h"
#include <map>
#include <string>
#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <regex>
#include <Row.h>
#include <random>

namespace tuplex {


    static std::mutex uuidGeneratorMutex; // boost uuids are not threadsafe
    static boost::uuids::random_generator uuidGenerator;

    uniqueid_t getUniqueID() {
        std::lock_guard<std::mutex> lock(uuidGeneratorMutex);
        auto uuid = uuidGenerator();
        return uuid;
    }


    std::string getUserName() {
        // UNIX specific function for username
        // check env variables 'LOGNAME', 'USER', 'LNAME', 'USERNAME'
        using namespace std;

        std::vector<std::string> vars = {"LOGNAME", "USER", "LNAME", "USERNAME"};

        for(auto var : vars) {
            auto name = getenv(var.c_str());
            if(name)
                return std::string(name);
        }

        // no user found above, return ""
        // more advanced methods possible...
        return "";
    }

    size_t memStringToSize(const std::string& str) {
        using namespace boost::algorithm;

        if(0 == str.length()) {
            Logger::instance()
                    .logger("memory")
                    .warn("empty memory config string given, defaulting to 0");
            return 0;
        }

        // use format similar to sparks, however also fractions and multiple combinations can be specified
        std::map<std::string, size_t> lookup = {{"b", 1LL},
                                                {"k", 1024LL},
                                                {"kb", 1024LL},
                                                {"m", 1024 * 1024LL},
                                                {"mb", 1024 * 1024LL},
                                                {"g", 1024 * 1024 * 1024LL},
                                                {"gb", 1024 * 1024 * 1024LL},
                                                {"t", 1024 * 1024 * 1024 * 1024LL},
                                                {"tb", 1024 * 1024 * 1024 * 1024LL}};

        // convert string to lower case
        std::string s = to_lower_copy(str);

        // split on numbers
        std::vector<std::string> parts;
        std::vector<bool> types;
        int last = 0;
        bool isLastNumber = isDigit(s[0]) || (s[0] == '.');
        for(int i = 1; i < s.length(); i++) {
            bool number = isDigit(s[i]) || (s[i] == '.');
            // if it changes, split string!
            if(number != isLastNumber) {
                parts.push_back(s.substr(last, i - last));
                types.push_back(isLastNumber);
                isLastNumber = number;
                last = i;
            }
        }
        parts.push_back(s.substr(last));
        types.push_back(isLastNumber);

        assert(parts.size() == types.size());
        assert(parts.size() > 0 && types.size() > 0);

        size_t sum = 0;


        // special case: only one symbol
        if(parts.size() == 1) {
            // first element - a symbol? i.e. m?
            if(!types[0]) {
                if(lookup.find(parts[0]) == lookup.end()) {
                    Logger::instance().logger("memory").error("Unknown memory suffix '" + parts[0] +"' encountered");
                    return 0;
                }
                sum += lookup[parts[0]];
            } else {
                // double or int?
                if(parts[0].find(".") != std::string::npos) {
                    // double!
                    double fraction = std::stod(parts[0]);
                    sum += (int64_t)fraction;
                } else {
                    // int
                    int64_t coeff = std::stoll(parts[0]);
                    sum += coeff;
                }
            }
        }

        // first element - a symbol? i.e. m?
        if(!types[0]) {
            if(lookup.find(parts[0]) == lookup.end()) {
                Logger::instance().logger("memory").error("Unknown memory suffix '" + parts[0] +"' encountered");
                return 0;
            }
            sum += lookup[parts[0]];
        }

        for(int i = 1; i < parts.size(); ++i) {
            // check what the type of the last entry is. If number, then add with lookup
            // if two suffices follow, issue error
            if(types[i - 1] && !types[i]){
                if(lookup.find(parts[i]) == lookup.end()) {
                    Logger::instance().logger("memory").error("Unknown memory suffix '" + parts[i] +"' encountered");
                    return 0;
                }
                int64_t factor = lookup[parts[i]];

                // double or int?
                if(parts[i - 1].find(".") != std::string::npos) {
                    // double!
                    double fraction = std::stod(parts[i - 1]);
                    sum += factor * fraction;
                } else {
                    // int
                    int64_t coeff = std::stoll(parts[i - 1]);
                    sum += factor * coeff;
                }

                i++;
            } else {
                Logger::instance().logger("memory").error("malformed memory string '" + str +"' encountered");
                return 0;
            }

        }

        return sum;
    }


    std::string sizeToMemString(size_t size) {
        using namespace std;
        static const char *SIZES[] = { "B", "KB", "MB", "GB", "TB", "PB"};

        int div = 0;
        size_t rem = 0;

        while (size >= 1024 && div < (sizeof(SIZES) / sizeof(*SIZES))) {
            rem = (size % 1024);
            div++;
            size /= 1024;
        }

        double size_d = (float)size + (float)rem / 1024.0;
        stringstream ss(stringstream::in | stringstream::out);
        ss<<setprecision(2)<<fixed<<size_d<<" "<<SIZES[div];
        return ss.str();
    }

    bool stringToBool(const std::string& s) {
        try {
            return parseBoolString(s);
        } catch(...) {
            Logger::instance().defaultLogger().error("could not convert " + s + " to boolean value. Returning false.");
            return false;
        }
    }

    std::string current_working_directory() {
        // from https://stackoverflow.com/questions/2203159/is-there-a-c-equivalent-to-getcwd
        char temp[PATH_MAX];

        if (getcwd(temp, PATH_MAX) != 0)
            return std::string ( temp );

        int error = errno;
        switch ( error ) {
            // EINVAL can't happen - size argument > 0
            // PATH_MAX includes the terminating nul,
            // so ERANGE should not be returned
            case EACCES:
                throw std::runtime_error("Access denied");

            case ENOMEM:
                // I'm not sure whether this can happen or not
                throw std::runtime_error("Insufficient storage");

            default: {
                std::ostringstream str;
                str << "Unrecognised error" << error;
                throw std::runtime_error(str.str());
            }
        }
    }

    std::string create_temporary_directory(const std::string& base_path, size_t max_tries) {
        unsigned long long i = 0;
        std::random_device dev;
        std::mt19937 prng(dev());
        std::uniform_int_distribution<uint64_t> rand(0);
        std::string path;
        auto tmp_dir = !base_path.empty() ? base_path + "/" : "";
        while (true) {
            std::stringstream ss;
            ss << std::hex << rand(prng);
            path = tmp_dir + ss.str();

            // true if the directory was created.
            if (0 == mkdir(path.c_str(), S_IRWXU)) {
                break;
            }
            if (i == max_tries) {
               return "";
            }
            i++;
        }
        return path;
    }

    namespace helper {
        void printSeparatingLine(std::ostream& os, const std::vector<int>& columnWidths) {
            for (auto columnWidth: columnWidths) {
                os << "+-";
                for (int j = 0; j < columnWidth; ++j) {
                    os << "-";
                }
                os << "-";
            }
            os << "+" << std::endl;
        }

        void printRow(std::ostream& os, const std::vector<int>& columnWidths,
                      const std::vector<std::string>& columnStrs) {

            // @ Todo: what about line breaks?

            for (int i = 0; i< std::min(columnWidths.size(), columnStrs.size()); ++i) {
                auto el = replaceLineBreaks(columnStrs[i]);
                os << "| " << el;
                for (int j = 0; j < columnWidths[i] - el.length(); ++j) {
                    os << " ";
                }
                os << " ";
            }

            // for any missing fields
            for (int i = std::min(columnWidths.size(), columnStrs.size()); i < std::max(columnWidths.size(), columnStrs.size()); ++i) {
                os << "| ";
                for (int j = 0; j < columnWidths[i]; ++j) {
                    os << " ";
                }
                os << " ";
            }

            os << "|" << std::endl;
        }
    }
}
