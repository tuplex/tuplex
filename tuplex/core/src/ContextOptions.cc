//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <ContextOptions.h>
#include <yaml-cpp/yaml.h>
#include <Logger.h>
#include <str_const.h>
#include <sstream>
#include <Utils.h>

#include <boost/dll/runtime_symbol_info.hpp>
#include <StringUtils.h>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <stack>

#include <VirtualFileSystem.h>
#include <Environment.h>
#include <PythonHelpers.h>
#include <Utils.h>
#include <StringUtils.h>
#include <nlohmann/json.hpp>

namespace tuplex {

#warning "safe guard such that users can't submit weird values. I.e. default to meaningful defaults," \
    " if crap is submitted as input"


    std::vector<std::string> decodePythonList(const std::string& s) {
        auto list_contents = s.substr(1, s.length() - 2);

        using boost::tokenizer;
        using boost::escaped_list_separator;
        typedef tokenizer<escaped_list_separator<char>> so_tokenizer;
        so_tokenizer tok(list_contents, escaped_list_separator<char>('\\', ',', '\''));
        std::vector<std::string> res;
        for(so_tokenizer::iterator it = tok.begin(); it != tok.end(); ++it) {
            auto token = *it;
            boost::trim_if(token, boost::is_any_of(" "));
            res.push_back(token);
        }

        return res;
    }

    void toYAMLValue(YAML::Emitter& out, const std::string& s) {
        assert(s.length() > 0);

        // check if starts with [ and ends with ]
        // then it is a list
        if (s.front() == '[' && s.back() == ']') {
            out << YAML::Value << YAML::Flow << YAML::BeginSeq;
            for (auto token : decodePythonList(s))
                out << token;

            out << YAML::EndSeq;
        } else {
            // if in '', strip them
            if (s.front() == '\'' && s.back() == '\'')
                out << s.substr(1, s.length() - 1);
            else
                // else, per default stream it
                out << YAML::Value;
            out << s;
        }
    }


    bool ContextOptions::toYAML(const URI &uri, bool overwrite) {
        YAML::Emitter out;
        out.SetIndent(4);

        out<<YAML::Comment("Tuplex configuration file");

        //std::string date_string = date::format(std::locale(""), "%F %T %Z", std::chrono::system_clock::now());
        auto cur_t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::string date_string = std::ctime(&cur_t);

        out<<YAML::Newline;
        out<<YAML::Comment("\tcreated " + date_string);
        using namespace std;
        vector<string> keys;
        for(auto entry : _store) {
            auto flattened_key = entry.first;
            // split after .
            vector<string> nodes;
            boost::split(nodes, flattened_key, boost::is_any_of("."));
            assert(nodes.size() >= 2);
            auto kvalue = nodes.back();
            nodes.pop_back();

            // special case: empty stack
            if(keys.empty()) {
                // push all onto stack
                for(auto node : nodes) {
                    out<<YAML::BeginMap;
                    out<<YAML::Key<<node;
                    keys.push_back(node);
                    out<<YAML::Value<<YAML::BeginSeq;
                }
            } else {
                // compare up to where the chain matches
                int imatch = -1;
                for(int i = 0; i < std::min(keys.size(), nodes.size()); ++i) {
                    if(keys[i] == nodes[i])
                        imatch++;
                }
                // no match at all? unload full chain!
                if(imatch < 0) {
                    while (!keys.empty()) {
                        out<<YAML::EndSeq;
                        out << YAML::EndMap;
                        keys.pop_back();
                    }
                    for(auto node : nodes) {
                        out<<YAML::BeginMap;
                        out<<YAML::Key<<node;
                        keys.push_back(node);
                        out<<YAML::Value<<YAML::BeginSeq;
                    }
                }
                else {
                    // new map chain
                    for(int i = keys.size() - 1; i > imatch; --i) {
                        out<<YAML::EndSeq;
                        out << YAML::EndMap;
                        keys.pop_back();
                    }

                    // add other chain
                    for(int i = imatch + 1; i < nodes.size(); ++i) {
                        out<<YAML::BeginMap;
                        out<<YAML::Key<<nodes[i];
                        keys.push_back(nodes[i]);
                        out<<YAML::Value<<YAML::BeginSeq;
                    }
                }
            }

            // add value...
            out<<YAML::BeginMap;
            out<<YAML::Key<<kvalue;
            toYAMLValue(out, entry.second);
            out<<YAML::EndMap;

        }
        // pop from stack
        while (!keys.empty()) {
            out<<YAML::EndSeq;
            out<<YAML::EndMap;
            keys.pop_back();
        }

        if(!out.good()) {
            Logger::instance().defaultLogger().error("error while saving setting to YAML: " + out.GetLastError());
            return false;
        }

        auto vfs = VirtualFileSystem::fromURI(uri);
        auto vf = vfs.open_file(uri, VirtualFileMode::VFS_WRITE);
        if(!vf)
            return false;

        auto status = vf->write(reinterpret_cast<const uint8_t*>(out.c_str()), out.size());

        return status == VirtualFileSystemStatus::VFS_OK;
    }

    bool ContextOptions::CSV_PARSER_SELECTION_PUSHDOWN() const {
        return stringToBool(_store.at("tuplex.csv.selectionPushdown"));
    }

    ContextOptions ContextOptions::defaults() {

        // Note: When adding new options, do not forget to update
        // PythonContext::options() as well...

        ContextOptions co;

        // set scratch dir to /tmp/tuplex-scratch-space-<user>
        auto user_name = getUserName();
        if("" == user_name) {
            user_name = "tuplex"; // use as default if user name detection fails.
        }
        auto temp_cache_path = "/tmp/tuplex-cache-" + user_name;
        auto temp_mongodb_path = temp_cache_path + "/mongodb";
#ifdef NDEBUG
        // release options
        co._store = {{"tuplex.useLLVMOptimizer", "true"},
                     {"tuplex.backend", "local"},
                     {"tuplex.runTimeMemory", "128MB"},
                     {"tuplex.runTimeMemoryBlockSize", "4MB"},
                     {"tuplex.partitionSize", "32MB"},
                     {"tuplex.driverMemory", "1GB"},
                     {"tuplex.executorMemory", "1GB"},
                     {"tuplex.executorCount", std::to_string(std::thread::hardware_concurrency())},
                     {"tuplex.autoUpcast", "false"},
                     {"tuplex.allowUndefinedBehavior", "false"},
                     {"tuplex.scratchDir", temp_cache_path},
                     {"tuplex.logDir", "."},
                     {"tuplex.runTimeLibrary", "tuplex_runtime"},
                     {"tuplex.csv.maxDetectionRows", "10000"},
                     {"tuplex.csv.maxDetectionMemory", "256KB"},
                     {"tuplex.csv.separators", "[',', ';', '|', '\t']"},
                     {"tuplex.csv.quotechar", "\""},
                     {"tuplex.csv.comments", "['#', '~']"},
                     {"tuplex.normalcaseThreshold", "0.9"},
                     {"tuplex.optionalThreshold", "0.7"},
                     {"tuplex.csv.selectionPushdown", "true"},
                     {"tuplex.webui.enable", "true"},
                     {"tuplex.webui.port", "5000"},
                     {"tuplex.webui.url", "localhost"},
                     {"tuplex.webui.mongodb.url", "localhost"},
                     {"tuplex.webui.mongodb.port", "27017"},
                     {"tuplex.webui.mongodb.path", temp_mongodb_path},
                     {"tuplex.webui.exceptionDisplayLimit", "5"},
                     {"tuplex.readBufferSize", "128KB"},
                     {"tuplex.inputSplitSize", "64MB"},
                     {"tuplex.optimizer.codeStats", "false"},
                     {"tuplex.optimizer.generateParser", "false"},
                     {"tuplex.optimizer.nullValueOptimization", "false"},
                     {"tuplex.optimizer.filterPushdown", "true"},
                     {"tuplex.optimizer.operatorReordering", "false"},
                     {"tuplex.optimizer.sharedObjectPropagation", "true"},
                     {"tuplex.optimizer.mergeExceptionsInOrder", "true"},
                     {"tuplex.interleaveIO", "true"},
                     {"tuplex.aws.scratchDir", ""},
                     {"tuplex.aws.requestTimeout", "600"},
                     {"tuplex.aws.connectTimeout", "1"},
                     {"tuplex.aws.maxConcurrency", "100"},
                     {"tuplex.aws.httpThreadCount", std::to_string(std::max(8u, std::thread::hardware_concurrency()))},
                     {"tuplex.aws.region", "us-east-1"},
                     {"tuplex.aws.lambdaMemory", "1536"},
                     {"tuplex.aws.lambdaTimeout", "600"},
                     {"tuplex.aws.lambdaThreads", "auto"},
                     {"tuplex.aws.lambdaInvokeOthers", "true"},
                     {"tuplex.aws.lambdaInvocationStrategy", "direct"},
                     {"tuplex.aws.requesterPay", "false"},
                     {"tuplex.useInterpreterOnly", "false"},
                     {"tuplex.resolveWithInterpreterOnly", "false"},
                     {"tuplex.network.caFile", ""},
                     {"tuplex.network.caPath", ""},
                     {"tuplex.network.verifySSL", "false"},  // if default is going to be changed to true, ship cacert.pem from Amazon to avoid issues.
                     {"tuplex.redirectToPythonLogging", "false"}};
#else
        // DEBUG options
        co._store = {{"tuplex.useLLVMOptimizer", "false"},
                     {"tuplex.backend", "local"},
                     {"tuplex.runTimeMemory", "32MB"},
                     {"tuplex.runTimeMemoryBlockSize", "4MB"},
                     {"tuplex.partitionSize", "1MB"},
                     {"tuplex.driverMemory", "128MB"},
                     {"tuplex.executorMemory", "128MB"},
                     {"tuplex.executorCount", std::to_string(std::min(3u, std::thread::hardware_concurrency()))},
                     {"tuplex.autoUpcast", "false"},
                     {"tuplex.allowUndefinedBehavior", "false"},
                     {"tuplex.scratchDir", temp_cache_path},
                     {"tuplex.logDir", "."},
                     {"tuplex.runTimeLibrary", "tuplex_runtime"},
                     {"tuplex.csv.maxDetectionRows", "10000"},
                     {"tuplex.csv.maxDetectionMemory", "256KB"},
                     {"tuplex.csv.separators", "[',', ';', '|', '\t']"},
                     {"tuplex.csv.quotechar", "\""},
                     {"tuplex.csv.comments", "['#', '~']"},
                     {"tuplex.normalcaseThreshold", "0.9"},
                     {"tuplex.optionalThreshold", "0.7"},
                     {"tuplex.csv.selectionPushdown", "true"}, //
                     {"tuplex.webui.enable", "true"},
                     {"tuplex.webui.port", "5000"},
                     {"tuplex.webui.url", "localhost"},
                     {"tuplex.webui.mongodb.url", "localhost"},
                     {"tuplex.webui.mongodb.port", "27017"},
                     {"tuplex.webui.mongodb.path", temp_mongodb_path},
                     {"tuplex.webui.exceptionDisplayLimit", "5"},
                     {"tuplex.readBufferSize", "4KB"},
                     {"tuplex.inputSplitSize", "16MB"},
                     {"tuplex.optimizer.codeStats", "true"},
                     {"tuplex.optimizer.generateParser", "false"},
                     {"tuplex.optimizer.nullValueOptimization", "false"},
                     {"tuplex.optimizer.filterPushdown", "true"},
                     {"tuplex.optimizer.operatorReordering", "false"},
                     {"tuplex.optimizer.sharedObjectPropagation", "true"},
                     {"tuplex.optimizer.mergeExceptionsInOrder", "false"},
                     {"tuplex.interleaveIO", "true"},
                     {"tuplex.aws.scratchDir", ""},
                     {"tuplex.aws.requestTimeout", "600"},
                     {"tuplex.aws.connectTimeout", "1"},
                     {"tuplex.aws.maxConcurrency", "100"},
                     {"tuplex.aws.httpThreadCount", std::to_string(std::min(8u, std::thread::hardware_concurrency()))},
                     {"tuplex.aws.region", "us-east-1"},
                     {"tuplex.aws.lambdaMemory", "1536"},
                     {"tuplex.aws.lambdaTimeout", "600"},
                     {"tuplex.aws.lambdaThreads", "auto"},
                     {"tuplex.aws.lambdaInvokeOthers", "true"},
                     {"tuplex.aws.lambdaInvocationStrategy", "direct"},
                     {"tuplex.aws.requesterPay", "false"},
                     {"tuplex.useInterpreterOnly", "false"},
                     {"tuplex.resolveWithInterpreterOnly", "true"},
                     {"tuplex.network.caFile", ""},
                     {"tuplex.network.caPath", ""},
                     {"tuplex.network.verifySSL", "false"},
                     {"tuplex.redirectToPythonLogging", "false"}}; // experimental feature, deactivate for now.
#endif

        // update with tuplex env
        auto env = getTuplexEnvironment();
        co._store.insert(env.begin(), env.end());

        return co;
    }

    std::string ContextOptions::NETWORK_CA_FILE() const { return _store.at("tuplex.network.caFile"); }
    std::string ContextOptions::NETWORK_CA_PATH() const { return _store.at("tuplex.network.caPath"); }
    bool ContextOptions::NETWORK_VERIFY_SSL() const { return stringToBool(_store.at("tuplex.network.verifySSL")); }
    bool ContextOptions::USE_WEBUI() const { return stringToBool(_store.at("tuplex.webui.enable")); }
    std::string ContextOptions::WEBUI_DATABASE_HOST() const { return _store.at("tuplex.webui.mongodb.url"); }
    uint16_t ContextOptions::WEBUI_DATABASE_PORT() const { return std::stoi(_store.at("tuplex.webui.mongodb.port")); }
    std::string ContextOptions::WEBUI_HOST() const { return _store.at("tuplex.webui.url"); }
    uint16_t ContextOptions::WEBUI_PORT() const { return std::stoi(_store.at("tuplex.webui.port")); }

    std::vector<std::pair<std::string, std::string>> decodeYAML(YAML::Node node) {
        using namespace std;

        vector<pair<string, string>> res;
        if(node.IsMap()) {
            // check what kind it is
            for(auto p : node) {
                auto key = p.first.as<std::string>();
                auto val = p.second;
                if(val.IsScalar()) {
                    res.push_back(make_pair(key, val.as<std::string>()));
                } else {
                    // add key...
                    auto decoded_el = decodeYAML(val);
                    for(auto pp : decoded_el) {
                        res.push_back(make_pair(pp.first.length() > 0
                                                ? key + "." + pp.first
                                                : key, pp.second));
                    }
                }
            }
            return res;
        } else if(node.IsSequence()) {

            // two types of sequences allowed:
            // (1) containing maps
            // (2) containing scalars
            if(0 == node.size())
                return {make_pair("", "[]")};

            auto type = node[0].Type();
            assert(type == YAML::NodeType::Scalar || type == YAML::NodeType::Map);
            bool valueList = node[0].IsScalar();
            if(valueList) {
                stringstream ss;
                ss<<"[";
                for(auto val : node) {
                    ss<<"'"<<val.as<std::string>()<<"',";
                }
                auto valstr = ss.str();
                valstr.back() = ']';
                return {make_pair("", valstr)};
            } else
                for(auto el : node) {
                    auto decoded_el = decodeYAML(el);
                    res.insert(res.end(), decoded_el.begin(), decoded_el.end());
                }
            return res;
        } else {
            // unsupported, i.e. do not add.
            return {};
        }
    }

    ContextOptions ContextOptions::fromYAML(const URI &uri) {
        try {
            // read whole file in memory & convert to string
            auto vfs = VirtualFileSystem::fromURI(uri);
            auto vf = vfs.open_file(uri, VirtualFileMode::VFS_READ);
            if(!vf)
                return defaults();

            char* buffer = new char[vf->size()];
            assert(buffer);
            size_t bytesRead = 0;
            vf->read(reinterpret_cast<uint8_t*>(buffer), vf->size(), &bytesRead);
            assert(bytesRead == vf->size());
            vf->close();

            auto content = std::string(buffer);
            delete [] buffer;
            buffer = nullptr;

            // first acquire defaults
            ContextOptions co = defaults();

            // parse YAML file
            YAML::Node config = YAML::Load(content);
            assert(config.Type() == YAML::NodeType::Map);
            auto res = decodeYAML(config);

            if(res.empty()) {
                Logger::instance().defaultLogger().warn("loaded empty configuration file from " + uri.toString());
            }

            for(auto kv : res) {
                if(co._store.find(kv.first) == co._store.end())
                    Logger::instance().defaultLogger().warn("parsing tuplex option that is not present in default settings.");
                co._store[kv.first] = kv.second;
            }

            return co;
        } catch(YAML::ParserException& e) {
            Logger::instance().defaultLogger().error("error while parsing configuration file "
                                                     + uri.toPath() + "\n" + e.what());
            return defaults();
        }
    }


    void ContextOptions::updateWith(const ContextOptions &other) {
        // override keys for which values differ from options
        for(auto keyval : other._store) {
            // simply add key + val
            _store[keyval.first] = keyval.second;
        }
    }

    ContextOptions ContextOptions::load(const std::string &filename) {
        ContextOptions options = defaults();


#warning "fix this to add settings better to Tuplex"

        // check whether TUPLEX_HOME is set
        char* tuplex_home = std::getenv("TUPLEX_HOME");
        if(tuplex_home) {
            auto uri = URI(std::string(tuplex_home) + "/" + filename);
            if(uri.exists()) {
                ContextOptions home_options = ContextOptions::fromYAML(uri);
                options.updateWith(home_options);
                Logger::instance().defaultLogger().info("updated TUPLEX settings via options through " + uri.toPath());
            }
            return options;
        }

        // check whether local file exists
        auto uri = URI("file://./" + filename);
        if(uri.exists()) {
            ContextOptions local_options = ContextOptions::fromYAML(uri);
            options.updateWith(local_options);
            Logger::instance().defaultLogger().info("updated TUPLEX settings via options through " + uri.toPath());
            return options;
        }

        return options;
    }

    size_t ContextOptions::PARTITION_SIZE() const {
        return memStringToSize(_store.at("tuplex.partitionSize"));
    }

    size_t ContextOptions::EXECUTOR_MEMORY() const {
        return memStringToSize(_store.at("tuplex.executorMemory"));
    }

    unsigned int ContextOptions::EXECUTOR_COUNT() const {
        unsigned int executorCount = std::stoi(_store.at("tuplex.executorCount"));
        return executorCount;
    }

    size_t ContextOptions::RUNTIME_MEMORY() const {
        return memStringToSize(_store.at("tuplex.runTimeMemory"));
    }

    size_t ContextOptions::RUNTIME_MEMORY_DEFAULT_BLOCK_SIZE() const {
        return memStringToSize(_store.at("tuplex.runTimeMemoryBlockSize"));
    }

    // @ Todo: add this to CSV parser at later stage (FAS-109)
    bool ContextOptions::AUTO_UPCAST_NUMBERS() const {
        return stringToBool(_store.at("tuplex.autoUpcast"));
    }


    // @ Todo: Requires major redesign as daemon process for multi user, ...
    // right now, whole framework is a library and not yet a distributed system/framework
    URI ContextOptions::LOG_DIR() const {
        Logger::instance().defaultLogger().error("unused setting, requires major overhaul of how things are logged.");
        return URI(_store.at("tuplex.logDir"));
    }

    // @ Todo: required for FAS-27
    URI ContextOptions::SCRATCH_DIR() const {
        return URI(_store.at("tuplex.scratchDir"));
    }

    bool ContextOptions::UNDEFINED_BEHAVIOR_FOR_OPERATORS() const {
        return stringToBool(_store.at("tuplex.allowUndefinedBehavior"));
    }

    bool ContextOptions::USE_LLVM_OPTIMIZER() const {
        return stringToBool(_store.at("tuplex.useLLVMOptimizer"));
    }

    double ContextOptions::NORMALCASE_THRESHOLD() const {
        auto val = std::stod(_store.at("tuplex.normalcaseThreshold"));
        assert(0.0 < val <= 1.0);

        // warn if less than 0.5. Why?
        if(val < 0.5)
            Logger::instance().defaultLogger().warn("normal case threshold is < 0.5. Why? This makes little sense...");

        return val;
    }

    double ContextOptions::OPTIONAL_THRESHOLD() const {
        auto val = std::stod(_store.at("tuplex.optionalThreshold"));
        assert(0.0 < val <= 1.0);

        // warn if less than 0.5. Why?
        if(val < 0.5)
            Logger::instance().defaultLogger().warn("optional threshold is < 0.5. Why? This will never allow options");

        return val;
    }

    size_t ContextOptions::WEBUI_EXCEPTION_DISPLAY_LIMIT() const {
        return std::stoi(_store.at("tuplex.webui.exceptionDisplayLimit"));
    }

    URI ContextOptions::RUNTIME_LIBRARY(bool checkWithPythonExtension) const {
        // advanced check:
        // first check if path is given
        auto dylib = _store.at("tuplex.runTimeLibrary");
        auto path = URI(dylib);

        // if URI exists, directly return it
        if(path.exists() && path.isFile())
            return path;

        // else, start searching!

        // extensions to search for
        std::vector<std::string> extensions = {".dylib", ".so"};

        // if python is initialized, also add platform specific distutils suffixes
        if(checkWithPythonExtension) {
            using namespace std;
            python::lockGIL();
            extensions.push_back(python::platformExtensionSuffix());
            python::unlockGIL();
        }

        // search strategy is as follows:
        // first, get basename
        auto pathParent = dylib.substr(0, dylib.rfind('/'));
        if(dylib.rfind('/') == std::string::npos)
            pathParent = "";
        auto pathBaseName = dylib.substr(dylib.rfind('/') + 1);

        std::vector<std::string> candidates;

        // add to candidates
        for(const auto& ext : extensions) {
            candidates.emplace_back(pathBaseName + ext);

            // strip possible extension from base
            auto baseWithOutExt = pathBaseName.substr(0, pathBaseName.rfind('.'));
            candidates.emplace_back(baseWithOutExt + ext);
        }

        // remove duplicates
        core::removeDuplicates(candidates);

        // check first with pathParent, then PATH
        std::vector<std::string> failedPaths;
        for(auto c : candidates) {
            URI p = URI(pathParent.empty() ? c : pathParent + "/" + c);
            if(p.exists() && p.isFile())
                return p;
            else
                failedPaths.emplace_back(p.toString());
        }

        // search different path locations
        // expand from PATH
        // and LD_LIBRARY_PATH
        // also search locally in workdir and exec dir
        // further, path of executable
        auto execPath = boost::dll::program_location();

        auto envHOME = getEnv("HOME");

        std::vector<URI> searchPaths = {URI("file://."),
                                        URI("file://" + envHOME + "/lib"),
                                        URI("file:///usr/local/lib"),
                                        URI("file:///lib"),
                                        URI("file:///usr/lib"),
                                        URI("file://" + execPath.parent_path().string())};

        // very generous searching, even wrong extension is tolerated...
        for(auto c : candidates) {
            auto uri = URI::searchPaths(c, searchPaths);
            if(uri != URI::INVALID)
                return uri;
            else {
                for(auto spuri : searchPaths)
                    failedPaths.emplace_back(spuri.toString() + "/" + c);
            }
        }

        std::stringstream ss;
        ss<<"Searched following paths:\n";
        core::removeDuplicates(failedPaths);
        std::sort(failedPaths.begin(), failedPaths.end());
        for(auto fp : failedPaths) {
            ss<<fp<<std::endl;
        }
        Logger::instance().defaultLogger()
                .error("tuplex.runTimeLibrary='" + path.toPath() + "' could not be found.\n" + ss.str());

        return URI::INVALID;
    }

    std::vector<char> ContextOptions::CSV_COMMENTS() const {
        auto val = _store.at("tuplex.csv.comments");

        auto list = decodePythonList(val);
        std::vector<char> res;
        for(auto el : list) {
            assert(el.length() == 1);
            res.push_back(el[0]);
        }
        assert(res.size() > 0);
        return res;
    }

    size_t ContextOptions::CSV_MAX_DETECTION_MEMORY() const {
        return memStringToSize(_store.at("tuplex.csv.maxDetectionMemory"));
    }

    size_t ContextOptions::INPUT_SPLIT_SIZE() const {
        return memStringToSize(_store.at("tuplex.inputSplitSize"));
    }

    size_t ContextOptions::READ_BUFFER_SIZE() const {
        return memStringToSize(_store.at("tuplex.readBufferSize"));
    }

    size_t ContextOptions::CSV_MAX_DETECTION_ROWS() const {
        auto val = _store.at("tuplex.csv.maxDetectionRows");
        return std::stoull(val);
    }

    char ContextOptions::CSV_QUOTECHAR() const {
        auto val = _store.at("tuplex.csv.quotechar");
        assert(val.length() == 1);
        return val[0];
    }

    std::vector<char> ContextOptions::CSV_SEPARATORS() const {
        auto val = _store.at("tuplex.csv.separators");

        auto list = decodePythonList(val);
        std::vector<char> res;
        for(auto el : list) {
            assert(el.length() == 1);
            res.push_back(el[0]);
        }
        assert(res.size() > 0);
        return res;
    }

    void ContextOptions::set(const std::string &key, const std::string &value) {
        // check that key exists, else issue warning!
        auto it = _store.find(key);

        if(it == _store.end())
            Logger::instance().defaultLogger().error("could not find key '" + key + "'");
        else
            _store[key] = value;
    }

    size_t ContextOptions::DRIVER_MEMORY() const {
        return memStringToSize(_store.at("tuplex.driverMemory"));
    }

    std::string ContextOptions::toString() const {
        std::stringstream ss;
        for(const auto& opt : _store) {
            ss<<opt.first<<" : "<<opt.second<<std::endl;
        }
        return ss.str();
    }

    bool ContextOptions::containsKey(const std::string &key) const {
        return _store.find(key) != _store.end();
    }

    std::string ContextOptions::get(const std::string &key, const std::string &alt) const {
        auto it = _store.find(key);
        if(it == _store.end())
            return alt;
        return it->second;
    }


    Backend ContextOptions::BACKEND() const {
        auto b = get("tuplex.backend", "");
        if(0 == b.length()) {
            Logger::instance().defaultLogger().warn("no backend specified explicitly, defaulting to local");
            return Backend::LOCAL;
        }

        if(b == "local") {
            return Backend::LOCAL;
        } else if(b == "lambda") {
            return Backend::LAMBDA;
        } else {
            Logger::instance().defaultLogger().error("found unknown backend '" + b + "', defaulting to local execution.");
            return Backend::LOCAL;
        }
    }

    std::string ContextOptions::asJSON() const {
        nlohmann::json json;

        for(const auto& keyval : _store) {
            // convert to correct type (match basically)
            if(keyval.second.empty())
                json[keyval.first] = keyval.second;
            else if(isBoolString(keyval.second))
                json[keyval.first] = parseBoolString(keyval.second);
            else if(isIntegerString(keyval.second.c_str()))
                json[keyval.first] = std::stoi(keyval.second);
            else if(isFloatString(keyval.second.c_str()))
                json[keyval.first] = std::stod(keyval.second);
            else
                json[keyval.first] = keyval.second;
        }
        return json.dump();
    }

    NetworkSettings ContextOptions::AWS_NETWORK_SETTINGS() const {
        NetworkSettings ns;
        ns.verifySSL = this->NETWORK_VERIFY_SSL();
        ns.caFile = this->NETWORK_CA_FILE();
        ns.caPath = this->NETWORK_CA_PATH();
        return ns;
    }
}