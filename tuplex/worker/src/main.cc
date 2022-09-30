//
// Created by Leonhard Spiegelberg on 11/22/21.
//

#include <main.h>
#include <lyra/lyra.hpp>

#include <Logger.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <ee/aws/ContainerInfo.h>

namespace tuplex {
    ContainerInfo getThisContainerInfo() {
        ContainerInfo info;
        return info;
    }
}

int initLogging(const std::string& logPath) {
    using namespace std;
    using namespace tuplex;
    if(!logPath.empty()) {
        try {
            Logger::instance().init({std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>(), std::make_shared<spdlog::sinks::basic_file_sink_mt>(logPath.c_str())});
        } catch(...) {
            return 1;
        }
    } else {
        Logger::instance().init();
    }
    return 0;
}

int main(int argc, char* argv[]) {
    using namespace std;
    using namespace tuplex;

    std::string logPath;
    bool isDaemon = false; // do not start in daemon mode
    unsigned int port = 9000; // default port
    std::string message; // no message to process
    unsigned int timeout = 10000; // 10s timeout
    bool show_help = false;

    // construct CLI
    auto cli = lyra::cli();
    cli.add_argument(lyra::help(show_help));
    cli.add_argument(lyra::opt(logPath, "logPath").name("--log-path").help("path where to store log file"));
    cli.add_argument(lyra::opt(isDaemon, "daemon").name("-d").name("--daemon").help("start worker as daemon process listening to connections"));
    cli.add_argument(lyra::opt(port, "port").name("-p").name("--port").help("port on which worker should listen for messages"));
    cli.add_argument(lyra::opt(message, "message").name("-m").name("--message").help("single message in JSON for worker to process, auto-shutdown after message or path to file holding a message"));
    cli.add_argument(lyra::opt(timeout, "timeout").name("-t").name("--timeout").help("if set to non-zero, timeout in ms after which worker will auto-shutdown"));

    auto result = cli.parse({argc, argv});
    if(!result) {
        cerr<<"Error parsing command line: "<<result.errorMessage()<<std::endl;
        return 1;
    }

    if(show_help) {
        cout<<cli<<endl;
        return 0;
    }

    // logPath given? init Logger with filesink!
    auto rc = initLogging(logPath);
    if(0 != rc) {
        cerr<<"Failed to initialize logging"<<endl;
        return 1;
    }
    auto&logger = Logger::instance().defaultLogger();

    Logger::instance().defaultLogger().info("Starting Tuplex worker process");

    // init Worker with default settings
    auto app = make_unique<WorkerApp>(WorkerSettings());

    // mode
    if(isDaemon) {
        rc = app->messageLoop();
         app->shutdown();
    } else {
        if(!message.empty()) {
            // check if message is a file (dequote shell command!)
            if(fileExists(message)) {
                logger.debug("Loading message from file " + message);
                message = fileToString(URI(message));
            }

            // process message
            rc = app->processJSONMessage(message);

            logger.debug("message found");
        }
        app->shutdown();
    }

    if(0 == rc)
        Logger::instance().defaultLogger().info("Terminating Tuplex worker process normally with exit code 0.");
    else
        Logger::instance().defaultLogger().error("Terminating Tuplex worker process with exit code " + std::to_string(rc));
    return rc;
}