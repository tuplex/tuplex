//
// Created by Leonhard Spiegelberg on 2019-06-23.
//

#include "main.h"

#include <Logger.h>
#include <VirtualFileSystem.h>
#include <Timer.h>
#include <aws/core/Aws.h>
#include <JITCompiler.h>
#include <StringUtils.h>
#include <RuntimeInterface.h>

// protobuf
#include <Lambda.pb.h>
#include <physical/TransformStage.h>
#include <physical/CSVReader.h>
#include <physical/TextReader.h>
#include <google/protobuf/util/json_util.h>

// maybe add FILE // LINE to exception


// the special callbacks (used for JITCompiler)
static_assert(sizeof(int64_t) == sizeof(size_t), "size_t must be 64bit");
int64_t writeCSVRow(void* userData, uint8_t* buffer, size_t size) {

    using namespace std;

    auto line = tuplex::fromCharPointers((char*)buffer, (char*)(buffer+size));

    // simply std::cout
    cout<<line<<endl;

    // return all ok
    return 0;
}

Aws::SDKOptions g_aws_options;
double g_aws_init_time = 0.0;
// note: because compiler uses logger, this here should be pointer for lazy init
std::shared_ptr<tuplex::JITCompiler> g_compiler;

extern "C" {
void global_init() {
    using namespace tuplex;
#ifndef NDEBUG
    std::cout<<"global init..."<<std::endl;
#endif

    // init logger to only act with stdout sink
    Logger::init({std::make_shared<spdlog::sinks::ansicolor_stdout_sink_mt>()});
    Logger::instance().defaultLogger().info("global_init(): logging system initialized");

    // init aws sdk
    Timer timer;
    Aws::InitAPI(g_aws_options);
    std::string caFile = "/etc/pki/tls/certs/ca-bundle.crt";
    VirtualFileSystem::addS3FileSystem("", "", caFile, true);
    g_aws_init_time = timer.time();

    // Note that runtime must be initialized BEFORE compiler due to linking
    runtime::init("lib/tuplex_runtime.so"); // don't change this path, this is how the lambda is packaged...

    g_compiler = std::make_shared<JITCompiler>();

    python::initInterpreter();
}
}

void global_cleanup() {
    using namespace tuplex;

#ifndef NDEBUG
    std::cout<<"global cleanup..."<<std::endl;
#endif

    runtime::freeRunTimeMemory();
    Aws::ShutdownAPI(g_aws_options);
}

struct LambdaExecutor {

    size_t numOutputRows;
    size_t numExceptionRows;
    size_t bytesWritten;
    size_t capacity;
    uint8_t* buffer;

    LambdaExecutor(size_t max_capacity) : capacity(max_capacity), numOutputRows(0), numExceptionRows(0), bytesWritten(0) {
        buffer = new uint8_t[max_capacity];
    }

    void reset() {
        numOutputRows = 0;
        numExceptionRows = 0;
        bytesWritten = 0;
    }

    ~LambdaExecutor() {
        delete [] buffer;
    }

};

int64_t writeRowCallback(LambdaExecutor* exec, const uint8_t* buf, int64_t bufSize) {

    // write row in whatever format...
    // -> simply write to LambdaExecutor
    if(exec->bytesWritten + bufSize < exec->capacity) {
        memcpy(exec->buffer + exec->bytesWritten, buf, bufSize);
        exec->bytesWritten += bufSize;
        exec->numOutputRows++;
    }
    return 0;
}

void writeHashCallback(void* user, const uint8_t* key, int64_t key_size, const uint8_t* bucket, int64_t bucket_size) {

}

void exceptRowCallback(LambdaExecutor* exec, int64_t exceptionCode, int64_t exceptionOperatorID, int64_t rowNumber, uint8_t* input, int64_t dataLength) {
    exec->numExceptionRows++;
}

// how much memory to use for the Lambda??
// TODO: make this dependent on the Lambda configuration!
// --> check env variables from here https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html#configuration-envvars-runtime
static const size_t MAX_RESULT_BUFFER_SIZE = 200 * 1024 * 1024; // 200MB result buffer?
LambdaExecutor *g_executor = nullptr; // global buffer to hold results!

// whether LambdaExecutor has been set up for this invocation
static bool lambda_executor_setup = false;
void reset_executor_setup() { lambda_executor_setup = false; }

void fillInGlobals(tuplex::messages::InvocationResponse* m) {
    using namespace std;

    if(!m)
        return;

    // fill in globals
    if(lambda_executor_setup) {
        m->set_numrowswritten(g_executor->numOutputRows);
        m->set_numexceptions(g_executor->numExceptionRows);
        m->set_numbyteswritten(g_executor->bytesWritten);
    } else { // didn't set up executor yet for this invocation
        m->set_numrowswritten(0);
        m->set_numexceptions(0);
        m->set_numbyteswritten(0);
    }

    m->set_containerreused(false);
//    m->set_containerid(tuplex::uuidToString(container_id()));
    m->set_awsinittime(g_aws_init_time);

    // fill in s3 stats
    auto stats = tuplex::VirtualFileSystem::s3TransferStats();
    for(const auto& keyval : stats) {
        (*m->mutable_s3stats())[keyval.first] = keyval.second;
    }
    tuplex::VirtualFileSystem::s3ResetCounters();
}
tuplex::messages::InvocationResponse make_exception(const std::string& message) {
    using namespace std;
    tuplex::messages::InvocationResponse m;

    m.set_status(tuplex::messages::InvocationResponse_Status_ERROR);
    m.set_errormessage(message);

    fillInGlobals(&m);

    return m;
}

class BreakdownTimings {
private:
    int counter;
    int group_counter[2];
    const char* group_prefix[2] = {"process_mem_", "process_file_"};

    tuplex::Timer timer;

    static std::string counter_prefix(int c) {
        return ((c < 10) ? "0" : "") + std::to_string(c) + "_";
    }

    inline void mark_time_and_reset(double time, const std::string &key) {
        timings[key] = time;
        timer.reset();
    }

    inline void mark_time_group(const std::string &s, int group_num) {
        auto time = timer.time();

        if(group_counter[group_num] == -1) {
            group_counter[group_num] = counter;
            counter++;
        }
        std::string key = counter_prefix(group_counter[group_num]) + group_prefix[group_num] + s;

        mark_time_and_reset(time, key);
    }

public:
    std::map<std::string, double> timings;

    void reset() { timer.reset(); }

    void mark_mem_time(const tuplex::URI &uri) {
        mark_time_group(uri.toString(), 0);
    }

    void mark_file_time(const tuplex::URI &uri) {
        mark_time_group(uri.toString(), 1);
    }

    void mark_time(const std::string &s) {
        auto time = timer.time();

        std::string key = counter_prefix(counter) + s;
        counter++;

        mark_time_and_reset(time, key);
    }

    BreakdownTimings(): counter(0), group_counter{-1, -1} {}
};

//! main function for lambda after all the error handling. This here is where work gets done
tuplex::messages::InvocationResponse lambda_main(const char* payload) {
    using namespace tuplex;
    using namespace std;

    // reset s3 stats
    VirtualFileSystem::s3ResetCounters();

    Timer task_execution_timer;
    BreakdownTimings timer;
    timer.reset();

    // parse protobuf request
    tuplex::messages::InvocationRequest req;
    auto rc = google::protobuf::util::JsonStringToMessage(payload, &req);
    if(rc != google::protobuf::util::Status::OK)
        throw std::runtime_error("could not parse json into protobuf message, bad parse for request");

    timer.mark_time("decode_request");

    // fetch TransformStage
    auto tstage = TransformStage::from_protobuf(req.stage());

    timer.mark_time("decode_transform_stage");

    // decode inputURIs, sizes + output URI
    URI outputURI = req.outputuri();
    if(outputURI.prefix() != "s3://")
        return make_exception("InvalidPath: output path must be s3:// path, is " + outputURI.toPath());

    vector<URI> inputURIs;
    vector<size_t> inputSizes;
    for(const auto& path : req.inputuris()) {
        // check paths are S3 paths
        inputURIs.emplace_back(path);
        if(inputURIs.back().prefix() != "s3://")
            return make_exception("InvalidPath: input path must be s3:// path, is " + inputURIs.back().toPath());
    }

    for(const auto& s : req.inputsizes())
        inputSizes.push_back(s);
    if(inputURIs.size() != inputSizes.size())
        throw std::runtime_error("encoding error, input uris should be same number of elements as input sizes");

    auto num_input_files = inputURIs.size();

    // register functions
    // Note: only normal case for now, the other stuff is not interesting yet...
    if(!tstage->writeMemoryCallbackName().empty())
        g_compiler->registerSymbol(tstage->writeMemoryCallbackName(), writeRowCallback);
    if(!tstage->exceptionCallbackName().empty())
        g_compiler->registerSymbol(tstage->exceptionCallbackName(), exceptRowCallback);
    if(!tstage->writeFileCallbackName().empty())
        g_compiler->registerSymbol(tstage->writeFileCallbackName(), writeRowCallback);
    if(!tstage->writeHashCallbackName().empty())
        g_compiler->registerSymbol(tstage->writeHashCallbackName(), writeHashCallback);

    timer.mark_time("gather_uris_and_register_symbols");

    auto& logger = Logger::instance().defaultLogger();
    logger.info("compiling llvm bitcode (" + sizeToMemString(tstage->bitCode().size()) + ")");
    // test code
    llvm::LLVMContext ctx;
    auto mod = codegen::bitCodeToModule(ctx, tstage->bitCode());
    if(!mod)
        logger.error("error parsing module");
    else {
        logger.info("parsed llvm module from bitcode, " + mod->getName().str());

        // run verify pass on module and print out any errors, before attempting to compile it
        std::string moduleErrors;
        llvm::raw_string_ostream os(moduleErrors);
        if (verifyModule(*mod, &os)) {
            os.flush();
            logger.error("could not verify module from bitcode");
            logger.error(moduleErrors);
            logger.error(core::withLineNumbers(codegen::moduleToString(*mod)));
        } else
        logger.info("module verified.");

    }
    timer.mark_time("parse_and_verify_module");

    // compile function + init stage
    auto syms = tstage->compile(*g_compiler, nullptr, true, false);
    timer.mark_time("compile_function");

    logger.info("compiled symbols");
    {
        std::stringstream ss;
        ss<<"init stage functor"<<std::hex<<syms->initStageFunctor<<endl;
        ss<<"release stage functor"<<std::hex<<syms->releaseStageFunctor<<endl;
        ss<<"functor"<<std::hex<<syms->functor<<endl;
        ss<<"resolve functor"<<std::hex<<syms->resolveFunctor<<endl;
        logger.info("symbols:\n" + ss.str());
    }

    task_execution_timer.reset();

    // init stage
    if(syms->initStageFunctor && syms->initStageFunctor(tstage->initData().numArgs,
                              reinterpret_cast<void**>(tstage->initData().hash_maps),
                              reinterpret_cast<void**>(tstage->initData().null_buckets)) != 0)
        throw std::runtime_error("initStage() failed for stage " + std::to_string(tstage->number()));

    // process data as given in invocation request
    if(tstage->inputMode() != EndPointMode::MEMORY && tstage->inputMode() != EndPointMode::FILE)
        throw std::runtime_error("only memory or file input supported for tuplex yet!");
    if(tstage->outputMode() != EndPointMode::MEMORY && tstage->outputMode() != EndPointMode::FILE)
        throw std::runtime_error("only memory or file output supported for tuplex yet!");

    // lazy init buffer
    if(!g_executor)
        g_executor = new LambdaExecutor(MAX_RESULT_BUFFER_SIZE);
    else
        g_executor->reset();
    lambda_executor_setup = true;

    timer.mark_time("initialize_stage_and_executor");

    void* userData = g_executor; // where to save state!

    // input reader
    std::unique_ptr<FileInputReader> reader;
    bool normalCaseEnabled = false;
    auto inputNodeID = req.stage().inputnodeid();

    switch(tstage->inputMode()) {
        case EndPointMode::FILE: {

            // there should be a couple input uris in this request!
            // => decode using optional fileinput params from the
            // @TODO: ranges

            // only csv + text so far supported!
            if(tstage->inputFormat() == FileFormat::OUTFMT_CSV) {

                // decode from file input params everything
                auto numColumns = tstage->csvNumFileInputColumns();
                vector<std::string> header;
                if(tstage->csvHasHeader())
                    header = tstage->csvHeader();
                auto delimiter = tstage->csvInputDelimiter();
                auto quotechar = tstage->csvInputQuotechar();
                auto colsToKeep = tstage->columnsToKeep();

                auto csv = new CSVReader(userData, reinterpret_cast<codegen::cells_row_f>(syms->functor),
                                         normalCaseEnabled,
                                         inputNodeID,
                                         reinterpret_cast<codegen::exception_handler_f>(exceptRowCallback),
                                         numColumns, delimiter,
                                         quotechar, colsToKeep);
                // fetch full file for now, later make this optional!
                // csv->setRange(rangeStart, rangeStart + rangeSize);
                csv->setHeader(header);
                reader.reset(csv);
            } else if(tstage->inputFormat() == FileFormat::OUTFMT_TEXT) {
                auto text = new TextReader(userData, reinterpret_cast<codegen::cells_row_f>(syms->functor));
                // fetch full range for now, later make this optional!
                // text->setRange(rangeStart, rangeStart + rangeSize);
                reader.reset(text);
            } else throw std::runtime_error("unsupported input file format given");


            // read files and process one by one
            timer.mark_time("initialize_file_reader");
            for(const auto &uri : inputURIs) {
                // reading files...
                reader->read(uri);
                runtime::rtfree_all();
                timer.mark_file_time(uri); // TODO: this might not be unique...
            }

            break;
        }
        case EndPointMode::MEMORY: {
            // not supported yet
            // => simply read in partition from file (tuplex in memory format)
            // load file into partition, then call functor on the partition.

            // TODO: Could optimize this by streaming in data & calling compute over blocks of data!
            for(const auto &uri : inputURIs) {
                auto vf = VirtualFileSystem::open_file(uri, VirtualFileMode::VFS_READ);
                if(vf) {
                    auto file_size = vf->size();
                    size_t bytes_read = 0;
                    auto input_buffer = new uint8_t[file_size];
                    vf->read(input_buffer, file_size, &bytes_read);
                    cout<<"Read "<<bytes_read<<" bytes from "<<uri.toString()<<endl;

                    assert(syms->functor);
                    int64_t normal_row_output_count = 0;
                    int64_t bad_row_output_count = 0;
                    auto response_code = syms->functor(userData, input_buffer, bytes_read, &normal_row_output_count, &bad_row_output_count, false);
                    cout<<"RC="<<response_code<<" ,computed "<<normal_row_output_count<<" normal rows, "<<bad_row_output_count<<" bad rows"<<endl;

                    delete [] input_buffer;
                    vf->close();
                } else {
                    cerr<<"Error reading "<<uri.toString()<<endl;
                }
                timer.mark_mem_time(uri); // TODO: this might not be unique...
            }

            break;
        }
        default: {
            throw std::runtime_error("unsupported input mode found");
            break;
        }
    }

    // when processing is done, simply output everything to URI (should be an S3 one!)
    cout<<"writing "<<sizeToMemString(g_executor->bytesWritten)<<" to "<<outputURI.toPath()<<endl;
    // write first the # rows, then the data
    auto vfs = VirtualFileSystem::fromURI(outputURI);
    auto mode = VirtualFileMode::VFS_OVERWRITE | VirtualFileMode::VFS_WRITE;
    if(tstage->outputFormat() == FileFormat::OUTFMT_CSV || tstage->outputFormat() == FileFormat::OUTFMT_TEXT)
        mode |= VirtualFileMode::VFS_TEXTMODE;
    auto file = tuplex::VirtualFileSystem::open_file(outputURI, mode);
    if(!file)
        throw std::runtime_error("could not open " + outputURI.toPath() + " to write output");

    if(tstage->outputMode() == EndPointMode::FILE) {
        // special case: CSV, need to write header!
        cout<<"output format is file"<<endl;

        // header?
        // create CSV header if desired
        uint8_t *header = nullptr;
        size_t header_length = 0;

        // write header if desired...
        auto outOptions = tstage->outputOptions();
        bool writeHeader = stringToBool(get_or(outOptions, "header", "false"));
        if(writeHeader) {
            // fetch special var csvHeader
            auto headerLine = outOptions["csvHeader"];
            header_length = headerLine.length();
            header = new uint8_t[header_length+1];
            memset(header, 0, header_length + 1 );
            memcpy(header, (uint8_t *)headerLine.c_str(), header_length);
        }

        file->write(header, header_length);
        file->write(g_executor->buffer, g_executor->bytesWritten);
        file->close(); // important, because S3 files work lazily

    } else if(tstage->outputMode() == EndPointMode::MEMORY) {
        // write as tuplex file, i.e. first the number of rows then the binary contents.
        cout<<"output format is memory"<<endl;

        assert(sizeof(int64_t) == sizeof(size_t));
        file->write(&g_executor->bytesWritten, sizeof(int64_t));
        file->write(&g_executor->numOutputRows, sizeof(int64_t));
        file->write(g_executor->buffer, g_executor->bytesWritten);
        file->close(); // important, because S3 files work lazily
    } else throw std::runtime_error("unsupported output format!");
    timer.mark_time("write_output");

    cout<<"Task done."<<endl;

    // call release func for stage globals
    if(syms->releaseStageFunctor && syms->releaseStageFunctor() != 0)
        throw std::runtime_error("releaseStage() failed for stage " + std::to_string(tstage->number()));
    auto taskTime = task_execution_timer.time();
    timer.mark_time("release_stage");

    // create protobuf result
    tuplex::messages::InvocationResponse result;
    // fill in globals
    fillInGlobals(&result);
    result.set_status(tuplex::messages::InvocationResponse_Status_SUCCESS);
    for(const auto& uri : inputURIs) {
        result.add_inputuris(uri.toPath());
    }
    result.add_outputuris(outputURI.toPath());
    result.set_taskexecutiontime(taskTime);
    for(const auto& keyval : timer.timings) {
        (*result.mutable_breakdowntimes())[keyval.first] = keyval.second;
    }

    return result;
}
