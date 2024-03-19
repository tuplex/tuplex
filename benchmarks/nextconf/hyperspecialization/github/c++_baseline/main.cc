//
// Created by Leonhard Spiegelberg on 3/15/24.
//

#include <iostream>
#include <vector>
#include <simdjson.h>
#include "timer.h"
#include <glob.h>
#include <sstream>

#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <lyra/lyra.hpp>

// use AWS SDK bundled cjson
#include <aws/core/external/cjson/cJSON.h>
#include <fstream>
#include <any>
#include <filesystem>

int dirExists(const char *path)
{
    struct stat info;

    if(stat( path, &info ) != 0)
        return 0;
    else if(info.st_mode & S_IFDIR)
        return 1;
    else
        return 0;
}

std::vector<std::string> glob_pattern(const std::string &pattern) {
    using namespace std;

    // from https://stackoverflow.com/questions/8401777/simple-glob-in-c-on-unix-system
    // glob struct resides on the stack
    glob_t glob_result;
    memset(&glob_result, 0, sizeof(glob_result));

    // do the glob operation
    int return_value = ::glob(pattern.c_str(), GLOB_TILDE | GLOB_MARK, NULL, &glob_result);
    if(return_value != 0) {
        globfree(&glob_result);

        // special case, no match
        if(GLOB_NOMATCH == return_value) {
            std::cerr<<"did not find any files for pattern '" + pattern + "'"<<std::endl;
            return {};
        }

        stringstream ss;
        ss << "glob() failed with return_value " << return_value << endl;
        throw std::runtime_error(ss.str());
    }

    // collect all the filenames into a std::list<std::string>
    vector<std::string> uris;
    for(size_t i = 0; i < glob_result.gl_pathc; ++i) {
        uris.emplace_back(std::string(glob_result.gl_pathv[i]));
    }

    // cleanup
    globfree(&glob_result);

    // done
    return uris;
}

std::string view_to_str(const std::string_view& v) {
    return std::string(v.begin(), v.end());
}


namespace mode {

    namespace load_to_condensed_c_struct {

        // idealized struct type representing every combo
        struct LoadedRow {

            // <NAME>_present indicates whether field <NAME> is contained within JSON.
            // <NAME> is only valid if <NAME>_present=true.

            std::string created_at;
            std::string type;

            int64_t payload_target_id;
            bool payload_target_id_present;

            int64_t payload_id;
            bool payload_id_present;

            bool repository_present;
            bool repository_id_present;
            int64_t repository_id;

            int64_t repo_id;
            bool repo_present;
            bool repo_id_present;

            struct Commit {
                std::string sha;
                std::string author_email;
                std::optional<std::string> author_name;
                std::string message;
                bool distinct_present;
                bool distinct;
                std::string url;
            };

            bool commits_present;
            std::vector<Commit> commits;

            inline void parse(const simdjson::dom::element& doc) {
                created_at = view_to_str(doc["created_at"].get_string().value());
                type = view_to_str(doc["type"].get_string().value());

                payload_target_id_present = doc["payload"]["target"]["id"].get(payload_target_id) == simdjson::SUCCESS;
                payload_id_present = doc["payload"]["id"].get(payload_id) == simdjson::SUCCESS;

                repo_present = doc["repo"].get_object().error() == simdjson::SUCCESS;
                repo_id_present = doc["repo"]["id"].get(repo_id) == simdjson::SUCCESS;

                repository_present = doc["repository"].get_object().error() == simdjson::SUCCESS;
                repository_id_present = doc["repository"]["id"].get(repository_id) == simdjson::SUCCESS;

                commits_present = doc["payload"]["commits"].get_array().error() == simdjson::SUCCESS;

                if(commits_present) {
                    for(auto el : doc["payload"]["commits"]) {
                        Commit c;
                        c.sha = view_to_str(el["sha"].get_string());

                        c.author_email = view_to_str(el["author"]["email"].get_string());
                        c.author_name = el["author"]["name"].is_null() ? std::optional<std::string>() : view_to_str(el["author"]["name"].get_string());

                        c.message = view_to_str(el["message"].get_string());
                        c.distinct_present = el["distinct"].get(c.distinct) == simdjson::SUCCESS;
                        c.url = view_to_str(el["url"].get_string());
                        commits.push_back(c);
                    }
                }
            }
        };

        int64_t extract_year(const LoadedRow& row) {
            std::string created_at = row.created_at;

            // extract to array
            std::vector<std::string> arr;
            std::string remaining = created_at;
            std::string delimiter = "-";
            auto pos = remaining.find(delimiter);
            while(pos != std::string::npos) {
                arr.push_back(remaining.substr(0, pos));
                remaining = remaining.substr(pos + delimiter.size());
                pos = remaining.find(delimiter);
            }
            arr.push_back(remaining);

            // optimized access: --> this actually saves some time!!!
            // auto year_str = created_at.substr(0, created_at.find("-"));


            auto year_str = arr.front();
            int64_t year = std::stoi(year_str);
            return year;
        }

        std::optional<int64_t> extract_repo_id(const LoadedRow& row, int64_t year) {
            // modeled after
            // def extract_repo_id(row):
            //    if 2012 <= row['year'] <= 2014:
            //
            //        if row['type'] == 'FollowEvent':
            //            return row['payload']['target']['id']
            //
            //        if row['type'] == 'GistEvent':
            //            return row['payload']['id']
            //
            //        repo = row.get('repository')
            //
            //        if repo is None:
            //            return None
            //        return repo.get('id')
            //    else:
            //        return row['repo'].get('id')
            if(2012 <= year && year <= 2014) {
                if("FollowEvent" == row.type) {
                    if(!row.payload_target_id_present)
                        throw std::runtime_error("error");
                    return row.payload_target_id;
                }

                if("GistEvent" == row.type) {
                    if(!row.payload_id_present)
                        throw std::runtime_error("error");
                    return row.payload_id;
                }

                if(!row.repository_present)
                    return {};
                if(!row.repository_id_present)
                    return {};
                return row.repository_id;
            } else {
                if(!row.repo_present)
                    throw std::runtime_error("");
                if(!row.repo_id_present)
                    return {};
                return row.repo_id;
            }
        }

        int64_t process_pipeline(FILE **pFile, const std::string &output_path, const simdjson::dom::element& doc) {
            using namespace std;

            auto pf = *pFile;
            if(!pf) {
                cout<<"Opening file "<<output_path<<" for writing"<<endl;
                pf = fopen(output_path.c_str(), "w");
                if(!pf) {
                    cerr<<"Could not open output file "<<output_path<<endl;
                    throw std::runtime_error("error opening file " + output_path);
                }

                // write header
                std::string header = "type,repo_id,year,number_of_commits\n";
                fwrite(header.c_str(), header.size(), 1, pf);

                *pFile = pf;
            }

            // process pipeline and write output to file as CSV
            // this pipeline is equivalent to the python pipeline
            //     ctx.json(input_pattern, True, True, sm) \
            //       .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
            //       .withColumn('repo_id', extract_repo_id) \
            //       .filter(lambda x: x['type'] == 'ForkEvent') \
            //       .withColumn('commits', lambda row: row['payload'].get('commits')) \
            //       .withColumn('number_of_commits', lambda row: len(row['commits']) if row['commits'] else 0) \
            //       .selectColumns(['type', 'repo_id', 'year', 'number_of_commits']) \
            //       .tocsv(s3_output_path)

            // first step is to parse JSON from simdjson into C struct (condensed)
            LoadedRow row;
            row.parse(doc);

            int64_t year = extract_year(row);

            std::string type = row.type;

            auto repo_id = extract_repo_id(row, year);

            if(type != "ForkEvent") {
                return 0;
            }


            std::optional<std::vector<LoadedRow::Commit>> commits = row.commits_present ? row.commits : std::optional<std::vector<LoadedRow::Commit>>();

            int64_t number_of_commits = commits.has_value() ? commits.value().size() : 0;

            std::stringstream ss;
            auto repo_str = repo_id.has_value() ? std::to_string(repo_id.value()) : "";
            ss<<type<<","<<repo_str<<","<<year<<","<<number_of_commits<<"\n";
            auto line = ss.str();

            // do not write '\0'
            fwrite(line.c_str(), line.size(), 1, pf);

            // return how many line are written.
            return 1;
        }

    }

    namespace cjson {

        std::optional<int64_t> extract_repo_id(cJSON* row, int64_t year, std::string type) {
            // modeled after
            // def extract_repo_id(row):
            //    if 2012 <= row['year'] <= 2014:
            //
            //        if row['type'] == 'FollowEvent':
            //            return row['payload']['target']['id']
            //
            //        if row['type'] == 'GistEvent':
            //            return row['payload']['id']
            //
            //        repo = row.get('repository')
            //
            //        if repo is None:
            //            return None
            //        return repo.get('id')
            //    else:
            //        return row['repo'].get('id')
            if(2012 <= year && year <= 2014) {
                if("FollowEvent" == type) {
                    return (int64_t)cJSON_AS4CPP_GetNumberValue(cJSON_AS4CPP_GetObjectItem(cJSON_AS4CPP_GetObjectItem(cJSON_AS4CPP_GetObjectItem(row, "payload"), "target"), "id"));
                }

                if("GistEvent" == type) {
                    return (int64_t)cJSON_AS4CPP_GetNumberValue(cJSON_AS4CPP_GetObjectItem(cJSON_AS4CPP_GetObjectItem(row, "payload"), "id"));
                }

                auto repo = cJSON_AS4CPP_GetObjectItem(row, "repository");
                if(!repo) {
                    return {};
                }

                auto id = cJSON_AS4CPP_GetObjectItem(repo, "id");
                if(!id)
                    return {};
                else
                    return (int64_t)cJSON_AS4CPP_GetNumberValue(id);
            } else {

                auto id = cJSON_AS4CPP_GetObjectItem(cJSON_AS4CPP_GetObjectItem(row, "repo"), "id");
                if(!id)
                    return {};
                else
                    return (int64_t)cJSON_AS4CPP_GetNumberValue(id);
            }
        }

        int64_t extract_year(cJSON* row) {
            std::string created_at = cJSON_AS4CPP_GetStringValue(cJSON_AS4CPP_GetObjectItem(row, "created_at"));

            // extract to array
            std::vector<std::string> arr;
            std::string remaining = created_at;
            std::string delimiter = "-";
            auto pos = remaining.find(delimiter);
            while(pos != std::string::npos) {
                arr.push_back(remaining.substr(0, pos));
                remaining = remaining.substr(pos + delimiter.size());
                pos = remaining.find(delimiter);
            }
            arr.push_back(remaining);

            // optimized access: --> this actually saves some time!!!
            // auto year_str = created_at.substr(0, created_at.find("-"));


            auto year_str = arr.front();
            int64_t year = std::stoi(year_str);
            return year;
        }

        std::optional<std::vector<cJSON*>> extract_commits(cJSON* row) {
            // lambda row: row['payload'].get('commits')

            auto commits = cJSON_AS4CPP_GetObjectItem(cJSON_AS4CPP_GetObjectItem(row, "payload"), "commits");
            if(!commits)
                return {};

            std::vector<cJSON*> v;
            for(unsigned i = 0; i < cJSON_AS4CPP_GetArraySize(commits); ++i)
                v.push_back(cJSON_AS4CPP_GetArrayItem(commits, i));
            return v;
        }

        int64_t process_pipeline(FILE **pFile, const std::string &output_path, const simdjson::dom::element& doc) {
            using namespace std;

            auto pf = *pFile;
            if(!pf) {
                cout<<"Opening file "<<output_path<<" for writing"<<endl;
                pf = fopen(output_path.c_str(), "w");
                if(!pf) {
                    cerr<<"Could not open output file "<<output_path<<endl;
                    throw std::runtime_error("error opening file " + output_path);
                }

                // write header
                std::string header = "type,repo_id,year,number_of_commits\n";
                fwrite(header.c_str(), header.size(), 1, pf);

                *pFile = pf;
            }

            // process pipeline and write output to file as CSV
            // this pipeline is equivalent to the python pipeline
            //     ctx.json(input_pattern, True, True, sm) \
            //       .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
            //       .withColumn('repo_id', extract_repo_id) \
            //       .filter(lambda x: x['type'] == 'ForkEvent') \
            //       .withColumn('commits', lambda row: row['payload'].get('commits')) \
            //       .withColumn('number_of_commits', lambda row: len(row['commits']) if row['commits'] else 0) \
            //       .selectColumns(['type', 'repo_id', 'year', 'number_of_commits']) \
            //       .tocsv(s3_output_path)

            // first step is to convert line to cJSON. --> could get rid off simdjson, by directly parsing to cjson.
            std::stringstream json_ss;
            json_ss<<doc;
            std::string json_line = json_ss.str();
            auto row = cJSON_AS4CPP_ParseWithLength(json_line.c_str(), json_line.size());

            int64_t year = extract_year(row);

            std::string type = cJSON_AS4CPP_GetStringValue(cJSON_AS4CPP_GetObjectItem(row, "type"));

            auto repo_id = extract_repo_id(row, year, type);

            if(type != "ForkEvent") {
                cJSON_AS4CPP_Delete(row);
                return 0;
            }


            auto commits = extract_commits(row);

            int64_t number_of_commits = commits.has_value() ? commits.value().size() : 0;

            std::stringstream ss;
            auto repo_str = repo_id.has_value() ? std::to_string(repo_id.value()) : "";
            ss<<type<<","<<repo_str<<","<<year<<","<<number_of_commits<<"\n";
            auto line = ss.str();

            // do not write '\0'
            fwrite(line.c_str(), line.size(), 1, pf);

            cJSON_AS4CPP_Delete(row);

            // return how many line are written.
            return 1;
        }
    }

    namespace best {
        std::optional<int64_t> extract_repo_id(const simdjson::dom::element& doc, int64_t year, std::string type) {
            // modeled after
            // def extract_repo_id(row):
            //    if 2012 <= row['year'] <= 2014:
            //
            //        if row['type'] == 'FollowEvent':
            //            return row['payload']['target']['id']
            //
            //        if row['type'] == 'GistEvent':
            //            return row['payload']['id']
            //
            //        repo = row.get('repository')
            //
            //        if repo is None:
            //            return None
            //        return repo.get('id')
            //    else:
            //        return row['repo'].get('id')
            if(2012 <= year && year <= 2014) {
                if("FollowEvent" == type) {
                    return doc["payload"]["target"]["id"].get_int64();
                }

                if("GistEvent" == type) {
                    return doc["payload"]["id"].get_int64();
                }

                auto repo = doc["repository"];
                if(repo.is_null()) {
                    return {};
                }

                int64_t id;
                auto error = repo["id"].get(id);
                if(error)
                    return {};
                else
                    return id;
            } else {
                int64_t id;
                auto error = doc["repo"]["id"].get(id);
                if(error)
                    return {};
                else
                    return id;
            }
        }

        int64_t extract_year(const simdjson::dom::element& doc) {
            auto created_at = view_to_str(doc["created_at"].get_string().value());

            // extract to array
            std::vector<std::string> arr;
            std::string remaining = created_at;
            std::string delimiter = "-";
            auto pos = remaining.find(delimiter);
            while(pos != std::string::npos) {
                arr.push_back(remaining.substr(0, pos));
                remaining = remaining.substr(pos + delimiter.size());
                pos = remaining.find(delimiter);
            }
            arr.push_back(remaining);

            // optimized access: --> this actually saves some time!!!
            // auto year_str = created_at.substr(0, created_at.find("-"));


            auto year_str = arr.front();
            int64_t year = std::stoi(year_str);
            return year;
        }

        std::optional<std::vector<simdjson::dom::element>> extract_commits(const simdjson::dom::element& doc) {
            // lambda row: row['payload'].get('commits')

            auto ret = doc["payload"]["commits"].get_array();
            if(ret.error()) {
                return {};
            } else {
                auto val = ret.value();
                return std::vector<simdjson::dom::element>(val.begin(), val.end());
            }
        }

        int64_t process_pipeline(FILE **pFile, const std::string &output_path, const simdjson::dom::element& doc) {
            using namespace std;

            auto pf = *pFile;
            if(!pf) {
                cout<<"Opening file "<<output_path<<" for writing"<<endl;
                pf = fopen(output_path.c_str(), "w");
                if(!pf) {
                    cerr<<"Could not open output file "<<output_path<<endl;
                    throw std::runtime_error("error opening file " + output_path);
                }

                // write header
                std::string header = "type,repo_id,year,number_of_commits\n";
                fwrite(header.c_str(), header.size(), 1, pf);

                *pFile = pf;
            }

            // process pipeline and write output to file as CSV
            // this pipeline is equivalent to the python pipeline
            //     ctx.json(input_pattern, True, True, sm) \
            //       .withColumn('year', lambda x: int(x['created_at'].split('-')[0])) \
            //       .withColumn('repo_id', extract_repo_id) \
            //       .filter(lambda x: x['type'] == 'ForkEvent') \
            //       .withColumn('commits', lambda row: row['payload'].get('commits')) \
            //       .withColumn('number_of_commits', lambda row: len(row['commits']) if row['commits'] else 0) \
            //       .selectColumns(['type', 'repo_id', 'year', 'number_of_commits']) \
            //       .tocsv(s3_output_path)

            int64_t year = extract_year(doc);

            auto type = view_to_str(doc["type"].get_string().value());

            auto repo_id = extract_repo_id(doc, year, type);

            if(type != "ForkEvent")
                return 0;


            auto commits = extract_commits(doc);

            int64_t number_of_commits = commits.has_value() ? commits.value().size() : 0;

            std::stringstream ss;
            auto repo_str = repo_id.has_value() ? std::to_string(repo_id.value()) : "";
            ss<<type<<","<<repo_str<<","<<year<<","<<number_of_commits<<"\n";
            auto line = ss.str();

            // do not write '\0'
            fwrite(line.c_str(), line.size(), 1, pf);

            // return how many line are written.
            return 1;
        }
    }
}

std::tuple<int,int,double> process_path(const std::string& input_path, const std::string& output_path, const std::string& mode) {
    using namespace std;
    Timer load_timer;
    // read file into memory
    uint8_t* buffer = nullptr;
    struct stat s;
    stat(input_path.c_str(), &s);
    cout<<"Found file "<<input_path<<" with size "<<s.st_size<<", loading to memory..."<<endl;
    buffer = new uint8_t[s.st_size + simdjson::SIMDJSON_PADDING];

    FILE *pf = fopen(input_path.c_str(), "r");
    fread(buffer, s.st_size, 1, pf);
    fclose(pf);
    pf = nullptr;
    auto loading_time_in_s = load_timer.time();


    // select function to use
    auto functor = mode::best::process_pipeline;

    if(mode == "best") {
        functor = mode::best::process_pipeline;
    } else if(mode == "cjson") {
        functor = mode::cjson::process_pipeline;
    } else if(mode == "cstruct") {
        functor = mode::load_to_condensed_c_struct::process_pipeline;
    } else {
        throw std::runtime_error("unsupported mode " + mode);
    }

    // output file
    FILE *pfout = nullptr;

    Timer timer;
    cout<<"Parsing file"<<endl;
    size_t row_count = 0;
    size_t output_row_count = 0;
    simdjson::dom::parser parser;
    simdjson::dom::document_stream stream;
    auto error = parser.parse_many((const char*)buffer,
                                   (size_t)s.st_size,
                                   std::min(simdjson::SIMDJSON_MAXSIZE_BYTES, (size_t)s.st_size)).get(stream);
    if (error) { /* do something */ }
    auto i = stream.begin();
    for(; i != stream.end(); ++i) {
        auto doc = *i;
        if (!doc.error()) {
            // std::cout << "got full document at " << i.current_index() << std::endl;
            //std::cout << i.source() << std::endl;

            // process pipeline here based on simdjson doc:
            output_row_count += functor(&pfout, output_path, doc.value());


            row_count++;
        } else {
            std::cout << "got broken document at " << i.current_index() << std::endl;
            break;
        }
    }
    cout<<"Parsed "<<row_count<<" rows from file "<<input_path<<endl;
    cout<<"Wrote "<<output_row_count<<" output rows to "<<output_path<<endl;
    cout<<"Took "<<timer.time()<<"s to process"<<endl;

    // lazy close file
    if(pfout) {
        fflush(pfout);
        fclose(pfout);
    }

    delete [] buffer;

    return make_tuple(row_count, output_row_count, loading_time_in_s);
}

std::string vec_to_string(const std::vector<std::string>& v) {
    if(v.empty())
        return "[]";
    std::stringstream ss;
    for(unsigned i = 0; i < v.size(); ++i) {
        ss<<v[i];
        if(i != v.size() - 1)
            ss<<",";
    }
    return ss.str();
}

int main(int argc, char* argv[]) {
    using namespace std;

//    static_assert(false == SIMDJSON_THREADS_ENABLED, "threads disabled");

    // parse arguments
    string input_pattern;
    string output_path = "local-output";
    string mode = "best";
    string result_path;
    std::vector<std::string> supported_modes{"best", "cjson", "cstruct"};
    bool show_help = false;

    // construct CLI
    auto cli = lyra::cli();
    cli.add_argument(lyra::help(show_help));
    cli.add_argument(lyra::opt(input_pattern, "inputPattern").name("-i").name("--input-pattern").help("input pattern from which to read files."));
    cli.add_argument(lyra::opt(output_path, "outputPath").name("-o").name("--output-path").help("output path where to store results, will be created as directory if not exists."));
    cli.add_argument(lyra::opt(result_path, "resultPath").name("-r").name("--result-path").help("output path where to store timings."));
    cli.add_argument(lyra::opt(mode, "mode").name("-m").name("--mode").help("C++ mode to use, supported modes are: " +
                                                                                           vec_to_string(supported_modes)));
    auto result = cli.parse({argc, argv});
    if(!result) {
        cerr<<"Error parsing command line: "<<result.errorMessage()<<std::endl;
        return 1;
    }

    if(show_help) {
        cout<<cli<<endl;
        return 0;
    }

    if(input_pattern.empty()) {
        cerr<<"Must specify input pattern via -i or --input-pattern."<<endl;
        return 1;
    }

    if(supported_modes.end() == std::find(supported_modes.begin(), supported_modes.end(), mode)) {
        cerr<<"Unknown mode "<<mode<<" found, supported are "<<vec_to_string(supported_modes)<<endl;
        return 1;
    }


    Timer timer;
    cout<<"starting C++ baseline::"<<endl;
    cout<<"Pipeline will process: "<<input_pattern<<" -> "<<output_path<<endl;
    cout<<timer.time()<<"s "<<"Globbing files from "<<input_pattern<<endl;
    auto paths = glob_pattern(input_pattern);
    cout<<timer.time()<<"s "<<"found "<<paths.size()<<" paths."<<endl;

    // create local output dir if it doesn't exist yet.
    if(!dirExists(output_path.c_str())) {
//        int rc = mkdir(output_path.c_str(), 0777);
//        if(rc != 0) {
//            cerr<<"Failed to create dir "<<output_path;
//            exit(1);
//        }
        std::filesystem::create_directories(output_path);
    }
    cout<<timer.time()<<"s "<<"saving output to "<<output_path<<endl;

    double total_time_in_s = timer.time();

    std::stringstream ss;
    for(unsigned i = 0; i < paths.size(); ++i) {
        auto path = paths[i];
        cout<<"Processing path "<<(i+1)<<"/"<<paths.size()<<endl;
        Timer path_timer;
        auto part_path = output_path + "/part_" + std::to_string(i) + ".csv";
        int input_row_count=0,output_row_count=0;
        double loading_time = 0.0;
        std::tie(input_row_count, output_row_count, loading_time) = process_path(path, part_path, mode);
        ss<<mode<<","<<path<<","<<part_path<<","<<path_timer.time()<<","<<loading_time<<","<<total_time_in_s<<","<<input_row_count<<","<<output_row_count<<"\n";
    }

    auto csv = "mode,input_path,output_path,time_in_s,loading_time_in_s,total_time_in_s,input_row_count,output_row_count\n" + ss.str();
    cout<<"per-file stats in CSV format::\n"<<csv<<"\n"<<endl;

    if(!result_path.empty()) {
        cout<<"Saving timings to "<<result_path<<endl;
        std::ofstream ofs(result_path, ios::app);
        ofs<<csv;
        ofs.close();
    }

    cout<<"Processed files in "<<timer.time()<<"s"<<endl;
    return 0;
}