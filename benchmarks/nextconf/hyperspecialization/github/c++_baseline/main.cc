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

void process_path(const std::string& input_path, const std::string& output_path) {
    using namespace std;

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
    cout<<"Parsing file"<<endl;
    size_t row_count = 0;
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
            row_count++;
        } else {
            std::cout << "got broken document at " << i.current_index() << std::endl;
            break;
        }
    }
    cout<<"Parsed "<<row_count<<" rows from file "<<input_path<<endl;

    delete [] buffer;
}

int main(int argc, char* argv[]) {
    using namespace std;

    string input_pattern = "/Users/leonhards/projects/2nd-copy/tuplex/test/resources/hyperspecialization/github_daily/*.json.sample";
    string output_path = "local-output";
    Timer timer;
    cout<<timer.time()<<"s "<<"Globbing files from "<<input_pattern<<endl;
    auto paths = glob_pattern(input_pattern);
    cout<<timer.time()<<"s "<<"found "<<paths.size()<<" paths."<<endl;

    // create local output dir if it doesn't exist yet.
    if(!dirExists(output_path.c_str())) {
        int rc = mkdir(output_path.c_str(), 0777);
        if(rc != 0) {
            cerr<<"Failed to create dir "<<output_path;
            exit(1);
        }
    }
    cout<<timer.time()<<"s "<<"saving output to "<<output_path<<endl;

    for(unsigned i = 0; i < paths.size(); ++i) {
        auto path = paths[i];
        cout<<"Processing path "<<(i+1)<<"/"<<paths.size()<<endl;
        process_path(path, output_path + "/part_" + std::to_string(i) + ".csv");
    }

    cout<<"Processing files in "<<timer.time()<<"s"<<endl;
    return 0;
}