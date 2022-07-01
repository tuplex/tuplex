//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <graphviz/GraphVizBuilder.h>
#include <fstream>

#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <array>
#include <sstream>
#include <Utils.h>

const std::string GraphVizBuilder::nodePrefix = "n";


std::string escapeDotLabel(const std::string& s) {
    std::string res = "";
    res.reserve(s.length());
    for(auto c : s) {
        // chars to escape
        if(c == '{' || c== '}' || c=='"' || c=='<' || c=='>' || c=='|') {
            res.push_back('\\');
        }
        res.push_back(c);
    }

    return res;
}

int GraphVizBuilder::addNode(const std::string &label) {
    int id = _id++;

    auto escaped_label = escapeDotLabel(label);
    _nodes.push_back(nodePrefix + std::to_string(id) + " [shape=record, ordering=out, label=\"" + escaped_label + "\"];");

    return id;
}

int GraphVizBuilder::addHTMLNode(const std::string &label) {
    int id = _id++;

    _nodes.push_back(nodePrefix + std::to_string(id) + " [shape=plaintext, ordering=out, label=<" + label + ">];");

    return id;
}

void GraphVizBuilder::addEdge(const int iFrom, const int iTo, const std::string &subfieldFrom,
                              const std::string &subfieldTo) {
    std::string from = nodePrefix + std::to_string(iFrom);
    std::string to = nodePrefix + std::to_string(iTo);

    if(subfieldFrom.length() > 0)
        from += ":" + subfieldFrom;

    if(subfieldTo.length() > 0)
        to += ":" + subfieldTo;


    _edges.push_back(from + " -> " + to + ";");
}


bool GraphVizBuilder::writeToStream(std::ostream &os) {
    os << "digraph G {" << std::endl;

    // first node definitions
    if(!_nodes.empty())
        for(auto it = _nodes.cbegin(); it != _nodes.cend(); ++it) {
            os << "\t" << *it << std::endl;
        }

    os << std::endl;

    if(!_edges.empty())
        for(auto it = _edges.cbegin(); it != _edges.cend(); ++it) {
            os << "\t" << *it << std::endl;
        }

    os << "}" << std::endl;

    return os.good();
}

bool GraphVizBuilder::saveToDotFile(const std::string &path) {

    std::ofstream ofs(path, std::ofstream::out);

    writeToStream(ofs);

    ofs.close();

    return ofs.good();
}

bool GraphVizBuilder::saveToPDF(const std::string &path) {

    // @TODO: Improve this later via direct streaming!
    // @TODO: This is bad design... However, I don't wanna spend time on handling temp files in C++...

    std::string tempfile = tempFileName();

    saveToDotFile(tempfile);


    // @TODO: make this code here signal-safe!
    // https://www.oreilly.com/library/view/secure-programming-cookbook/0596003943/ch01s06.html
    // i.e. check https://www.oreilly.com/library/view/secure-programming-cookbook/0596003943/ch01s07.html
    // => left for now, b.c. saveToPDF will be anyways only handled in debug version...
    std::string cmd = "dot -Tpdf " + tempfile + " -o " + path;
    std::array<char, 128> buffer;
    std::string result;
    std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
    if (!pipe) throw std::runtime_error("popen() failed!");
    while (!feof(pipe.get())) {
        if (fgets(buffer.data(), 128, pipe.get()) != nullptr)
        result += buffer.data();
    }

    // can process result here if necessary...

    return true;
}