//
// Created by Leonhard Spiegelberg on 9/6/22.
//

#ifndef TUPLEX_ACCESSPATHVISITOR_H
#define TUPLEX_ACCESSPATHVISITOR_H

#include <graphviz/GraphVizGraph.h>
#include <visitors/IPrePostVisitor.h>
#include <unordered_map>
#include <visitors/IReplaceVisitor.h>
#include <visitors/ColumnRewriteVisitor.h>
#include <tracing/TraceVisitor.h>
#include <visitors/ApplyVisitor.h>

namespace tuplex {

    // for access path detection - no support for . syntax, i.e. something like x.hello.test won't be supported.
    // basically the algorithm is for each identifier, fetch longest id[key1][key2]... chain.
    // => then sort out unique ones!
    class SelectionPathAtom {
    public:
        SelectionPathAtom() : _index(-1), _is_wildcard(true) {}

        SelectionPathAtom(int index) : _index(index), _is_wildcard(false) {}
        SelectionPathAtom(const std::string& key) : _index(-1), _is_wildcard(false), _key(key) {}

        static SelectionPathAtom wildcard() {
            SelectionPathAtom a; a._is_wildcard = true;
            return a;
        }

        std::string desc() const {
            if(_is_wildcard)
                return "*";
            if(_index >= 0) {
                return std::to_string(_index);
            }
            return escape_to_python_str(_key);
        }

        bool operator == (const SelectionPathAtom& other) const {
            return _index == other._index && _key == other._key && _is_wildcard == other._is_wildcard;
        }

        bool operator != (const SelectionPathAtom& other) const {
            return !(*this == other);
        }
    private:
        // only int and string keys supported for now
        int _index; // raw value
        std::string _key; // raw value
        bool _is_wildcard;
    };

    // new projection pushdown mechanism. I.e., subselect "paths" into JSON/dict structure
    struct SelectionPath {
        // each path maps a single item to a target?
        // 0.*.'test'
        // maybe use https://datatracker.ietf.org/doc/id/draft-goessner-dispatch-jsonpath-00.html ?

        // so if we have something like row['repo']['name'] -> this should return only the name field
        // and the selection path 'repo'.'name'.
        // for arrays, use wildcard syntax for now, i.e. [*] to return everything. Yet, we can subselect within them
        // by appending to the path
        std::vector<SelectionPathAtom> atoms;
        std::string name; // identifier name

        SelectionPath() {}

        SelectionPath(const std::string& name,
                      const std::vector<SelectionPathAtom>& atoms) : name(name), atoms(atoms) {}

        std::string desc() const {
            std::stringstream ss;
            ss<<name;
            if(!atoms.empty())
                ss<<".";
            for(unsigned i = 0; i < atoms.size(); ++i) {
                ss<<atoms[i].desc();
                if(i != atoms.size() - 1)
                    ss<<".";
            }
            return ss.str();
        }

        bool empty() const { return atoms.empty(); }

        inline bool operator == (const SelectionPath& other) const {
            if(name != other.name)
                return false;
            if(atoms.size() != other.atoms.size())
                return false;
            for(unsigned i = 0; i < atoms.size(); ++i)
                if(atoms[i] != other.atoms[i])
                    return false;
            return true;
        }

        inline bool operator != (const SelectionPath& other) const {
            return !(*this == other);
        }
    };

    inline std::ostream& operator << (std::ostream& os, const SelectionPath& path) { return os<<path.desc(); }

    // based on LambdaAccessedColumnVisitor
    class AccessPathVisitor : public IPrePostVisitor {
    protected:
        virtual void postOrder(ASTNode *node) override;
        virtual void preOrder(ASTNode *node) override;

        bool _tupleArgument;
        size_t _numColumns;
        bool _singleLambda;
        std::vector<std::string> _argNames;
        std::unordered_map<std::string, bool> _argFullyUsed;
        std::unordered_map<std::string, std::vector<size_t>> _argSubscriptIndices;

        // holds map from identifier -> accessPath
        std::unordered_map<std::string, std::vector<SelectionPath>> _accessPaths;

        // has subscript been already visited?
        std::unordered_map<ASTNode*, bool> _nodeVisited;

        SelectionPath longestAccessPath(NSubscription* sub);

        std::vector<SelectionPath> _accessedPaths; // holds ALL accessed paths
    private:
        bool node_visited(ASTNode* n) {
            assert(n);
            auto it = _nodeVisited.find(n);
            if(it == _nodeVisited.end())
                return false;
            return it->second;
        }
        void mark_visited(ASTNode* n) { if(!n)return; _nodeVisited[n] = true; }
    public:
        AccessPathVisitor() : _tupleArgument(false),
                                        _numColumns(0), _singleLambda(false) {}


        std::vector<size_t> getAccessedIndices() const;

        std::vector<SelectionPath> accessedPaths() const { return _accessedPaths; }
    };
}
#endif //TUPLEX_ACCESSPATHVISITOR_H
