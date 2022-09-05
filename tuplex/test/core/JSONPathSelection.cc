//
// Created by Leonhard Spiegelberg on 9/5/22.
//

#include "TestUtils.h"
#include "JsonStatistic.h"

namespace tuplex {
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
            if(_index < 0) {
                return std::to_string(_index);
            }
            return escape_to_python_str(_key);
        }
    private:
        // only int and string keys supported for now
        int _index;
        std::string _key;
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

        std::string desc() const {
            std::stringstream ss;
            for(unsigned i = 0; i < atoms.size(); ++i) {
                ss<<atoms[i].desc();
                if(i != atoms.size() - 1)
                    ss<<".";
            }
            return ss.str();
        }
    };
}

class JSONPathSelection : public PyTest {};

TEST_F(JSONPathSelection, BasicPathSelection) {
    using namespace tuplex;
    using namespace std;

    // fetch the type of some github rows
    std::string gh_path = "../resources/ndjson/github.json";
    std::string data = fileToString(gh_path);
    std::vector<std::vector<std::string>> column_names;
    auto rows = parseRowsFromJSON(data, &column_names, false);
    ASSERT_FALSE(rows.empty());

    cout<<"type of first row: \n"<<rows.front().getRowType().desc()<<endl;

    auto input_row_type = rows.front().getRowType();

    UDF udf("lambda x: x['repo']['url']");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, input_row_type));
    auto output_row_type = udf.getOutputSchema().getRowType();
    cout<<"output type of UDF: "<<output_row_type.desc()<<endl;
}