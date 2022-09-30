//
// Created by leonhard on 9/29/22.
//

#include "HyperUtils.h"

class JsonTuplexTest : public HyperPyTest {};


TEST_F(JsonTuplexTest, BasicLoad) {
    using namespace tuplex;
    using namespace std;

    Context ctx(microTestOptions());

    ctx.json("../resources/ndjson/example1.json").show();
}