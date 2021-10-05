#include <gtest/gtest.h>
#include <Context.h>

//TEST(ORC, ReadProfile) {
//    using namespace tuplex;
//    ContextOptions co = ContextOptions::defaults();
//    co.set("tuplex.executorCount", "0");
//    Context context(co);
//    context.orc("../../../../benchmarks/orc/data/large100MB.orc")
//            .aggregate(UDF("lambda x, y: x + y"), UDF("lambda a, x: a + 1"), Row(0))
//            .collectAsVector();
//}
//
//TEST(ORC, WriteProfile) {
//    using namespace tuplex;
//    ContextOptions co = ContextOptions::defaults();
//    co.set("tuplex.executorCount", "0");
//    Context context(co);
//    context.csv("../../../../benchmarks/orc/data/large100MB.csv").toorc("out.orc");
//}
//
//TEST(ORC, WriteParts) {
//    using namespace tuplex;
//    ContextOptions co = ContextOptions::defaults();
//    Context context(co);
//    context.csv("../../../../benchmarks/orc/data/large100MB.csv").toorc("out.orc");
//}
