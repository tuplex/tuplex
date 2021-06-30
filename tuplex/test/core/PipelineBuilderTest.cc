//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 3/30/20                                                                  //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

//----------------------------------------------------------------------------//
//                                                                            //
//                  Tuplex: Blazing Fast Python Data Science                  //
//                                                                            //
//                                                                            //
//  (c) 2017 - 2021, Tuplex team                                              //
//  Created by Leonhard Spiegelberg first on 3/30/20                          //
//  License: Apache 2.0                                                       //
//----------------------------------------------------------------------------//

////
//// Created by Leonhard Spiegelberg on 3/30/20.
////
//
//#include "TestUtils.h"
//#include <physical/PipelineBuilder.h>
//
//class PipBuilderTest : public PyTest {};
//
//// if(MAP_OK == hashmap_get(hmap, skey, (void**)(&value))) {
////
////                    // old entry exists, free it & copy it over
////                    // determine size
////                    int64_t bucket_size = calc_bucket_size((uint8_t*)value);
////                    int64_t num_rows = *((int64_t*)value);
////
////                    // check calculation is not off, i.e. less than one MB for the bucket.
////                    // else probably probed with the bigger table -.-
////                    // assert(bucket_size < 1024 * 1024);
////
////                    uint8_t* sdata = new uint8_t[bucket_size + sizeof(int64_t) + rowLength]; // memory leak, fix later...
////                    memcpy(sdata, value, bucket_size);
////                    *((int64_t*)sdata) = num_rows + 1;
////                    *(((int64_t*)(sdata + bucket_size))) = rowLength;
////                    memcpy(sdata + bucket_size + sizeof(int64_t), ptr, rowLength);
////                    hashmap_put(hmap, skey, sdata);
////
////                    // check
////                    assert(calc_bucket_size(sdata) == bucket_size + sizeof(int64_t) + rowLength);
////
////                    delete [] value;
//
//struct hash_struct {
//    map_t hm;
//    uint8_t* null_bucket;
//};
//
//// note: could specialize bucket structures A LOT for better performance...
//uint8_t* extend_bucket(uint8_t* bucket, uint8_t* buf, size_t buf_size) {
//
//    // structure of bucket is as follows (varlen content)
//    // 32 bit => num elements | 32 bit => size of bucket | int32_t row length | row content
//
//    // empty bucket?
//    // ==> alloc and init with defaults!
//
//    // @TODO: maybe alloc one page?
//    if(!bucket) {
//        auto bucket_size = sizeof(int64_t) + sizeof(int32_t) + buf_size;
//        bucket = (uint8_t*)realloc(nullptr, bucket_size);
//        if(!bucket)
//            return nullptr;
//
//        uint64_t info = (0x1ul << 32ul) | bucket_size;
//
//        *(uint64_t*)bucket = info;
//        *(uint32_t*)(bucket + sizeof(int64_t)) = buf_size;
//        if(buf)
//            memcpy(bucket + sizeof(int64_t) + sizeof(int32_t), buf, buf_size);
//    } else {
//
//        uint64_t info = *(uint64_t*)bucket;
//        auto bucket_size = info & 0xFFFFFFFF;
//        auto num_elements = (info >> 32ul);
//
//        // realloc and copy contents to end of bucket...
//        bucket = (uint8_t*)realloc(bucket, bucket_size + sizeof(int32_t) + buf_size);
//
//        auto new_size = bucket_size + sizeof(int32_t) + buf_size;
//        info = ((num_elements + 1) << 32ul) | new_size;
//        *(uint64_t*)bucket = info;
//
//        *(uint32_t*)(bucket + bucket_size) = buf_size;
//        if(buf)
//            memcpy(bucket + bucket_size + sizeof(int32_t), buf, buf_size);
//    }
//
//    return bucket;
//}
//
//void hash_callback(hash_struct* hs, char* key, size_t key_size, char* buf, size_t buf_size) {
//    using namespace std;
//
//    // put into hashmap or null bucket
//    if(key) {
//        // put into hashmap!
//        uint8_t *bucket = nullptr;
//        hashmap_get(hs->hm, key, (void**)(&bucket));
//        // update or new entry
//        bucket = extend_bucket(bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
//        hashmap_put(hs->hm, key, bucket);
//    } else {
//        // goes into null bucket, no hash
//        hs->null_bucket = extend_bucket(hs->null_bucket, reinterpret_cast<uint8_t *>(buf), buf_size);
//    }
//}
//
//
//void hmw_processrow(tuplex::codegen::process_row_f functor, void *userData, tuplex::Row input) {
//
//    // serialize row to buffer
//    static const int safetyFactor = 5;
//    uint8_t *buffer = new uint8_t[safetyFactor * input.serializedLength()];
//    input.serializeToMemory(buffer, input.serializedLength() * safetyFactor / 2);
//    // call over buf
//    auto bytesWritten = functor(userData, buffer, 0, 0);
//
//    delete [] buffer;
//}
//TEST_F(PipBuilderTest, HashMapWriter) {
//    using namespace std;
//    using namespace tuplex;
//    using namespace tuplex::codegen;
//
//    auto env = make_shared<LLVMEnvironment>();
//    auto pip = make_shared<PipelineBuilder>(env, Row(option<string>(string("test"))).getRowType(), "processRow");
//
//    pip->buildWithHashmapWriter("hash", 0);
//
//    auto llvmFunc = codegen::createSingleProcessRowWrapper(*pip.get(), "execRow");
//    string funName = llvmFunc->getName();
//
//    auto ir_code = env->getIR();
//
//    // compile
//    JITCompiler compiler;
//    compiler.registerSymbol("hash", hash_callback);
//    compiler.compile(ir_code);
//
//
//    auto hm = new hash_struct;
//    hm->hm = hashmap_new();
//    hm->null_bucket = nullptr;
//
//    auto functor = (codegen::process_row_f)compiler.getAddrOfSymbol(funName);
//    string test_key = "testsdjhgdsj";
//    hmw_processrow(functor, hm, Row(option<string>(test_key)));
//    hmw_processrow(functor, hm, Row(option<string>::none));
//    hmw_processrow(functor, hm, Row(option<string>(test_key)));
//
//    // test hashmap + null bucket contents
//    ASSERT_TRUE(hm->null_bucket);
//    EXPECT_EQ(*((uint64_t*)hm->null_bucket)  >> 32ul, 1ul); // 1 element
//
//    // test bucket
//    ASSERT_TRUE(hm);
//
//    // get test key
//    char* bucket = nullptr;
//    ASSERT_EQ(MAP_OK, hashmap_get(hm->hm, (char*)test_key.c_str(), (void**)&bucket));
//    EXPECT_EQ(*((uint64_t*)bucket)  >> 32ul, 2ul); // 2 element
//
//    char* other_key = "not_in_map";
//    ASSERT_NE(MAP_OK, hashmap_get(hm->hm, other_key, (void**)&bucket));
//
//    // freeing hashmap properly
//    // free hashmap + all entries
//    hashmap_free_key_and_data(hm->hm);
//    hashmap_free(hm->hm);
//
//    // manually free null bucket
//    if(hm->null_bucket)
//        free(hm->null_bucket);
//    // freeing hashmap properly, done
//
//    // pointer of hash struct
//    delete hm;
//
//    cout<<ir_code<<endl;
//}