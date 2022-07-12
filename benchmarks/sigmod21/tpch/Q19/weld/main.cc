//
// Created by Rahul Yesantharao on 2/02/20.
//

#include <iostream>
#include <string>
#include <weld.h>

#include "CLI11.hpp"
#include "csvmonkey.h"
#include "timer.h"

using namespace std;

// @TODO: need to load FULL data, so it's fair to the other frameworks...
template<typename T>
struct weld_vector {
    T *data;
    int64_t length;
};


struct Lineitem {
    int64_t l_partkey;
    double l_quantity;
    double l_extendedprice;
    double l_discount;
    int64_t l_shipinstruct;
    int64_t l_shipmode;

    Lineitem(int64_t l_partkey_, double l_quantity_, double l_extendedprice_,
             double l_discount_, int64_t l_shipinstruct_, int64_t l_shipmode_)
        : l_partkey(l_partkey_), l_quantity(l_quantity_),
          l_extendedprice(l_extendedprice_), l_discount(l_discount_),
          l_shipinstruct(l_shipinstruct_), l_shipmode(l_shipmode_) {}
};

struct Part {
    int64_t p_partkey;
    int64_t p_brand;
    int64_t p_size;
    int64_t p_container;

    Part(int64_t p_partkey_, int64_t p_brand_, int64_t p_size_,
         int64_t p_container_)
       : p_partkey(p_partkey_), p_brand(p_brand_), p_size(p_size_),
         p_container(p_container_) {}
};

struct Argument {
  struct weld_vector<struct Lineitem> lineitem;
  struct weld_vector<struct Part> part;
};

template<typename T>
weld_vector<T> make_weld_vector(T *data, int64_t length) {
    struct weld_vector<T> vector;
    vector.data = data;
    vector.length = length;
    return vector;
}


const char *program = "|lineitem: vec[{i64, f64, f64, f64, i64, i64}], part: vec[{i64, i64, i64, i64}]|\n"
                      "    # build mapping {p_partkey : joined row }\n"
                      "    let part_by_partkey = result(for(\n"
                      "        part,\n"
                      "        groupmerger[i64,{i64,i64,i64}], \n"
                      "        |b,i,x| merge(b, {x.$0, {x.$1, x.$2, x.$3}})\n"
                      "    ));\n"
                      "    let join_vec = flatten(result(for(\n"
                      "        lineitem,\n"
                      "        appender[vec[{i64,i64,i64,i64,f64,f64,f64,i64,i64}]],\n"
                      "        |b,i,x| let vals = result(for(\n"
                      "          lookup(part_by_partkey, x.$0),\n"
                      "          appender[{i64,i64,i64,i64,f64,f64,f64,i64,i64}],\n"
                      "          |b,i,p| merge(b, {p.$0,p.$1,p.$2,x.$0,x.$1,x.$2,x.$3,x.$4,x.$5})\n"
                      "        ));\n"
                      "        merge(b, vals)\n"
                      "    )));\n"
                      "    result(for(\n"
                      "        map(\n"
                      "            filter(\n"
                      "                join_vec,\n"
                      "                |j| let p_brand = j.$0;\n"
                      "                let p_size = j.$1;\n"
                      "                let p_container = j.$2;\n"
                      "                let l_quantity = j.$4;\n"
                      "                let l_extendedprice = j.$5;\n"
                      "                let l_discount = j.$6;\n"
                      "                let l_shipinstruct = j.$7;\n"
                      "                let l_shipmode = j.$8;\n"
                      "                (p_brand == 12l &&\n"
                      "                 (p_container == 18l || p_container == 31l || p_container == 25l || p_container == 4l) &&\n"
                      "                 (l_quantity >= 1.0 && l_quantity <= 11.0) &&\n"
                      "                 (p_size >= 1l && p_size <= 5l) &&\n"
                      "                 (l_shipinstruct == 0l) &&\n"
                      "                 (l_shipmode == 7l || l_shipmode == 3l)\n"
                      "                ) ||\n"
                      "                (p_brand == 23l &&\n"
                      "                 (p_container == 5l || p_container == 38l || p_container == 19l || p_container == 13l) &&\n"
                      "                 (l_quantity >= 10.0 && l_quantity <= 20.0) &&\n"
                      "                 (p_size >= 1l && p_size <= 10l) &&\n"
                      "                 (l_shipinstruct == 0l) &&\n"
                      "                 (l_shipmode == 7l || l_shipmode == 3l)\n"
                      "                ) ||\n"
                      "                (p_brand == 34l &&\n"
                      "                 (p_container == 1l || p_container == 14l || p_container == 29l || p_container == 21l) &&\n"
                      "                 (l_quantity >= 20.0 && l_quantity <= 30.0) &&\n"
                      "                 (p_size >= 1l && p_size <= 15l) &&\n"
                      "                 (l_shipinstruct == 0l) &&\n"
                      "                 (l_shipmode == 7l || l_shipmode == 3l)\n"
                      "                )\n"
                      "            ), |j| j.$5 * (1.0 - j.$6)\n"
                      "        ), \n"
                      "        merger[f64,+], \n"
                      "        |b,i,x| merge(b,x)\n"
                      "    ))\n"
                      "";

// all the (required) vectors
vector<struct Lineitem> lineitem;
vector<struct Part> part;

static int L_PARTKEY_IDX = 1;
static int L_QUANTITY_IDX = 4;
static int L_EXTENDEDPRICE_IDX = 5;
static int L_DISCOUNT_IDX = 6;
static int L_SHIPINSTRUCT_IDX = 13;
static int L_SHIPMODE_IDX = 14;

static int P_PARTKEY_IDX = 0;
static int P_BRAND_IDX = 3;
static int P_SIZE_IDX = 5;
static int P_CONTAINER_IDX = 6;

int main(int argc, char **argv) {

    // cli11 quick parse for file, cf. https://cliutils.gitlab.io/CLI11Tutorial/
    CLI::App app("Weld TPC-H Q19");

    string lineitem_path = "";
    string part_path = "";
    bool preprocessed = false;
    app.add_option("--lineitem_path", lineitem_path, "lineitem.tbl data path")
            ->required()
            ->check(CLI::ExistingFile);
    app.add_option("--part_path", part_path, "part.tbl data path")
            ->required()
            ->check(CLI::ExistingFile);
    app.add_flag("--preprocessed", preprocessed, "whether to use 4 column input or original dbgen file");

    CLI11_PARSE(app, argc, argv);

    if (preprocessed) {
        L_PARTKEY_IDX = 0;
        L_QUANTITY_IDX = 1;
        L_EXTENDEDPRICE_IDX = 2;
        L_DISCOUNT_IDX = 3;
        L_SHIPINSTRUCT_IDX = 4;
        L_SHIPMODE_IDX = 5;

        P_PARTKEY_IDX = 0;
        P_BRAND_IDX = 1;
        P_SIZE_IDX = 2;
        P_CONTAINER_IDX = 3;
    }

    Timer timer;
    // parse lineitem file
    auto fd = open(lineitem_path.c_str(), O_RDONLY);
    csvmonkey::FdStreamCursor stream(fd);
    csvmonkey::CsvReader<csvmonkey::FdStreamCursor> reader(stream, '|');

    while (reader.read_row()) {
        auto &cursor = reader.row();

        if (!preprocessed) {
            throw std::runtime_error("C++ preprocessing not implemented yet! Use [preprocess.py --weld]"); 
        } else {
          auto l_partkey = cursor.cells[L_PARTKEY_IDX].as_int();
          auto l_quantity = cursor.cells[L_QUANTITY_IDX].as_double();
          auto l_extendedprice = cursor.cells[L_EXTENDEDPRICE_IDX].as_double();
          auto l_discount = cursor.cells[L_DISCOUNT_IDX].as_double();
          auto l_shipinstruct = cursor.cells[L_SHIPINSTRUCT_IDX].as_int();
          auto l_shipmode = cursor.cells[L_SHIPMODE_IDX].as_int();
          lineitem.emplace_back(l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode);
        }
    }
    close(fd);

    // parse part file
    auto fd1 = open(part_path.c_str(), O_RDONLY);
    csvmonkey::FdStreamCursor stream1(fd1);
    csvmonkey::CsvReader<csvmonkey::FdStreamCursor> reader1(stream1, '|');

    while (reader1.read_row()) {
        auto &cursor = reader1.row();


        if (!preprocessed) {
            throw std::runtime_error("C++ preprocessing not implemented yet! Use [preprocess.py --weld]"); 
        } else {
            auto p_partkey = cursor.cells[P_PARTKEY_IDX].as_int();
            auto p_brand = cursor.cells[P_BRAND_IDX].as_int();
            auto p_size = cursor.cells[P_SIZE_IDX].as_int();
            auto p_container = cursor.cells[P_CONTAINER_IDX].as_int();
            part.emplace_back(p_partkey, p_brand, p_size, p_container);
        }
    }
    close(fd1);

    double load_time = timer.time();
    cout << "Loading CSVs took: " << load_time << "s" << endl;
    timer.reset();

    // convert to Weld
    weld_error_t e = weld_error_new();
    weld_conf_t conf = weld_conf_new();
    weld_conf_set(conf, "weld.memory.limit", "100000000000");

    weld_module_t m = weld_module_compile(program, conf, e);
    weld_conf_free(conf);
    if (weld_error_code(e)) {
        const char *err = weld_error_message(e);
        cerr << "Error message: " << err << endl;
        exit(1);
    }

    double compile_time = timer.time();
    cout << "Compiling the Weld module took: " << compile_time << "s" << endl;
    timer.reset();


    // make input tables
    Argument arg;
    arg.lineitem = make_weld_vector<struct Lineitem> (lineitem.data(), lineitem.size());
    arg.part = make_weld_vector<struct Part> (part.data(), part.size());
    weld_value_t weld_arg = weld_value_new(&arg);

    conf = weld_conf_new();
    weld_conf_set(conf, "weld.memory.limit", "100000000000");
    auto ctx = weld_context_new(conf);
    weld_value_t result = weld_module_run(m, ctx, weld_arg, e);
    if (weld_error_code(e)) {
        const char *err = weld_error_message(e);
        cerr << "Error message: " << err << endl;
        exit(1);
    }

    auto *result_data = (double *) weld_value_data(result);
    double final_result = *result_data;

    cout << fixed << "Result::\n" << final_result << endl;

    // Free the values.
    weld_value_free(result);
    weld_value_free(weld_arg);
    weld_context_free(ctx);
    weld_conf_free(conf);

    weld_error_free(e);
    weld_module_free(m);

    double query_time = timer.time();
    cout << "Weld query took: " << query_time << "s" << endl;

    cout << endl;

    // nice parseable representation of the numbers
    cout << fixed << "framework,load,compile,query,total,result\n"
         << "weld," << load_time << "," << compile_time << "," << query_time << ","
         << load_time + compile_time + query_time << "," << final_result << endl;
    return 0;
}
