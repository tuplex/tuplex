//
// Created by Leonhard Spiegelberg on 9/19/20.
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
    struct weld_vector<int32_t> shipdates;
    struct weld_vector<double> discounts;
    struct weld_vector<double> quantities;
    struct weld_vector<double> extended_prices;
};

template<typename T>
weld_vector<T> make_weld_vector(T *data, int64_t length) {
    struct weld_vector<T> vector;
    vector.data = data;
    vector.length = length;
    return vector;
}


const char *program = "|l_shipdate: vec[i32], l_discount: vec[f64], l_quantity: vec[f64], l_ep: vec[f64]|\n"
                      "    result(for(\n"
                      "        map(\n"
                      "            filter(\n"
                      "                zip(l_shipdate, l_discount, l_quantity, l_ep), \n"
                      "                |x| x.$0 >= 19940101 && x.$0 < 19950101 && x.$1 >= 0.05 && x.$1 <= 0.07 && x.$2 < 24.0\n"
                      "            ), |x| x.$1 * x.$3\n"
                      "        ), \n"
                      "        merger[f64,+], \n"
                      "        |b,i,x| merge(b,x)\n"
                      "    ))\n"
                      "";


// all the (required) vectors
vector<int32_t> shipdates;
vector<double> discounts;
vector<double> quantities;
vector<double> extended_prices;

//         TableDefinition.Column("l_orderkey", SqlType.big_int(), NOT_NULLABLE),  0
//        TableDefinition.Column("l_partkey", SqlType.big_int(), NOT_NULLABLE),    1
//        TableDefinition.Column("l_suppkey", SqlType.big_int(), NOT_NULLABLE),     2
//        TableDefinition.Column("l_linenumber", SqlType.big_int(), NOT_NULLABLE),  3
//        TableDefinition.Column("l_quantity", SqlType.double(), NOT_NULLABLE),     4
//        TableDefinition.Column("l_extendedprice", SqlType.double(), NOT_NULLABLE),5
//        TableDefinition.Column("l_discount", SqlType.double(), NOT_NULLABLE),     6
//        TableDefinition.Column("l_tax", SqlType.double(), NOT_NULLABLE),          7
//        TableDefinition.Column("l_returnflag", SqlType.char(1), NOT_NULLABLE),    8
//        TableDefinition.Column("l_linestatus", SqlType.char(1), NOT_NULLABLE),    9
//        TableDefinition.Column("l_shipdate", SqlType.date(), NOT_NULLABLE),       10
//        TableDefinition.Column("l_commitdate", SqlType.date(), NOT_NULLABLE),     11
//        TableDefinition.Column("l_receiptdate", SqlType.date(), NOT_NULLABLE),    12
//        TableDefinition.Column("l_shipinstruct", SqlType.char(25), NOT_NULLABLE),
//        TableDefinition.Column("l_shipmode", SqlType.char(10), NOT_NULLABLE),
//        TableDefinition.Column("l_comment", SqlType.varchar(44), NOT_NULLABLE)
static int QUANTITY_IDX = 4;
static int EXTENDED_PRICE_IDX = 5;
static int DISCOUNT_IDX = 6;
static int SHIPDATE_IDX = 10;

int main(int argc, char **argv) {

    // cli11 quick parse for file, cf. https://cliutils.gitlab.io/CLI11Tutorial/
    CLI::App app("Weld TPC-H Q6");

    string data_path = "";
    bool preprocessed = false;
    app.add_option("-p,--path", data_path, "lineitem.tbl data path")
            ->required()
            ->check(CLI::ExistingFile);
    app.add_flag("--preprocessed", preprocessed, "whether to use 4 column input or original dbgen file");

    CLI11_PARSE(app, argc, argv);

    cout << "data path is: " << data_path << endl;

    if (preprocessed) {
        QUANTITY_IDX = 0;
        EXTENDED_PRICE_IDX = 1;
        DISCOUNT_IDX = 2;
        SHIPDATE_IDX = 3;
    }

    Timer timer;
    // parse file
    auto fd = open(data_path.c_str(), O_RDONLY);
    csvmonkey::FdStreamCursor stream(fd);
    csvmonkey::CsvReader<csvmonkey::FdStreamCursor> reader(stream, '|');

    while (reader.read_row()) {
        auto &cursor = reader.row();

        if (!preprocessed) {
            auto raw_date = cursor.cells[SHIPDATE_IDX].as_str();
            auto date_str = raw_date.substr(0, 4) + raw_date.substr(5, 2) + raw_date.substr(8, 2);
            shipdates.emplace_back(stoi(date_str));
        } else {
            shipdates.emplace_back(cursor.cells[SHIPDATE_IDX].as_int());
        }

        // parse the others
        discounts.emplace_back(cursor.cells[DISCOUNT_IDX].as_double());
        quantities.emplace_back(cursor.cells[QUANTITY_IDX].as_double());
        extended_prices.emplace_back(cursor.cells[EXTENDED_PRICE_IDX].as_double());
    }

    close(fd);
    double load_time = timer.time();
    cout << "Loading CSV took: " << load_time << "s" << endl;
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

    auto num_rows = shipdates.size();
    Lineitem tbl;
    tbl.shipdates = make_weld_vector<int32_t>(shipdates.data(), num_rows);
    tbl.discounts = make_weld_vector<double>(discounts.data(), num_rows);
    tbl.quantities = make_weld_vector<double>(quantities.data(), num_rows);
    tbl.extended_prices = make_weld_vector<double>(extended_prices.data(), num_rows);

    weld_value_t weld_lineitem = weld_value_new(&tbl);

    conf = weld_conf_new();
    weld_conf_set(conf, "weld.memory.limit", "100000000000");
    auto ctx = weld_context_new(conf);
    weld_value_t result = weld_module_run(m, ctx, weld_lineitem, e);
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
    weld_value_free(weld_lineitem);
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
