//
// Created by Leonhard Spiegelberg on 2/22/22.
//
// SPECIALIZED VERSION...!
#include "./agg_interface.h"

#include <chrono>

#define NUM_COLUMNS 110

// this query implements the aggregate query
// c.csv('flights*.csv')
// .mapColumn('WEATHER_DELAY', lambda x: 0 if x is None else x)
// .filter(lambda t: t['AIRPORT_CODE'] == 'JFK')
// .selectColumns(['MONTH', 'DAY', 'WEATHER_DELAY'])
// .aggregateByKey(['MONTH', 'DAY'], ['mean', 'std'])
// .collect()

// there should be some basic functions:

// hashmap using both month and year, can use fast, specialized map!
// online aggregate for both mean and std

// days are in range 0...32 for sure, years in range 1988 - 2022, so less than 255.
// months are in range 1-12
// use 256 * 32 = 8192 entries when aggregating after year/month.
// yet, use here more interesting aggregate over month/day!
// 16 * 512 -> 8192
#define MAP_ENTRIES 8192

struct AggregateEntry {
    bool in_use = false;
    int64_t count;
    double mean;
    double m2;

    AggregateEntry() : in_use(false), count(0), mean(0.0), m2(0.0) {}
};


// this is only for the fallback!
static AggregateEntry *agg_map = nullptr;
extern "C" int64_t init_aggregate(void* userData) {
    agg_map = new AggregateEntry[MAP_ENTRIES]; // this nulls everything
    return 0;
}

#define WEATHER_DELAY_COLUMN_INDEX 57

// dest airport ID
#define AIRPORT_CODE_INDEX 20

#define MONTH_COLUMN_INDEX 2
#define DAY_COLUMN_INDEX 3

// could also make the query even more interesting wrt to filter condition
// if it's international vs. domestic flights leaving from JFK?

// JFK entry:
// "12478","New York, NY: John F. Kennedy International"

// special return code if specialization fails...
#define SPECIALIZATION_FAILURE 42

// use special return code to FAIL specialization!
extern "C" int64_t process_cells(void *userData, char **cells, int64_t *cell_sizes) {

    // only care about the WEATHER_DELAY cell, yet filter based on JFK airport code!
    auto s_day = cells[DAY_COLUMN_INDEX];
    auto s_month = cells[MONTH_COLUMN_INDEX];
    auto s_weather_delay = cells[WEATHER_DELAY_COLUMN_INDEX];
    auto s_airport_code = cells[AIRPORT_CODE_INDEX];

    // parsing avoided here...
    //if(0 == strcmp(s_airport_code, "12478")) {

        // compute aggregate for current airport
        double weather_delay;

        if(0 != strlen(s_weather_delay))
            return SPECIALIZATION_FAILURE;

        // Note: could hash using string directly, i.e. delayed parsing?
        auto day = atoi(s_day);
        auto month = atoi(s_month);

        // put into aggregate
        int key = month + 16 * day;

        // using https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
        // # For a new value newValue, compute the new count, new mean, the new M2.
        //# mean accumulates the mean of the entire dataset
        //# M2 aggregates the squared distance from the mean
        //# count aggregates the number of samples seen so far
        //def update(existingAggregate, newValue):
        //    (count, mean, M2) = existingAggregate
        //    count += 1
        //    delta = newValue - mean
        //    mean += delta / count
        //    delta2 = newValue - mean
        //    M2 += delta * delta2
        //    return (count, mean, M2)

        // not in use? init!
        auto& entry = agg_map[key];
        if(!entry.in_use) {
            entry.in_use = true;
            entry.count = 0;
        } else {
            entry.count++; // this is NOT the actual count, but rather a count of how many constant inputs were encountered...
        }
   // }

    return 0;
}

extern "C" int64_t fetch_aggregate(void *userData, uint8_t** buf, size_t* buf_size) {

    // perform specialized aggregate here, i.e. need to know how many "specialized rows" were emitted. then compute
    // result of specialized code and add up.

    uint64_t num_specialized_rows = *(uint64_t*)userData;

    // if all inputs to perform update are constant, can do quick update...
    // --> it's another optimization basically.

    Timer timer;
    // reconstruct year, month from key.
    // --> can specialize the hashmap based on input data!
    // compute fast aggregate
    for(unsigned i = 0; i < MAP_ENTRIES; ++i) {
        if (agg_map[i].in_use) {
            // perform update using special code generated for constant! ideally, can propagate everything!
            // ==> smart Tuplex compiler should figure out this is basically 0.
            auto n = agg_map[i].count;
            double mean = 0.0, m2 = 0.0;
            auto count = 0;
            double weather_delay = 0;
            for(unsigned j = 0; j < n; ++j) {
                count++;
                auto delta = weather_delay - mean;
                mean += delta / count;
                auto delta2 = weather_delay - mean;
                m2 += delta * delta2;
            }
            agg_map[i].count = count;
            agg_map[i].mean = mean;
            agg_map[i].m2 = m2;
        }
    }

    std::cout<<"aggregate quick compute took: "<<timer.time()<<"s"<<std::endl;

    // # Retrieve the mean, variance and sample variance from an aggregate
    //def finalize(existingAggregate):
    //    (count, mean, M2) = existingAggregate
    //    if count < 2:
    //        return float("nan")
    //    else:
    //        (mean, variance, sampleVariance) = (mean, M2 / count, M2 / (count - 1))
    //        return (mean, variance, sampleVariance)

    auto work_buffer = (char*)malloc(4096);
    memset(work_buffer, 0, sizeof(char) * 4096);
    size_t num_bytes = 0;
    for(unsigned i = 0; i < MAP_ENTRIES; ++i) {
        if(agg_map[i].in_use) {
            // output!
            int month = i % 16;
            int day = (i / 16);

            auto mean = agg_map[i].mean;
            auto variance = agg_map[i].m2 / agg_map[i].count;
            auto std = sqrt(variance);

            // write out as beautiful string
            num_bytes = sprintf(work_buffer, "%s%d,%d,%f,%f\n", work_buffer, day, month, mean, std);
            if(strlen(work_buffer) < num_bytes) {
                work_buffer = (char*)realloc(work_buffer, num_bytes + 4096);
                num_bytes = sprintf(work_buffer, "%s%d,%d,%f,%f\n", work_buffer, day, month, mean, std);
            }
        }
    }

    if(buf)
        *buf = reinterpret_cast<uint8_t*>(work_buffer);
    if(buf_size)
        *buf_size = num_bytes;

    delete [] agg_map;

    return 0;
}