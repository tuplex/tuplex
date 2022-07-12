//
// Created by Leonhard Spiegelberg on 2/22/22.
//

#include "./agg_interface.h"

#include <unordered_map>

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

static std::unordered_map<std::tuple<int64_t, int64_t>, AggregateEntry> agg_map;

extern "C" int64_t init_aggregate(void* userData) {
    agg_map.clear();
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

extern "C" int64_t process_cells(void *userData, char **cells, int64_t *cell_sizes) {

    // only care about the WEATHER_DELAY cell, yet filter based on JFK airport code!
    auto s_day = cells[DAY_COLUMN_INDEX];
    auto s_month = cells[MONTH_COLUMN_INDEX];
    auto s_weather_delay = cells[WEATHER_DELAY_COLUMN_INDEX];
    auto s_airport_code = cells[AIRPORT_CODE_INDEX];

    //// parsing avoided here...
    //if(0 == strcmp(s_airport_code, "12478")) {
        // compute aggregate for current airport
        double weather_delay;

        if(0 == strlen(s_weather_delay))
            weather_delay = 0.0;
        else {
            // parse as float!
            weather_delay = atof(s_weather_delay);
        }

        // Note: could hash using string directly, i.e. delayed parsing?
        auto day = atoi(s_day);
        auto month = atoi(s_month);

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
        auto key = std::make_tuple(day, month);
        auto it = agg_map.find(key);
        if(it == agg_map.end()) {
            AggregateEntry entry;
            entry.count = 0;
            entry.mean = 0.0;
            entry.m2 = 0.0;
            agg_map[key] = entry;
        }
        auto& entry = agg_map[key];

        auto count = agg_map[key].count;
        auto mean = agg_map[key].mean;
        auto m2 = agg_map[key].m2;

        count++;
        auto delta = weather_delay - mean;
        mean += delta / count;
        auto delta2 = weather_delay - mean;
        m2 += delta * delta2;
        agg_map[key].count = count;
        agg_map[key].mean = mean;
        agg_map[key].m2 = m2;
    //}

    return 0;
}

extern "C" int64_t fetch_aggregate(void *userData, uint8_t** buf, size_t* buf_size) {
    // reconstruct year, month from key.
    // --> can specialize the hashmap based on input data!


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
    for(auto keyval : agg_map) {
        int day = std::get<0>(keyval.first);
        int month = std::get<1>(keyval.first);

        auto mean = keyval.second.mean;
        auto variance = keyval.second.m2 / keyval.second.count;
        auto std = sqrt(variance);

        // write out as beautiful string
        num_bytes = sprintf(work_buffer, "%s%d,%d,%f,%f\n", work_buffer, day, month, mean, std);
        if(strlen(work_buffer) < num_bytes) {
            work_buffer = (char*)realloc(work_buffer, num_bytes + 4096);
            num_bytes = sprintf(work_buffer, "%s%d,%d,%f,%f\n", work_buffer, day, month, mean, std);
        }
    }

    if(buf)
        *buf = reinterpret_cast<uint8_t*>(work_buffer);
    if(buf_size)
        *buf_size = num_bytes;

    agg_map.clear();

    return 0;
}