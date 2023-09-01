#!/usr/bin/env python3
import shutil

from pyspark.sql.functions import col, stddev, mean
from pyspark.sql.types import DoubleType


def extract_feature_vector(row):
    carrier_list = [None, 'EA', 'UA', 'PI', 'NK', 'PS', 'AA', 'NW', 'EV', 'B6', 'HP', 'TW', 'DL', 'OO', 'F9', 'YV',
                    'TZ', 'US',
                    'MQ', 'OH', 'HA', 'ML (1)', 'XE', 'G4', 'YX', 'DH', 'AS', 'KH', 'QX', 'CO', 'FL', 'VX', 'PA (1)',
                    'WN', '9E']

    airport_list = [None, 'ABE', 'ABI', 'ABQ', 'ABR', 'ABY', 'ACK', 'ACT', 'ACV', 'ACY', 'ADK', 'ADQ', 'AEX', 'AGS',
                    'AKN',
                    'ALB', 'ALO', 'ALS', 'ALW', 'AMA', 'ANC', 'ANI', 'APF', 'APN', 'ART', 'ASE', 'ATL', 'ATW', 'ATY',
                    'AUS', 'AVL', 'AVP', 'AZA', 'AZO', 'BDL', 'BET', 'BFF', 'BFI', 'BFL', 'BFM', 'BGM', 'BGR', 'BHM',
                    'BIL', 'BIS', 'BJI', 'BKG', 'BLI', 'BLV', 'BMI', 'BNA', 'BOI', 'BOS', 'BPT', 'BQK', 'BQN', 'BRD',
                    'BRO', 'BRW', 'BTM', 'BTR', 'BTV', 'BUF', 'BUR', 'BWI', 'BZN', 'CAE', 'CAK', 'CBM', 'CCR', 'CDB',
                    'CDC', 'CDV', 'CEC', 'CGI', 'CHA', 'CHO', 'CHS', 'CIC', 'CID', 'CIU', 'CKB', 'CLD', 'CLE', 'CLL',
                    'CLT', 'CMH', 'CMI', 'CMX', 'CNY', 'COD', 'COS', 'COU', 'CPR', 'CRP', 'CRW', 'CSG', 'CVG', 'CWA',
                    'CYS', 'DAB', 'DAL', 'DAY', 'DBQ', 'DCA', 'DDC', 'DEC', 'DEN', 'DET', 'DFW', 'DHN', 'DIK', 'DLG',
                    'DLH', 'DRO', 'DRT', 'DSM', 'DTW', 'DUT', 'DVL', 'EAR', 'EAT', 'EAU', 'ECP', 'EFD', 'EGE', 'EKO',
                    'ELM', 'ELP', 'ENV', 'ERI', 'ESC', 'EUG', 'EVV', 'EWN', 'EWR', 'EYW', 'FAI', 'FAR', 'FAT', 'FAY',
                    'FCA', 'FLG', 'FLL', 'FLO', 'FMN', 'FNL', 'FNT', 'FOD', 'FOE', 'FSD', 'FSM', 'FWA', 'GCC', 'GCK',
                    'GCN', 'GEG', 'GFK', 'GGG', 'GJT', 'GLH', 'GNV', 'GPT', 'GRB', 'GRI', 'GRK', 'GRR', 'GSO', 'GSP',
                    'GST', 'GTF', 'GTR', 'GUC', 'GUM', 'HDN', 'HGR', 'HHH', 'HIB', 'HKY', 'HLN', 'HNL', 'HOB', 'HOU',
                    'HPN', 'HRL', 'HSV', 'HTS', 'HVN', 'HYA', 'HYS', 'IAD', 'IAG', 'IAH', 'ICT', 'IDA', 'IFP', 'ILE',
                    'ILG', 'ILM', 'IMT', 'IND', 'INL', 'IPL', 'IPT', 'ISN', 'ISO', 'ISP', 'ITH', 'ITO', 'IYK', 'JAC',
                    'JAN', 'JAX', 'JFK', 'JLN', 'JMS', 'JNU', 'JST', 'KOA', 'KSM', 'KTN', 'LAN', 'LAR', 'LAS', 'LAW',
                    'LAX', 'LBB', 'LBE', 'LBF', 'LBL', 'LCH', 'LCK', 'LEX', 'LFT', 'LGA', 'LGB', 'LIH', 'LIT', 'LMT',
                    'LNK', 'LNY', 'LRD', 'LSE', 'LWB', 'LWS', 'LYH', 'MAF', 'MAZ', 'MBS', 'MCI', 'MCN', 'MCO', 'MCW',
                    'MDT', 'MDW', 'MEI', 'MEM', 'MFE', 'MFR', 'MGM', 'MHK', 'MHT', 'MIA', 'MIB', 'MKC', 'MKE', 'MKG',
                    'MKK', 'MLB', 'MLI', 'MLU', 'MMH', 'MOB', 'MOD', 'MOT', 'MQT', 'MRY', 'MSN', 'MSO', 'MSP', 'MSY',
                    'MTH', 'MTJ', 'MVY', 'MWH', 'MYR', 'OAJ', 'OAK', 'OGD', 'OGG', 'OGS', 'OKC', 'OMA', 'OME', 'ONT',
                    'ORD', 'ORF', 'ORH', 'OTH', 'OTZ', 'OWB', 'OXR', 'PAE', 'PAH', 'PBG', 'PBI', 'PDX', 'PFN', 'PGD',
                    'PGV', 'PHF', 'PHL', 'PHX', 'PIA', 'PIB', 'PIE', 'PIH', 'PIR', 'PIT', 'PLN', 'PMD', 'PNS', 'PPG',
                    'PRC', 'PSC', 'PSE', 'PSG', 'PSM', 'PSP', 'PUB', 'PUW', 'PVD', 'PVU', 'PWM', 'RAP', 'RCA', 'RDD',
                    'RDM', 'RDR', 'RDU', 'RFD', 'RHI', 'RIC', 'RIW', 'RKS', 'RNO', 'ROA', 'ROC', 'ROP', 'ROR', 'ROW',
                    'RST', 'RSW', 'SAF', 'SAN', 'SAT', 'SAV', 'SBA', 'SBN', 'SBP', 'SCC', 'SCE', 'SCK', 'SDF', 'SEA',
                    'SFB', 'SFO', 'SGF', 'SGU', 'SHD', 'SHR', 'SHV', 'SIT', 'SJC', 'SJT', 'SJU', 'SKA', 'SLC', 'SLE',
                    'SLN', 'SMF', 'SMX', 'SNA', 'SOP', 'SPI', 'SPN', 'SPS', 'SRQ', 'STC', 'STL', 'STS', 'STT', 'STX',
                    'SUN', 'SUX', 'SWF', 'SWO', 'SYR', 'TBN', 'TEX', 'TKI', 'TLH', 'TOL', 'TPA', 'TRI', 'TTN', 'TUL',
                    'TUP', 'TUS', 'TVC', 'TVL', 'TWF', 'TXK', 'TYR', 'TYS', 'UCA', 'UIN', 'USA', 'UST', 'UTM', 'VCT',
                    'VEL', 'VIS', 'VLD', 'VPS', 'WRG', 'WYS', 'XNA', 'XWA', 'YAK', 'YAP', 'YKM', 'YNG', 'YUM']

    state_list = [None, 'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
                  'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky',
                  'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
                  'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico',
                  'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania',
                  'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas',
                  'U.S. Pacific Trust Territories and Possessions', 'U.S. Virgin Islands', 'Utah', 'Vermont',
                  'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

    # categorical variables
    quarter = row['QUARTER']
    month = row['MONTH']
    day_of_month = row['DAY_OF_MONTH']
    day_of_week = row['DAY_OF_WEEK']
    carrier = carrier_list.index(row['OP_UNIQUE_CARRIER'])
    origin_airport = airport_list.index(row['ORIGIN'])
    dest_airport = airport_list.index(row['DEST'])

    origin_state = state_list.index(row['ORIGIN_STATE_NM'])
    dest_state = state_list.index(row['DEST_STATE_NM'])

    # numerical variables
    dep_delay = row['DEP_DELAY']
    arr_delay = row['ARR_DELAY']

    crs_arr_hour = float(int(row['CRS_ARR_TIME']) // 100)
    crs_dep_hour = float(int(row['CRS_DEP_TIME']) // 100)
    crs_arr_5min = float(int(row['CRS_ARR_TIME']) % 100 // 5)
    crs_dep_5min = float(int(row['CRS_DEP_TIME']) % 100 // 5)

    features = [float(quarter), float(month), float(day_of_month),
                float(day_of_week), float(carrier), float(origin_state),
                float(dest_state), dep_delay, arr_delay,
                crs_arr_hour, crs_dep_hour,
                crs_arr_5min, crs_dep_5min]

    return features

def assemble_row(row):
    x = row['features']

    return {"f_quarter": x[0],
            "f_month": x[1],
            "f_day_of_month": x[2],
            "f_day_of_week": x[3],
            "f_carrier": x[4],
            "f_origin_state": x[5],
            "f_dest_state": x[6],
            "f_dep_delay": x[7],
            "f_arr_delay": x[8],
            "f_crs_arr_hour": x[9],
            "f_crs_dep_hour": x[10],
            "f_crs_arr_5min": x[11],
            "f_crs_dep_5min": x[12],
            "t_carrier_delay": row['CARRIER_DELAY'],
            "t_weather_delay": row['WEATHER_DELAY'],
            "t_nas_delay": row['NAS_DELAY'],
            "t_security_delay": row['SECURITY_DELAY'],
            "t_late_aircraft_delay": row['LATE_AIRCRAFT_DELAY']}

import pyspark
from pyspark.sql import SparkSession
import json
import shutil

from pyspark.ml.feature import VectorAssembler

def main():
    input_pattern = '/hot/data/flights_all/flights_on_time_performance_*.csv'
    output_path_json = './preprocess/spark/json/features.json'
    output_path = './preprocess/spark/csv/features.csv'

    spark = SparkSession.builder.master("local[*]").appName("Flights - preprocess").getOrCreate()

    # df = spark.read.csv(input_pattern, header=True).rdd
    #
    # def extract_as_dict(row):
    #     out_row = row.asDict()
    #     out_row['features'] = extract_feature_vector(row)
    #     return out_row
    #
    # shutil.rmtree(output_path_json)
    # df.map(extract_as_dict).map(assemble_row).map(json.dumps).saveAsTextFile(output_path_json)

    df = spark.read.csv(
        '/home/leonhards/projects/tuplex-public/benchmarks/nextconf/hyperspecialization/flights/preprocess/features.part0.csv',
        header=True)

    # drop null values for features to predict
    df = df.filter(~df.t_nas_delay.isNull() & ~df.t_carrier_delay.isNull() & ~df.t_weather_delay.isNull() & ~df.t_nas_delay.isNull() & ~df.t_security_delay.isNull() & ~df.t_late_aircraft_delay.isNull())

    num_entries = df.count()

    print('Found {} rows to build linear reg model on'.format(num_entries))

    # create lin reg model for one variable t_nas_delay
    # use this here: https://anujsyal.com/introduction-to-pyspark-ml-lib-build-your-first-linear-regression-model
    columns = ['f_quarter', 'f_month', 'f_day_of_month', 'f_day_of_week', 'f_carrier', 'f_origin_state', 'f_dest_state', 'f_dep_delay', 'f_arr_delay', 'f_crs_arr_hour', 'f_crs_dep_hour', 'f_crs_arr_5min', 'f_crs_dep_5min', 't_carrier_delay', 't_weather_delay', 't_nas_delay', 't_security_delay', 't_late_aircraft_delay']
    feature_columns = ['f_quarter', 'f_month', 'f_day_of_month', 'f_day_of_week', 'f_carrier', 'f_origin_state', 'f_dest_state', 'f_dep_delay', 'f_arr_delay', 'f_crs_arr_hour', 'f_crs_dep_hour', 'f_crs_arr_5min', 'f_crs_dep_5min']

    for name in columns:
        df = df.withColumn(name, col(name).cast(DoubleType()))
    # df = df.cache()

    # df.write.mode("overwrite").option('header', True).csv('./preprocess/spark/csv/features.csv')

    df.printSchema()

    # normalize features
    normalize_columns = ['f_quarter', 'f_month', 'f_day_of_month', 'f_day_of_week', 'f_dep_delay', 'f_arr_delay', 'f_crs_arr_hour', 'f_crs_dep_hour', 'f_crs_arr_5min', 'f_crs_dep_5min']
    normalize_dict = {}
    for name in normalize_columns:
        normalize_dict[name] = df.select(mean(col(name)).alias('mean'), stddev(col(name)).alias('std')).collect()[0]
        print(normalize_dict[name])
        mu = normalize_dict[name]['mean']
        std = normalize_dict[name]['std']
        print('column {}: mu={} std={}'.format(name, mu, std))
        df = df.withColumn(name, (col(name) - mu) / std)

    target_var = 't_nas_delay'
    target_var = 't_carrier_delay'
    assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
    data_set = assembler.transform(df)
    final_data = data_set.select(['features', target_var])
    final_data.show(2)

    train_data, test_data = final_data.randomSplit([0.7, 0.3])

    train_data.describe().show()
    test_data.describe().show()


    from pyspark.ml.regression import LinearRegression

    lr = LinearRegression(labelCol=target_var)
    lrModel = lr.fit(train_data)

    test_stats = lrModel.evaluate(test_data)
    print(f"RMSE: {test_stats.rootMeanSquaredError}")
    print(f"R2: {test_stats.r2}")
    print(f"R2: {test_stats.meanSquaredError}")
    print('Coefficients: ' + str(lrModel.coefficients))
    print('Intercept: ' + str(lrModel.intercept))

if __name__ == '__main__':
    main()