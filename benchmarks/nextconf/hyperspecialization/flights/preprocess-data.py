#!/usr/bin/env python3

# Tuplex based cleaning script
import tuplex
import time
import sys
import json
import os
import glob
import argparse

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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='preprocessign script')
    args = parser.parse_args()

    #if not 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
    #    raise Exception('Did not find AWS credentials in environment, please set.')

    lambda_size = "10000"
    lambda_threads = 1 #3
    s3_scratch_dir = "s3://tuplex-leonhard/scratch/flights-exp"
    use_hyper_specialization = False #not args.no_hyper
    use_constant_folding = False #not args.no_cf
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2003_*.csv'
    s3_output_path = 's3://tuplex-leonhard/experiments/flights_hyper'

    output_path = './preprocess/features.csv'

    # full dataset here (oO)
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_*.csv'

    # local path
    input_pattern = '/hot/data/flights_all/flights_on_time_performance_*.csv'
    
    # use following as debug pattern
    sm_map = {'A' : tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.FIRST_ROWS,
            'B': tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_ROWS,
            'C':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
            'D':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.LAST_FILE,
            'E':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES,
            'F':tuplex.dataset.SamplingMode.FIRST_ROWS | tuplex.dataset.SamplingMode.LAST_ROWS | tuplex.dataset.SamplingMode.ALL_FILES
            }

    sm = sm_map['D'] #ism_map.get(args.sampling_mode, None)
    sm = sm_map['B']

    if use_hyper_specialization:
        sm = sm_map['D']
        #sm = sm_map['A'] 
        #sm = sm | tuplex.dataset.SamplingMode.RANDOM_ROWS
    else:
        sm = sm_map['D']
        #sm = sm_map['A']

    #sm = sm | tuplex.dataset.SamplingMode.RANDOM_ROWS

    if use_hyper_specialization:
        s3_output_path += '/hyper'
    else:
        s3_output_path += '/general'

    print('>>> running {} on {} -> {}'.format('tuplex', input_pattern, s3_output_path))

    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    # run on Lambda
    conf = {"webui.enable" : False,
            "aws.lambdaMemory": lambda_size,
            "aws.lambdaThreads": lambda_threads,
            "aws.httpThreadCount": 410,
            "aws.maxConcurrency": 410,
            "scratchDor "
            'tuplex.sample.maxDetectionMemory': '256KB',
            "aws.scratchDir": s3_scratch_dir,
            "autoUpcast":True,
            "experimental.hyperspecialization": use_hyper_specialization,
            "executorCount": 0,
            "executorMemory": "2G",
            "driverMemory": "2G",
            "partitionSize": "32MB",
            "runTimeMemory": "128MB",
            "useLLVMOptimizer": True,
            "optimizer.generateParser":False, # not supported on lambda yet
            "optimizer.nullValueOptimization": True,
            "resolveWithInterpreterOnly": False,
            "optimizer.constantFoldingOptimization": use_constant_folding,
            "optimizer.selectionPushdown" : True,
            "experimental.forceBadParseExceptFormat": False}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    conf['inputSplitSize'] = '2GB' #'256MB' #'128MB'
    conf["tuplex.experimental.opportuneCompilation"] = True #False #True

    conf["optimizer.nullValueOptimization"] = True

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()
    ### QUERY HERE ###

    ctx.csv(input_pattern, sampling_mode=sm) \
       .withColumn("features", extract_feature_vector) \
       .map(assemble_row) \
       .tocsv(output_path)

    ### END QUERY ###
    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
