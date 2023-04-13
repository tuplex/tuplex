#!/usr/bin/env python3

# Tuplex based cleaning script
import tuplex
import time
import sys
import json
import os
import glob
import argparse

# note: this doesn't work yet! Fix in tuplex
# from udfs import *

def make_year_range(num_years):
    """
    helper function to create year range for year based experiment
    """
    if num_years <= 1:
        return [2003]
    start_year = max(1987, 2003 - (num_years - 1) // 2)
    end_year = start_year + num_years

    years = list(range(start_year, end_year))[:num_years]
    assert len(years) == num_years and 2003 in years
    return years

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

    # invalid -> return 0-based feature vector.
    if row['CRS_ARR_TIME'] is None or row['CRS_DEP_TIME'] is None:
        return [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

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

def fill_in_delays(row):
    # want to fill in data for missing carrier_delay, weather delay etc.
    # only need to do that prior to 2003/06

    year = row['YEAR']
    month = row['MONTH']
    arr_delay = row['ARR_DELAY']

    if year == 2003 and month < 6 or year < 2003:
        # fill in delay breakdown using model and complex logic
        if arr_delay is None:
            # stays None, because flight arrived early
            # if diverted though, need to add everything to div_arr_delay
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': None,
                    'carrier_delay' : None,
                    'weather_delay': None,
                    'nas_delay' : None,
                    'security_delay': None,
                    'late_aircraft_delay' : None}
        elif arr_delay < 0.:
            # stays None, because flight arrived early
            # if diverted though, need to add everything to div_arr_delay
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': row['ARR_DELAY'],
                    'carrier_delay' : None,
                    'weather_delay': None,
                    'nas_delay' : None,
                    'security_delay': None,
                    'late_aircraft_delay' : None}
        elif arr_delay < 5.:
            # it's an ontime flight, just attribute any delay to the carrier
            carrier_delay = arr_delay
            # set the rest to 0
            # ....
            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': row['ARR_DELAY'],
                    'carrier_delay' : carrier_delay,
                    'weather_delay': None,
                    'nas_delay' : None,
                    'security_delay': None,
                    'late_aircraft_delay' : None}
        else:
            # use model to determine everything and set into (join with weather data?)
            # i.e., extract here a couple additional columns & use them for features etc.!

            # use here linear model
            # (for now random numbers, but can easily fit some from the data)
            # features is a list of 13 values.
            f = row['features']
            mu = [ 10.34020663,  -5.32110325, -23.76501655,  -9.9471319 ,
                   11.08306402,  28.44419468,  34.33795322,  -3.5271869 ,
                   14.31617576,  -2.37601258,   3.10218378,  31.74930251,
                   30.65958678]

            std = [ 6.14500234, 15.43086255,  3.95305606,  8.95857769,  5.82440163,
                    10.0310225 ,  8.20700403,  8.16668611,  3.34533295,  9.30209542,
                    7.54420077,  0.17705991, 13.322939  ]
            normalized_f = [(f[0] - mu[0]) / std[0], (f[1] - mu[1]) / std[1], (f[2] - mu[2]) / std[2], (f[3] - mu[3]) / std[3], (f[4] - mu[4]) / std[4], (f[5] - mu[5]) / std[5], (f[6] - mu[6]) / std[6], (f[7] - mu[7]) / std[7], (f[8] - mu[8]) / std[8], (f[9] - mu[9]) / std[9], (f[10] - mu[10]) / std[10], (f[11] - mu[11]) / std[11], (f[12] - mu[12]) / std[12]]

            normalized_t_carrier_delay = normalized_f[0] * 1.12 + normalized_f[1] * 4.49 + normalized_f[2] * 3.42 + normalized_f[3] * 4.28 + normalized_f[4] * 0.81 + normalized_f[5] * 5.13 + normalized_f[6] * 5.26 + normalized_f[7] * -0.07 + normalized_f[8] * -1.39 + normalized_f[9] * 2.74 + normalized_f[10] * -2.93 + normalized_f[11] * 4.81 + normalized_f[12] * 4.36
            t_carrier_delay = normalized_t_carrier_delay * 4.06 + 1.48

            normalized_t_weather_delay = normalized_f[0] * 1.84 + normalized_f[1] * -1.92 + normalized_f[2] * 2.59 + normalized_f[3] * -1.49 + normalized_f[4] * 9.29 + normalized_f[5] * 7.88 + normalized_f[6] * 1.11 + normalized_f[7] * 3.87 + normalized_f[8] * 0.57 + normalized_f[9] * 4.32 + normalized_f[10] * 7.65 + normalized_f[11] * -4.69 + normalized_f[12] * 0.07
            t_weather_delay = normalized_t_weather_delay * -5.07 + 5.95

            normalized_t_nas_delay = normalized_f[0] * -3.33 + normalized_f[1] * 9.12 + normalized_f[2] * 2.64 + normalized_f[3] * 0.05 + normalized_f[4] * 2.51 + normalized_f[5] * -3.30 + normalized_f[6] * -1.44 + normalized_f[7] * -2.38 + normalized_f[8] * 6.60 + normalized_f[9] * 2.86 + normalized_f[10] * 2.74 + normalized_f[11] * 3.00 + normalized_f[12] * 2.61
            t_nas_delay = normalized_t_nas_delay * 5.90 + 5.87

            normalized_t_security_delay = normalized_f[0] * 4.25 + normalized_f[1] * -0.68 + normalized_f[2] * 3.40 + normalized_f[3] * 5.63 + normalized_f[4] * 3.57 + normalized_f[5] * 3.19 + normalized_f[6] * 2.90 + normalized_f[7] * 1.43 + normalized_f[8] * 3.86 + normalized_f[9] * 4.24 + normalized_f[10] * 10.60 + normalized_f[11] * 0.84 + normalized_f[12] * 2.97
            t_security_delay = normalized_t_security_delay * 0.11 + 8.68

            normalized_t_late_aircraft_delay = normalized_f[0] * 2.89 + normalized_f[1] * -1.12 + normalized_f[2] * 1.52 + normalized_f[3] * 3.10 + normalized_f[4] * 4.74 + normalized_f[5] * -0.97 + normalized_f[6] * 1.47 + normalized_f[7] * 8.53 + normalized_f[8] * 0.63 + normalized_f[9] * 6.67 + normalized_f[10] * 4.44 + normalized_f[11] * 3.12 + normalized_f[12] * 2.92
            t_late_aircraft_delay = normalized_t_late_aircraft_delay * -4.50 + 1.04

            # the arrival delay is more than 5min, now scale such that numbers add up!
            s_factor = t_carrier_delay + t_weather_delay + t_nas_delay + t_security_delay + t_late_aircraft_delay
            if abs(s_factor) < 0.001:
                s_factor = 1.0
            s_factor = arr_delay / s_factor
            t_carrier_delay *= s_factor
            t_weather_delay *= s_factor
            t_nas_delay *= s_factor
            t_security_delay *= s_factor
            t_late_aircraft_delay *= s_factor

            return {'year' : year, 'month' : month,
                    'day' : row['DAY_OF_MONTH'],
                    'carrier': row['OP_UNIQUE_CARRIER'],
                    'flightno' : row['OP_CARRIER_FL_NUM'],
                    'origin': row['ORIGIN_AIRPORT_ID'],
                    'dest': row['DEST_AIRPORT_ID'],
                    'distance' : row['DISTANCE'],
                    'dep_delay' : row['DEP_DELAY'],
                    'arr_delay': row['ARR_DELAY'],
                    'carrier_delay' : t_carrier_delay,
                    'weather_delay': t_weather_delay,
                    'nas_delay' : t_nas_delay,
                    'security_delay': t_security_delay,
                    'late_aircraft_delay' : t_late_aircraft_delay}
    else:
        # just return it as is
        return {'year' : year, 'month' : month,
                'day' : row['DAY_OF_MONTH'],
                'carrier': row['OP_UNIQUE_CARRIER'],
                'flightno' : row['OP_CARRIER_FL_NUM'],
                'origin': row['ORIGIN_AIRPORT_ID'],
                'dest': row['DEST_AIRPORT_ID'],
                'distance' : row['DISTANCE'],
                'dep_delay' : row['DEP_DELAY'],
                'arr_delay': row['ARR_DELAY'],
                'carrier_delay' : row['CARRIER_DELAY'],
                'weather_delay':row['WEATHER_DELAY'],
                'nas_delay' : row['NAS_DELAY'],
                'security_delay': row['SECURITY_DELAY'],
                'late_aircraft_delay' : row['LATE_AIRCRAFT_DELAY']}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Flights hyper specialization query')
    parser.add_argument('--no-hyper', dest='no_hyper', action="store_true",
                        help="deactivate hyperspecialization optimization explicitly.")
    parser.add_argument('--no-cf', dest='no_cf', action="store_true",
                        help="deactivate constant-folding optimization explicitly.")
    parser.add_argument('--no-nvo', dest='no_nvo', action="store_true",
                        help="deactivate null value optimization explicitly.")
    parser.add_argument('--python-mode', dest='python_mode', action="store_true",
                        help="process in pure python mode.")
    parser.add_argument('--internal-fmt', dest='use_internal_fmt',
                        help='if active, use the internal tuplec storage format for exceptions, no CSV format optimization',
                        action='store_true')
    parser.add_argument('--samples-per-strata', dest='samples_per_strata', default=10, help='how many samples to use per strata')
    parser.add_argument('--strata-size', dest='strata_size', default=1024,
                        help='how many samples to use per strata')
    parser.add_argument('--num-years', dest='num_years', action='store', choices=['auto'] + [str(year) for year in list(range(1, 2021-1987+2))], default='auto', help='if auto the range 2002-2005 will be used (equivalent to --num-years=4).')
    args = parser.parse_args()

    if not 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        raise Exception('Did not find AWS credentials in environment, please set.')

    lambda_size = "10000"
    lambda_threads = 3
    s3_scratch_dir = "s3://tuplex-leonhard/scratch/flights-exp"
    use_hyper_specialization = not args.no_hyper
    use_constant_folding = not args.no_cf
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2003_*.csv'
    s3_output_path = 's3://tuplex-leonhard/experiments/flights_hyper'
    strata_size = args.strata_size
    samples_per_strata = args.samples_per_strata

    # full dataset here (oO)
    input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_*.csv'

    # manipulate inout pattern depending on files
    # here are the default values (auto)
    year_lower = 2002
    year_upper = 2005
    if args.num_years != 'auto':
        num_years = int(args.num_years)
        years = make_year_range(num_years)
        print('-- Running with filter over years: {}'.format(', '.join([str(year) for year in years])))
        year_lower = min(years)
        year_upper = max(years)
    print(f"-- filter: lambda x: {year_lower} <= x['year'] <= {year_upper}")
    
    # use following as debug pattern
    #input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv'
    #input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_2002*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2003*.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2004*.csv'
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
        #sm = tuplex.dataset.SamplingMode.FIRST_FILE | tuplex.dataset.SamplingMode.RANDOM_ROWS
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
    print('    running in interpreter mode: {}'.format(args.python_mode))
    print('    hyperspecialization: {}'.format(use_hyper_specialization))
    print('    constant-folding: {}'.format(use_constant_folding))
    print('    null-value optimization: {}'.format(not args.no_nvo))
    print('    strata: {} per {}'.format(samples_per_strata, strata_size))
    # load data
    tstart = time.time()

    # configuration, make sure to give enough runtime memory to the executors!
    # run on Lambda
    conf = {"webui.enable" : False,
            "backend": "lambda",
            "aws.lambdaMemory": lambda_size,
            "aws.lambdaThreads": lambda_threads,
            "aws.httpThreadCount": 410,
            "aws.maxConcurrency": 410,
            'sample.maxDetectionMemory': '32MB',
            'sample.strataSize': strata_size,
            'sample.samplesPerStrata': samples_per_strata,
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
            "useInterpreterOnly": args.python_mode,
            "experimental.forceBadParseExceptFormat": not args.use_internal_fmt}

    if os.path.exists('tuplex_config.json'):
        with open('tuplex_config.json') as fp:
            conf = json.load(fp)

    conf['inputSplitSize'] = '2GB' #'256MB' #'128MB'
    conf["tuplex.experimental.opportuneCompilation"] = True #False #True #False #True

    if args.no_nvo:
        conf["optimizer.nullValueOptimization"] = False
    else:
        conf["optimizer.nullValueOptimization"] = True

    tstart = time.time()
    import tuplex
    ctx = tuplex.Context(conf)

    startup_time = time.time() - tstart
    print('Tuplex startup time: {}'.format(startup_time))
    tstart = time.time()
    ### QUERY HERE ###

    #debug
    #input_pattern = 's3://tuplex-public/data/flights_all/flights_on_time_performance_1999_05.csv'
    #input_pattern = "s3://tuplex-public/data/flights_all/flights_on_time_performance_1987_10.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2001_09.csv,s3://tuplex-public/data/flights_all/flights_on_time_performance_2021_11.csv"

    ctx.csv(input_pattern, sampling_mode=sm) \
        .withColumn("features", extract_feature_vector) \
        .map(fill_in_delays) \
        .filter(lambda x: year_lower <= x['year'] <= year_upper) \
        .tocsv(s3_output_path)

    ### END QUERY ###
    run_time = time.time() - tstart

    job_time = time.time() - tstart
    print('Tuplex job time: {} s'.format(job_time))
    m = ctx.metrics
    print(ctx.options())
    print(m.as_json())
    # print stats as last line
    print(json.dumps({"startupTime" : startup_time, "jobTime" : job_time}))
