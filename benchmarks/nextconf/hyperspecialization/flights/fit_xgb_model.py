#!/usr/bin/env python3
# requirements: pip3 install scikit-learn xgboost pandas
import argparse
import os.path

import pandas as pd
import glob
import re
import sys
from tqdm import tqdm

import numpy as np
import xgboost as xgb
import logging
import time

from sklearn.metrics import mean_absolute_error
import multiprocessing
from multiprocessing import Pool


# initialize logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

DEFAULT_DATA_ROOT = '../../../../tuplex/test/resources/hyperspecialization/flights_all/'
OUTPUT_DIR = 'model'

def string_parser(s):
    if len(re.findall(r":leaf=", s)) == 0:
        out = re.findall(r"[\w.-]+", s)
        tabs = re.findall(r"[\t]+", s)
        if (out[4] == out[8]):
            missing_value_handling = (" or math.isnan(x['" + out[1] + "']) ")
        else:
            missing_value_handling = ""

        if len(tabs) > 0:
            return (re.findall(r"[\t]+", s)[0].replace('\t', '    ') +
                    '        if state == ' + out[0] + ':\n' +
                    re.findall(r"[\t]+", s)[0].replace('\t', '    ') +
                    '            state = (' + out[4] +
                    ' if ' + "x['" + out[1] + "']<" + out[2] + missing_value_handling +
                    ' else ' + out[6] + ')\n')

        else:
            return ('        if state == ' + out[0] + ':\n' +
                    '            state = (' + out[4] +
                    ' if ' + "x['" + out[1] + "']<" + out[2] + missing_value_handling +
                    ' else ' + out[6] + ')\n')
    else:
        out = re.findall(r"[\d.-]+", s)
        return (re.findall(r"[\t]+", s)[0].replace('\t', '    ') +
                '        if state == ' + out[0] + ':\n    ' +
                re.findall(r"[\t]+", s)[0].replace('\t', '    ') +
                '        return ' + out[1] + '\n')


def tree_parser(tree, i):
    if i == 0:
        return ('    if num_booster == 0:\n        state = 0\n'
                + "".join([string_parser(tree.split('\n')[i])
                           for i in range(len(tree.split('\n')) - 1)]))
    else:
        return ('    elif num_booster == ' + str(i) + ':\n        state = 0\n'
                + "".join([string_parser(tree.split('\n')[i])
                           for i in range(len(tree.split('\n')) - 1)]))

def replace_indices(text):
    def replace_func(m):
        return 'x[{}]'.format(m[1])
    regex = r"x\['(\d+)']"
    new_text, n_occurences = re.subn(regex, replace_func, text, )
    return new_text

def model_to_py(base_score, model, out_file):
    trees = model.get_dump()
    result = ["import math\n\n"
              + "def xgb_tree(x, num_booster):\n"]

    for i in range(len(trees)):
        result.append(tree_parser(trees[i], i))

    result = [replace_indices(t) for t in result]

    with open(out_file, 'w') as the_file:
        the_file.write("".join(result) + "\ndef xgb_predict(x):\n    predict = "
                       + str(base_score) + "\n"
                       + "    # initialize prediction with base score\n"
                       + "    for i in range("
                       + str(len(trees))
                       + "):\n        predict = predict + xgb_tree(x, i)"
                       + "\n    return predict")


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

def fit_model(df_X, df_y, delay_type, py_filename):

    df_y_sel = df_y[~df_y[delay_type].isnull()]

    df_Xy_sel = pd.concat((df_X, df_y_sel), axis=1, join='inner')

    X = df_Xy_sel[df_X.columns]
    y = df_Xy_sel[delay_type]

    logging.info(f'starting model fit for delay_type={delay_type}')
    base_score = np.mean(y)
    model = xgb.XGBRegressor(objective='reg:squarederror',
                         tree_method='hist',
                         base_score=base_score,
                         eval_metric=mean_absolute_error,
                         max_depth=3,
                         eta=0.8,
                         gamma=3,
                         n_estimators=10)

    model.fit(X, y, eval_set=[(X, y)])

    m_score = model.score(X, y)
    logging.info(f'Fit done with score {m_score}')
    logging.info("model fit done, converting model to python code")


    model_to_py(base_score, model.get_booster(), py_filename)
    logging.info(f'saved model to: {py_filename}')

def preprocess_file(input_path, X_path, y_path):
    logging.info(f"loading {input_path}")
    df = pd.read_csv(input_path, encoding='latin1')

    # restrict df
    logging.info('restricting dataframe')
    df = df[(df['ARR_DELAY'] >= 0.0)]

    # get feature vector
    logging.info('Transforming into features')
    df['features'] = df.apply(lambda x: np.array(extract_feature_vector(x)), axis=1)

    delay_names = [name for name in list(df.columns) if '_DELAY' in name]
    df_prep = df[delay_names + ['features']]

    logging.info('Converting into prepared dataframe for training')
    df_final = pd.DataFrame([pd.Series(x) for x in df_prep.features])

    # save X, y
    logging.info(f'Saving to {X_path}')
    df_final.to_csv(X_path, index=None)
    logging.info(f'Saving to {y_path}')
    df_prep[delay_names].to_csv(y_path, index=None)

# multi processing limitations
def pool_func(t):
    return preprocess_file(t[0], t[1], t[2])

def main():

    # parse args and set params
    parser = argparse.ArgumentParser(
        prog='Flights XGB model',
        description='Generates XGB model for flights dataset to predict delay components',
        epilog='(c) L. Spiegelberg 2023)')
    parser.add_argument('--data-root', default=DEFAULT_DATA_ROOT, help='root folder for flights data')
    parser.add_argument('-o', '--output-dir', default=OUTPUT_DIR, help='output directory for prepped files and model files')
    args = parser.parse_args()

    # vars
    data_root = args.data_root
    output_dir = (args.output_dir + '/').replace('//', '/')
    X_path = os.path.join(output_dir, 'flights_prepped_X.csv')
    y_path = os.path.join(output_dir, 'flights_prepped_y.csv')

    logging.info('Flights XGB model file generator')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logging.info('Created output dir {}'.format(output_dir))

    if not os.path.exists(X_path) or not os.path.exists(y_path):

        paths = glob.glob(data_root + '*.csv.sample') + glob.glob(data_root + '*.csv')

        logging.info(f'Found {len(paths)} paths to train model(s) on.')
        if len(paths) == 0:
            logging.info('No files found, abort.')
            sys.exit(0)

        logging.info('Loading data...')
        df = pd.DataFrame()

        paths_to_process = []
        part_no = 0
        for path in tqdm(paths):
            # skip years before 2003
            year = int(path.split('_')[-2])
            if year < 2003:
                continue

            tmp_X, tmp_y = X_path + f".part{part_no}", y_path + f".part{part_no}"

            paths_to_process.append((path, tmp_X, tmp_y))
            part_no += 1

        cpu_count = multiprocessing.cpu_count()
        logging.info(f"Processing in parallel {len(paths_to_process)} files with {cpu_count} processes")

        #preprocess_file(path, X_path + f".part{part_no}", y_path + f".part{part_no}")
        tstart = time.time()

        with Pool(cpu_count) as p:
            res = p.map(pool_func,
                        paths_to_process)
        logging.info(f'multi took {time.time() - tstart}s')
        logging.info("combining part results...")

        # combine X
        logging.info('combining X')
        df_X = pd.DataFrame()
        for t in tqdm(paths_to_process):
            df_X = pd.concat((df_X, pd.read_csv(t[1], header=0)))
        df_X.to_csv(X_path, index=None)
        del df_X

        # combine Y
        logging.info('combining y')
        df_y = pd.DataFrame()
        for t in tqdm(paths_to_process):
            df_y = pd.concat((df_y, pd.read_csv(t[2], header=0)))
        df_y.to_csv(y_path, index=None)
        del df_y

        logging.info('Removing files...')
        for t in tqdm(paths_to_process):
            os.remove(t[1])
            os.remove(t[2])
        logging.info('part files removed')
        logging.info('saving prepared dataset done')

    logging.info('Fitting models::')

    df_X = pd.read_csv(X_path, header=0)
    df_y = pd.read_csv(y_path, header=0)

    delay_types = ['DEP_DELAY', 'CARRIER_DELAY', 'WEATHER_DELAY',
                   'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY']

    for delay_type in delay_types:
        logging.info(f'Creating model for {delay_type}')

        # where to store model
        py_filename = os.path.join(output_dir, 'xgb_' + delay_type.lower() + ".py")

        fit_model(df_X, df_y, delay_type, py_filename)

        logging.info('Done.')
    logging.info('Script done...!')

if __name__ == '__main__':
    main()