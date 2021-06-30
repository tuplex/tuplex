#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

import csv
file_path = 'test/resources/L_CARRIER_HISTORY.csv'
res = []

with open(file_path) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        d = {}

        name_udf = lambda x: x[:x.rfind('(')].strip()
        founded_udf = lambda x: int(x[x.rfind('(')+1:x.rfind('-')])


        def extractDefunctYear(x):
            desc = x[x.rfind('-')+1:x.rfind(')')].strip()
            return int(desc) if len(desc) > 0 else None

        d['Code'] = row['Code']
        d['Description'] = row['Description']
        d['AirlineName'] = name_udf(row['Description'])
        d['AirlineYearFounded'] = founded_udf(row['Description'])
        d['AirlineYearDefunct'] = extractDefunctYear(row['Description'])
        res.append(d)