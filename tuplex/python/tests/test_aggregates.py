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

import unittest
import functools
import random
import numpy as np
from tuplex import *
import typing
import os

class TestAggregates(unittest.TestCase):
    def setUp(self):
        self.conf = {"webui.enable": False, "driverMemory": "8MB", "partitionSize": "256KB"}

    def test_simple_count(self):
        c = Context(self.conf)

        data = [1, 2, 3, 4, 5, 6]
        res = c.parallelize(data).aggregate(lambda a, b: a + b, lambda a, x: a + 1, 0).collect()
        self.assertEqual(res[0], len(data))

    def test_simple_sum(self):
        c = Context(self.conf)

        data = [1, 2, 3, 4, 5, 6]
        res = c.parallelize(data).aggregate(lambda a, b: a + b, lambda a, x: a + x, 0).collect()
        self.assertEqual(res[0], sum(data))

    def test_sum_by_key(self):
        c = Context(self.conf)

        data = [(0, 10.0), (1, 20.0), (0, -4.5)]

        res = c.parallelize(data, columns=['id', 'volume']).aggregateByKey(lambda a, b: a + b,
                                                                            lambda a, x: a + x['volume'],
                                                                            0.0,
                                                                            ['id']).collect()

        self.assertEqual(len(res), 2)

        # sort result for comparison (in the future Tuplex should provide a function for this!)
        res = sorted(res, key=lambda t: t[0])

        self.assertEqual(res[0][0], 0)
        self.assertEqual(res[1][0], 1)

        self.assertAlmostEqual(res[0][1], 10.0 - 4.5)
        self.assertAlmostEqual(res[1][1], 20.0)

    def test_311(self):
        input_path = 'test_311_testfile.csv'
        output_path = 'test_311_testfile.out.csv'
        with open(input_path, 'w') as fp:
            data = '''UniqueKey,CreatedDate,Agency,ComplaintType,Descriptor,IncidentZip,StreetName
46688741,06/30/2020 07:24:41 PM,NYPD,Noise - Residential,Loud Music/Party,10037.0,MADISON AVENUE
53493739,02/28/2022 07:30:31 PM,NYPD,Illegal Parking,Double Parked Blocking Traffic,11203.0,EAST   56 STREET
48262955,11/27/2020 12:00:00 PM,DSNY,Derelict Vehicles,Derelict Vehicles,11203.0,CLARKSON AVENUE
48262956,11/27/2020 12:00:00 PM,DSNY,Derelict Vehicles,Derelict Vehicles,11208.0,SHEPHERD AVENUE
48262957,11/27/2020 12:00:00 PM,DSNY,Derelict Vehicles,Derelict Vehicles,11238.0,BERGEN STREET
46688747,06/30/2020 02:51:45 PM,NYPD,Noise - Vehicle,Engine Idling,10009.0,EAST   12 STREET
46688748,06/30/2020 09:26:45 AM,NYPD,Non-Emergency Police Matter,Face Covering Violation,11204.0,20 AVENUE
48262973,11/27/2020 03:46:00 PM,DEP,Water Quality,unknown odor/taste in drinking water (QA6),10021.0,EAST   70 STREET
53493766,02/28/2022 05:28:38 AM,NYPD,Noise - Vehicle,Car/Truck Horn,11366.0,PARSONS BOULEVARD'''
            fp.write(data)

        def fix_zip_codes(zips):
            if not zips:
                return None
            # Truncate everything to length 5
            s = zips[:5]

            # Set 00000 zip codes to nan
            if s == "00000":
                return None
            else:
                return s

        ctx = Context(self.conf)
        df = ctx.csv(input_path,
            null_values=["Unspecified", "NO CLUE", "NA", "N/A", "0", ""],
            type_hints={0: typing.Optional[str],
                        1: typing.Optional[str],
                        2: typing.Optional[str],
                        3: typing.Optional[str],
                        4: typing.Optional[str],
                        5: typing.Optional[str],
                        },
        )
        # Do the pipeline
        df = df.mapColumn("IncidentZip", fix_zip_codes).unique()

        # Output to csv
        output_path_part0 = 'test_311_testfile.out.part0.csv'
        df.tocsv(output_path)

        self.assertTrue(os.path.isfile(output_path_part0))
