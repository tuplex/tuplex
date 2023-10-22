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
import os
from tuplex import *
from helper import options_for_pytest

class TestCSV(unittest.TestCase):

    def _generate_csv_file(self, path, delimiter, has_header=False):
        with open(path, 'w') as csv_file:
            if has_header:
                csv_file.write(delimiter.join(['a', 'b', 'c', 'd']) + '\n')
            for i in range(3):
                elements = []
                for j in range(3):
                    elements.append(str(3 * i + j + 1))
                elements.append('FAST ETL!')
                csv_file.write(delimiter.join(elements) + '\n')

    def setUp(self):
        self._generate_csv_file('test.csv', ',')
        self._generate_csv_file('test.tsv', '\t')
        self._generate_csv_file('test_header.csv', ',', True)
        self.conf = options_for_pytest()
        self.conf.update({"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"})

    def tearDown(self):
        os.remove('test.csv')
        os.remove('test.tsv')
        os.remove('test_header.csv')

    def test_show(self):
        c = Context(self.conf)
        dataset = c.csv("test.csv")
        dataset.show()

    def test_map(self):
        c = Context(self.conf)
        dataset = c.csv("test.csv")
        res = dataset.map(lambda a, b, c, d: a).collect()
        assert res == [1, 4, 7]

        dataset = c.csv("test.csv")
        res = dataset.map(lambda a, b, c, d: d).collect()
        assert res == ["FAST ETL!", "FAST ETL!", "FAST ETL!"]

    def test_tsv(self):
        c = Context(self.conf)
        dataset = c.csv("test.tsv", delimiter='\t')
        res = dataset.map(lambda a, b, c, d: a).collect()
        assert res == [1, 4, 7]

        dataset = c.csv("test.tsv", delimiter='\t')
        res = dataset.map(lambda a, b, c, d: d).collect()
        assert res == ["FAST ETL!", "FAST ETL!", "FAST ETL!"]

    def test_non_existent_file(self):
        c = Context(self.conf)
        dataset = c.csv("test.ccc")
        dataset.show()

    def test_csv_with_header(self):
        c = Context(self.conf)
        dataset = c.csv("test_header.csv", header=True)
        res = dataset.map(lambda a, b, c, d: a).collect()
        assert res == [1, 4, 7]

    def test_csv_select(self):

        content = """Unique Key,Created Date,Closed Date,Agency,Agency Name,Complaint Type,Descriptor,Location Type,Incident Zip,Incident Address,Street Name,Cross Street 1,Cross Street 2,Intersection Street 1,Intersection Street 2,Address Type,City,Landmark,Facility Type,Status,Due Date,Resolution Description,Resolution Action Updated Date,Community Board,BBL,Borough,X Coordinate (State Plane),Y Coordinate (State Plane),Open Data Channel Type,Park Facility Name,Park Borough,Vehicle Type,Taxi Company Borough,Taxi Pick Up Location,Bridge Highway Name,Bridge Highway Direction,Road Ramp,Bridge Highway Segment,Latitude,Longitude,Location
19937896,03/01/2011 02:27:57 PM,03/14/2011 03:59:20 PM,DOF,Refunds and Adjustments,DOF Property - Payment Issue,Misapplied Payment,Property Address,10027,,,,,,,ADDRESS,NEW YORK,,N/A,Closed,03/22/2011 02:27:57 PM,The Department of Finance resolved this issue.,03/14/2011 03:59:20 PM,09 MANHATTAN,1019820050,MANHATTAN,,,PHONE,Unspecified,MANHATTAN,,,,,,,,,,
19937901,03/01/2011 10:41:13 AM,03/15/2011 04:14:19 PM,DOT,Department of Transportation,Street Sign - Dangling,Street Cleaning - ASP,Street,11232,186 25 STREET,25 STREET,3 AVENUE,4 AVENUE,,,ADDRESS,BROOKLYN,,N/A,Closed,03/15/2011 05:32:23 PM,The Department of Transportation has completed the request or corrected the condition.,03/15/2011 04:14:19 PM,07 BROOKLYN,3006540024,BROOKLYN,984640,180028,PHONE,Unspecified,BROOKLYN,,,,,,,,40.660811976282695,-73.99859430999363,"(40.660811976282695, -73.99859430999363)"
19937902,03/01/2011 09:07:45 AM,03/15/2011 08:26:09 AM,DOT,Department of Transportation,Street Sign - Missing,Other/Unknown,Street,11358,,,,,158 STREET,NORTHERN BOULEVARD,INTERSECTION,FLUSHING,,N/A,Closed,03/15/2011 02:24:33 PM,The Department of Transportation has completed the request or corrected the condition.,03/15/2011 08:26:09 AM,07 QUEENS,,QUEENS,1037621,217498,PHONE,Unspecified,QUEENS,,,,,,,,40.763497105049986,-73.80733639290203,"(40.763497105049986, -73.80733639290203)"
19937903,03/01/2011 05:39:26 PM,04/04/2011 11:32:57 AM,DOT,Department of Transportation,Street Sign - Missing,School Crossing,Street,10014,10 SHERIDAN SQUARE,SHERIDAN SQUARE,BARROW STREET,GROVE STREET,,,ADDRESS,NEW YORK,,N/A,Closed,04/01/2011 03:43:12 PM,"Upon inspection, the reported condition was not found, therefore no action was taken.",04/04/2011 11:32:57 AM,02 MANHATTAN,1005920040,MANHATTAN,983719,206336,PHONE,Unspecified,MANHATTAN,,,,,,,,40.733021305197404,-74.00191597502526,"(40.733021305197404, -74.00191597502526)"
19937904,03/01/2011 11:08:14 AM,03/02/2011 07:55:37 AM,DOT,Department of Transportation,Street Sign - Missing,Stop,Street,10069,,,,,WEST   63 STREET,WEST END AVENUE,INTERSECTION,NEW YORK,,N/A,Closed,03/08/2011 11:08:14 AM,"The condition has been inspected/investigated, see customer notes for more information.",03/02/2011 07:55:37 AM,07 MANHATTAN,,MANHATTAN,987400,221308,PHONE,Unspecified,MANHATTAN,,,,,,,,40.77411510013836,-73.98862703263869,"(40.77411510013836, -73.98862703263869)"
19937906,03/01/2011 03:16:09 PM,03/02/2011 09:06:30 AM,DOF,Correspondence Unit,DOF Property - Request Copy,Copy of Notice of Property Value,Property Address,11105,,,,,,,ADDRESS,ASTORIA,,N/A,Closed,03/06/2011 03:16:09 PM,The Department of Finance mailed the requested item.,03/02/2011 09:06:31 AM,01 QUEENS,4009650074,QUEENS,,,PHONE,Unspecified,QUEENS,,,,,,,,,,
19937907,03/01/2011 01:22:59 PM,03/02/2011 09:06:28 AM,DOF,Correspondence Unit,DOF Property - Request Copy,Copy of Notice of Property Value,Property Address,10469,,,,,,,ADDRESS,BRONX,,N/A,Closed,03/06/2011 01:22:59 PM,The Department of Finance mailed the requested item.,03/02/2011 09:06:28 AM,12 BRONX,2046970142,BRONX,,,PHONE,Unspecified,BRONX,,,,,,,,,,
19937908,03/01/2011 12:01:58 PM,03/02/2011 09:05:26 AM,DOF,Correspondence Unit,DOF Property - Request Copy,Copy of Notice of Property Value,Property Address,10305,,,,,,,ADDRESS,STATEN ISLAND,,N/A,Closed,03/06/2011 12:01:58 PM,The Department of Finance mailed the requested item.,03/02/2011 09:05:26 AM,02 STATEN ISLAND,5032350004,STATEN ISLAND,,,PHONE,Unspecified,STATEN ISLAND,,,,,,,,,,
19937909,03/01/2011 02:35:46 PM,03/02/2011 09:06:31 AM,DOF,Correspondence Unit,DOF Property - Request Copy,Copy of Notice of Property Value,Property Address,11221,,,,,,,ADDRESS,BROOKLYN,,N/A,Closed,03/06/2011 02:35:46 PM,The Department of Finance mailed the requested item.,03/02/2011 09:06:31 AM,04 BROOKLYN,3033660059,BROOKLYN,,,PHONE,Unspecified,BROOKLYN,,,,,,,,,,"""
        with open('311_subset.csv', 'w') as fp:
            fp.write(content)

        c = Context(self.conf)
        ds = c.csv('311_subset.csv')
        cols = ds.selectColumns(['Unique Key']).columns
        self.assertEqual(cols, ['Unique Key'])

# # test cases for CSV files (in resources folder)
# # same test files as Spark uses
# class TestSparkCSVFiles(unittest.TestCase):
#     def test_spark(self):
#         fname = 'bool.csv'
#         c = Context()
#         ds = c.csv(fname)
#         res = ds.collect()
#         assert len(res) == 3
#         assert res == [True, False, True]