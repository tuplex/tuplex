#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 11/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

import unittest
from tuplex import *
from tuplex.utils.common import auto_shutdown_all, get_json

import logging
import urllib.request


class TestWebUI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)
        # bug in logging redirect?
        conf ={'webui.enable': True, "driverMemory": "8MB", "executorMemory" : "1MB",
               "partitionSize": "256KB", "tuplex.redirectToPythonLogging": True}
        cls.context = Context(conf)

    @classmethod
    def tearDownClass(cls) -> None:
        del cls.context

        # shutdown processes manually!
        auto_shutdown_all()


    # check connection to WebUI works
    def test_webuiconnect(self):

        # get webui uri
        ui_url = self.context.uiWebURL

        # connect to HTTP URL (index.html) and simply search for Tuplex string.
        req = urllib.request.Request(ui_url)
        with urllib.request.urlopen(req, timeout=10) as response:
            page_content = response.read().decode()

        self.assertTrue('Tuplex' in page_content)

        # make API request
        version_info = get_json(ui_url + '/api/version')
        self.assertIn('version', version_info)