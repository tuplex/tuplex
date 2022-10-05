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

from .helper import test_options

class TestWebUI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

        localhost_ip = '127.0.0.1'
        conf = test_options()
        # bug in logging redirect?
        conf.update({"webui.enable": True, "driverMemory": "8MB", "executorMemory" : "1MB",
               "partitionSize": "256KB", "tuplex.redirectToPythonLogging": True,
               "webui.mongodb.url": "localhost", "webui.url" : localhost_ip})

        logging.debug('WebUI Test setUpClass called')
        cls.context = Context(conf)
        logging.debug('Context created...')

    @classmethod
    def tearDownClass(cls) -> None:
        logging.debug('WebUI Test tearDownClass called')
        del cls.context
        cls.context = None

        # shutdown processes manually!
        auto_shutdown_all()

    # check connection to WebUI works
    def test_webuiconnect(self):

        logging.debug('Entering webuiconnect test...')

        # get webui uri
        ui_url = self.context.uiWebURL

        logging.debug('Retrieved webui url as {}'.format(ui_url))

        # connect to HTTP URL (index.html) and simply search for Tuplex string.
        req = urllib.request.Request(ui_url)
        with urllib.request.urlopen(req, timeout=10) as response:
            page_content = response.read().decode()

        self.assertTrue('Tuplex' in page_content)

        # make API request
        version_info = get_json(ui_url + '/api/version')
        self.assertIn('version', version_info)
