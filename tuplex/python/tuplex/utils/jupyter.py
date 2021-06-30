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

import json
import os.path
import re
import ipykernel
import urllib.request
from urllib.parse import urljoin
from notebook.notebookapp import list_running_servers

def get_jupyter_notebook_info():
    """
    retrieve infos about the currently running jupyter notebook if possible
    Returns: dict with several info attributes. If info for current notebook could not be retrieved, returns empty dict

    """
    def get(url):
        req = urllib.request.Request(url, headers={'content-type': 'application/json'})
        response = urllib.request.urlopen(req)
        return json.loads(response.read())

    kernel_id = re.search('kernel-(.*).json',
                          ipykernel.connect.get_connection_file()).group(1)
    servers = list_running_servers()

    for ss in servers:
        # there may be a 403 from jupyter...
        try:
            notebook_infos = get(urljoin(ss['url'], 'api/sessions?token={}'.format(ss.get('token', ''))))

            # search for match
            for ninfo in notebook_infos:
                if ninfo['kernel']['id'] == kernel_id:
                    return {'kernelID' : kernel_id, 'notebookID' : ninfo['id'],
                          'kernelName' : ninfo['kernel']['name'],
                          'path' : os.path.join(ss['notebook_dir'], ninfo['notebook']['path']),
                          'url' : urljoin(ss['url'],'notebooks/{}?token={}'.format(ninfo['path'], ss.get('token', '')))}
        except urllib.error.HTTPError as e:
            # ignore 403s (i.e. no allowed access)
            if e.getcode() !=  403:
                raise e
    return {}