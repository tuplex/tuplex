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

import optparse
import sys
from thserver.common import mongodb_available
from thserver import app, socketio

def run_history_server(default_host="127.0.0.1", default_port="5000", default_mongo_uri="mongodb://localhost:27017"):
    """
    Runs history server in debug mode
    Args:
        default_host: host name under which to run the server
        default_port: port of the server

    """

    parser = optparse.OptionParser()

    parser.add_option("-H", "--host",
                      help="Hostname of the Flask app " + \
                           "[default %s]" % default_host,
                      default=default_host)
    parser.add_option("-P", "--port",
                      help="Port for the Flask app " + \
                           "[default %s]" % default_port,
                      default=default_port)

    parser.add_option("-p", "--profile",
                      action="store_true", dest="profile",)

    options, _ = parser.parse_args()

    # If the user selects the profiling option, then we need
    # to do a little extra setup
    if options.profile:
        from werkzeug.contrib.profiler import ProfilerMiddleware

        app.config['PROFILE'] = True
        app.wsgi_app = ProfilerMiddleware(app.wsgi_app,
                                          restrictions=[30])
        options.debug = True

    # run with socketio
    socketio.run(app, debug=True, host=options.host, port=int(options.port))

    # normal flask without socketio
    # app.run(debug=True, host='0.0.0.0')

if __name__ == '__main__':
    # check here for options
    # http://flask.pocoo.org/snippets/133/

    # check connection to MongoDB
    # https://docs.jelastic.com/connection-to-mongodb-python
    if not mongodb_available():
        print('ERROR: could not connect to mongodb\nplease start MongoDB via mongod --dbpath history')
        sys.exit(1)

    run_history_server()
