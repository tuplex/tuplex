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

import os

from flask import Flask, render_template, request, abort, jsonify, make_response
from flask_socketio import SocketIO
from flask_pymongo import PyMongo
import pymongo

# try to connect to MongoDB backend. If it fails, print out warning.
mongo_default_uri = "mongodb://localhost:27017/tuplex-history"

# @ Todo... I.e. start with mongod --dbpath history


app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
app.config['PROPAGATE_EXCEPTIONS'] = True
# config MONGO URI
# app.config["MONGO_URI"] = "mongodb://localhost:27017/tuplex-history"

MONGO_URI = os.environ.get('MONGO_URI')
if not MONGO_URI:
    MONGO_URI = mongo_default_uri

app.config["MONGO_URI"] = MONGO_URI

socketio = SocketIO(app)
mongo = PyMongo(app, uri=MONGO_URI, serverSelectionTimeoutMS=2000, connectTimeoutMS=2000) # 2s server selection timeout


# register here MongoDB handler, because if the connection fails object mongo can't be used of course.
def MongoDBErrorHandler(e):
    return make_response(jsonify({'error' : 'MongoDB {} could not be reached.'.format(MONGO_URI)}), 404)
app.register_error_handler(pymongo.errors.ServerSelectionTimeoutError, MongoDBErrorHandler)

# rest.py contains all REST API endpoints
import thserver.rest

# views.py contains all UI endpoints
import thserver.views

# place config file in
#/instance/config.py for stuff that is not repo related
# app = Flask(__name__, instance_relative_config=True)
#
# app.config.from_object('config')
# app.config.from_pyfile('config.py')