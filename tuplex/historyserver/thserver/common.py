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

import datetime
import iso8601

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# cf. https://www.artificialworlds.net/blog/2017/06/09/python-printing-utc-dates-in-iso8601-format-with-time-zone/
# all time formats according to RFC8601
def current_utc_string():
    return datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

def current_utc_timestamp():
    dt = datetime.datetime.utcnow()
    return dt.replace(tzinfo=datetime.timezone.utc).timestamp()

def string_to_utc(s):
    return iso8601.parse_date(s)

def find_lambda(arr, f):
    L = [i for i, a in enumerate(arr) if f(a)]
    assert len(L) == 1, 'none or more than one element found'
    return arr[L[0]], L[0]

def mongodb_available():
    """
    checks whether it is possible to establish a connection to MongoDB database
    Returns: true if possible, false else

    """
    client = MongoClient(serverSelectionTimeoutMS=20, connectTimeoutMS=20000)
    try:
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        return True
    except ConnectionFailure:
        return False