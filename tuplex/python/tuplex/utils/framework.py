#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 8/3/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

# this file contains Framework specific exceptions
class TuplexException(Exception):
    """Base Exception class on which all Tuplex Framework specific exceptions are based"""
    pass

class UDFCodeExtractionError(TuplexException):
    """thrown when UDF code extraction/reflection failed"""
    pass