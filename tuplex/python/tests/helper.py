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

def test_options():
    return {'tuplex.partitionSize': "128KB",
            "tuplex.executorMemory": "8MB",
            "tuplex.useLLVMOptimizer": True,
            "tuplex.allowUndefinedBehavior": False,
            "tuplex.webui.enable": False,
            "tuplex.optimizer.mergeExceptionsInOrder": True,
            "tuplex.optimizer.selectionPushdown": True,
            "tuplex.scratchDir": ".cache/"}
