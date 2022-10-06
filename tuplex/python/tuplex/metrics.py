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
import logging
import typing
try:
    from .libexec.tuplex import _Context
    from .libexec.tuplex import _Metrics
except ModuleNotFoundError as e:
    logging.error("need to compiled Tuplex first, details: {}".format(e))
    _Metrics = typing.Any

import json

class Metrics:
    """
    Stores a reference to the metrics associated with a
    context object.
    """

    def __init__(self, metrics: _Metrics):
        """
        Creates a Metrics object by using the context object
        to set its metric parameter and store the resulting
        Metrics object.
        Args:
        context (_Context object):  The context object the metrics
                                    should be associated with.
        """
        assert metrics
        self._metrics = metrics

    @property
    def totalExceptionCount(self) -> int:
        """
        Retrieves the total exception count in seconds.
        Returns:
            int:  total exception count in seconds
        """
        assert self._metrics
        return self._metrics.getTotalExceptionCount()

    @property
    def logicalOptimizationTime(self) -> float:
        """
        Retrieves the logical optimization time in seconds.
        Returns:
            float:  the logical optimization time in seconds
        """
        assert self._metrics
        return self._metrics.getLogicalOptimizationTime()

    @property
    def LLVMOptimizationTime(self) -> float:
        """
        Retrieves the optimization time in LLVM in seconds.
        Returns:
            float:  the optimization time in LLVM in seconds
        """
        assert self._metrics
        return self._metrics.getLLVMOptimizationTime()

    @property
    def LLVMCompilationTime(self) -> float:
        """
        Retrieves the compilation time in LLVM in seconds.
        Returns:
            float:  the compilation time in LLVM in seconds
        """
        assert self._metrics
        return self._metrics.getLLVMCompilationTime()

    @property
    def totalCompilationTime(self) -> float:
        """
        Retrieves the total compilation time in seconds.
        Returns:
            float:  the total compilation time in seconds
        """
        assert self._metrics
        return self._metrics.getTotalCompilationTime()

    def as_json(self) -> str:
        """
        all measurements as json encoded string
        Returns:
            str: json encoded measurements
        """
        assert self._metrics
        return self._metrics.getJSONString()

    def as_dict(self):
        """
        all measurements in nested dictionary
        Returns:
            dict: measurements
        """
        return json.loads(self.as_json())
