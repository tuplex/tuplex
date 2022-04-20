#!/usr/bin/env python3
# ----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by March Boonyapaluk first on 4/19/2022                                                                     #
#  License: Apache 2.0                                                                                                 #
# ----------------------------------------------------------------------------------------------------------------------#

def getHTMLRow(ind, row):
    """
    Given a row, converts all the contents to an HTML row and return
    :param ind: the index of that row
    :param row: a row output from dataset
    :return: an HTML row, representative of the row
    """
    row_str = ""
    row_str += "    <tr>\n"
    row_str += "      <th>{}</th>\n".format(ind)
    if isinstance(row, list) or isinstance(row, tuple):
        for col in row:
            row_str += "      <td>{}</td>\n".format(col)
    else:
        row_str += "      <td>{}</td>\n".format(row)
    row_str += "    </tr>\n"
    return row_str


def getStrTableRow(ind, row):
    """
    Given a row, converts all the contents to string and return
    :param ind: the index of that row
    :param row: a row output from dataset
    :return: a list of string, representative of the row
    """
    row_str_list = ["{}".format(ind)]
    if isinstance(row, list) or isinstance(row, tuple):
        for col in row:
            row_str_list.append("{}".format(col))
    else:
        row_str_list.append("{}".format(row))
    return row_str_list


def _getLineDivider(col_width):
    out = ""
    for w in col_width:
        out += "+" + ("-" * (w + 2))
    out += "+\n"

    return out

def generateStrTable(numCols, strTable):
    """
    Given a 2-dimensional list of strings, print a nicely formatted table of the contents in the list
    :param numCols: number of columns in the table
    :param strTable: 2-dimensional list of strings, as list of list
    :return: a nicely formatted table in string
    """
    max_col_width = [0] * numCols

    for r in strTable:
        for i in range(0, len(r)):
            assert (isinstance(r[i], str))
            if len(r[i]) > max_col_width[i]:
                max_col_width[i] = len(r[i])

    output_str = ""

    for r in strTable:
        output_str += _getLineDivider(max_col_width)
        for i in range(0, len(r)):
            output_str += "| {:<{width}} ".format(r[i], width=max_col_width[i])
        output_str += "|\n"

    output_str += _getLineDivider(max_col_width) + "{} columns\n".format(numCols)

    return output_str
