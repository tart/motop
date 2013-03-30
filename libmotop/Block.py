#!/usr/bin/env python
# -*- coding: utf-8 -*-
##
# motop - Unix "top" Clone for MongoDB
#
# Copyright (c) 2012, Tart İnternet Teknolojileri Ticaret AŞ
#
# Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby
# granted, provided that the above copyright notice and this permission notice appear in all copies.
# 
# The software is provided "as is" and the author disclaims all warranties with regard to the software including all
# implied warranties of merchantability and fitness. In no event shall the author be liable for any special, direct,
# indirect, or consequential damages or any damages whatsoever resulting from loss of use, data or profits, whether
# in an action of contract, negligence or other tortious action, arising out of or in connection with the use or
# performance of this software.
##

"""Imports for Python 3 compatibility"""
from __future__ import print_function

"""Library imports"""
import sys

class Block:
    """Class to print blocks of ordered printables."""
    def __init__ (self, columnHeaders):
        self.__columnHeaders = columnHeaders
        self.__columnWidths = [6] * len (self.__columnHeaders)

    def reset (self, lines):
        self.__lines = lines

    def __len__ (self):
        return len (self.__lines)

    def __cell (self, value):
        if isinstance (value, tuple):
            return ' / '.join (self.__cell (value) for value in value)
        if value is not None:
            return str (value)
        return ''

    def __printLine (self, line, leftWidth, bold = False):
        """Print the cells separated by 2 spaces, cut the part after the width."""
        for index, value in enumerate (line):
            cell = self.__cell (value)
            if leftWidth < len (self.__columnHeaders [index]):
                """Do not show the column if there is not enough space for the header."""
                break
            if index + 1 < len (line):
                """Check the cell lenght if it is not the cell in the column. Set the column width to the cell lenght
                plus 2 for space if it is longer than the exisent column width."""
                self.__columnWidths [index] = max (len (cell) + 2, self.__columnWidths [index])
            if bold and sys.stdout.isatty ():
                print ('\x1b[1m', end = '')
            print (cell.ljust (self.__columnWidths [index]) [:leftWidth], end = '')
            if bold and sys.stdout.isatty ():
                print ('\x1b[0m', end = '')
            leftWidth -= self.__columnWidths [index]
        print ()

    def print (self, height, width):
        """Print the lines, cut the ones after the height."""
        assert height > 1
        self.__printLine (self.__columnHeaders, width, True)
        height -= 1
        for line in self.__lines:
            if height <= 1:
                break
            assert len (line) <= len (self.__columnHeaders)
            height -= 1
            self.__printLine (line, width)

