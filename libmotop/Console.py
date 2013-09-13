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
try:
    import __builtin__
    __builtin__.input = __builtin__.raw_input
except ImportError: pass

"""Library imports"""
import sys
import os
import tty
import termios
import struct
import fcntl
import select
import signal
import time
from datetime import datetime

class DeactiveConsole ():
    """Class to use with "with" statement as "wihout" statement for Console class defined below."""
    def __init__ (self, console):
        self.__console = console

    def __enter__ (self):
        self.__console.__exit__ ()

    def __exit__ (self, *ignored):
        self.__console.__enter__ ()

class Console:
    """Main class for input and output. Used with "with" statement to hide pressed buttons on the console."""
    def __init__ (self):
        self.__deactiveConsole = DeactiveConsole (self)
        self.__saveSize ()
        signal.signal (signal.SIGWINCH, self.__saveSize)
        self.__lastCheckTime = None

    def __enter__ (self):
        """Hide pressed buttons on the console."""
        try:
            self.__settings = termios.tcgetattr (sys.stdin)
            tty.setcbreak (sys.stdin.fileno())
        except termios.error:
            self.__settings = None
        return self

    def __exit__ (self, *ignored):
        if self.__settings:
            termios.tcsetattr (sys.stdin, termios.TCSADRAIN, self.__settings)

    def __saveSize (self, *ignored):
        try:
            self.__height, self.__width = struct.unpack ('hhhh', fcntl.ioctl(0, termios.TIOCGWINSZ , '\000' * 8)) [:2]
        except IOError:
            self.__height, self.__width = 20, 80

    def waitButton (self):
        while True:
            try:
                return sys.stdin.read (1)
            except IOError: pass

    def checkButton (self, waitTime):
        """Check one character input. Waits for approximately waitTime parameter as seconds."""
        if self.__lastCheckTime:
            timedelta = datetime.now () - self.__lastCheckTime
            waitTime -= timedelta.seconds + (timedelta.microseconds / 1000000.0)
        while waitTime > 0 and not select.select ([sys.stdin], [], [], 0) [0]:
            time.sleep (0.1)
            waitTime -= 0.1
        self.__lastCheckTime = datetime.now ()
        if select.select ([sys.stdin], [], [], 0) [0]:
            return sys.stdin.read (1)

    def refresh (self, blocks):
        """Print the blocks with height and width left on the screen."""
        os.system ('clear')
        leftHeight = self.__height
        for block in blocks:
            if not len (block):
                """Do not show the block if there are no lines."""
                continue
            if leftHeight <= 2:
                """Do not show the block if there are not enough lines left for header and a row."""
                break
            height = len (block) + 2 if len (block) + 2 < leftHeight else leftHeight
            try:
                block.print (height, self.__width)
                leftHeight -= height
                if leftHeight >= 2:
                    print ()
                    leftHeight -= 1
            except IOError: pass

    def askForInput (self, *attributes):
        """Ask for input for given attributes in given order."""
        with self.__deactiveConsole:
            print ()
            values = []
            for attribute in attributes:
                value = input (attribute + ': ')
                if not value:
                    break
                values.append (value)
        return values

