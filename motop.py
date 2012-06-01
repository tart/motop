#!/usr/bin/env python
# -*- coding: utf-8 -*-
##
# motop - Unix "top" Clone for MongoDB
#
# Copyright (c) 2012, Tart İnternet Teknolojileri AŞ
#
# Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby
# granted, provided that the above copyright notice and this permission notice appear in all copies.
# 
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
# INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
# AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
# PERFORMANCE OF THIS SOFTWARE.
##

"""Imports for Python 3 compatibility."""
from __future__ import print_function
try:
    import __builtin__
    __builtin__.input = __builtin__.raw_input
except ImportError: pass

"""Common library imports"""
import sys
import os
import tty
import termios
import struct
import fcntl
import select
import json
import signal
import pymongo
from bson import json_util
from time import sleep

class Value (int):
    """Class extents int to show big numbers human readable."""

    def __str__ (self):
        if self > 10 ** 12:
            return str (int (round (self / 10 ** 12))) + 'T'
        if self > 10 ** 9:
            return str (int (round (self / 10 ** 9))) + 'G'
        if self > 10 ** 6:
            return str (int (round (self / 10 ** 6))) + 'M'
        if self > 10 ** 3:
            return str (int (round (self / 10 ** 3))) + 'K'
        return int.__str__ (self)

class Block:
    """Class to print blocks of ordered printables."""

    def __init__ (self, columnHeaders, height, reverseOrder = False):
        self.__columnHeaders = columnHeaders
        self.__columnWidths = [len (columnHeader) + 2 for columnHeader in self.__columnHeaders]
        self.__height = height
        self.__reverseOrder = reverseOrder

    def height (self):
        return self.__height

    def reset (self, printables):
        self.__printables = sorted (printables, key = lambda printable: printable.sortOrder (), reverse = self.__reverseOrder) [:self.__height]
        self.__lines = [printable.line () for printable in self.__printables]

    def printLines (self, leftHeight, width):
        assert leftHeight > 2
        leftWidth = width
        for index, columnHeader in enumerate (self.__columnHeaders):
            if leftWidth <= len (columnHeader):
                break
            print (columnHeader.ljust (self.__columnWidths [index]) [:self.__columnWidths [index]], end = '')
            leftWidth -= self.__columnWidths [index]
        print ()
        leftHeight -= 2
        for line in self.__lines:
            if not leftHeight:
                break
            leftHeight -= 1
            leftWidth = width
            for index, cell in enumerate (line):
                if leftWidth <= len (self.__columnHeaders [index]):
                    break
                assert isinstance (cell, str)
                if len (cell) + 2 > self.__columnWidths [index]:
                    self.__columnWidths [index] = len (cell) + 2 if len (cell) + 2 < leftWidth else leftWidth - 1
                leftWidth -= self.__columnWidths [index]
                print (cell.ljust (self.__columnWidths [index]) [:self.__columnWidths [index]], end = '')
            print ()

    def findLine (self, line):
        """Returns the printable."""

        for printable in self.__printables:
            printableLine = printable.line ()
            different = False
            index = 0
            while not different and len (line) > index:
                if printableLine [index] != line [index]:
                    different = True
                index += 1
            if not different:
                return printable

class Operation:
    def __init__ (self, server, opid):
        self.__server = server
        self.__opid = opid

    def getServer (self):
        return self.__server

    def sortOrder (self):
        return -1 * self.__opid

    def line (self):
        cells = []
        cells.append (str (self.__server))
        cells.append (str (self.__opid))
        return cells

    def kill (self):
        return self.__server.killOperation (self.__opid)

class Query (Operation):
    def __init__ (self, server, opid, namespace, body, duration = None):
        Operation.__init__ (self, server, opid)
        self.__namespace = namespace
        self.__body = body
        self.__duration = duration

    def sortOrder (self):
        return self.__duration if self.__duration else 0

    block = Block (['Server', 'OpId', 'Namespace', 'Sec', 'Query'], 30, reverseOrder = True)

    def line (self):
        cells = Operation.line (self)
        cells.append (str (self.__namespace))
        cells.append (str (self.__duration))
        cells.append (json.dumps (self.__body, default = json_util.default) [:200])
        return cells

    def printExplain (self):
        """Prints the output of the explain command executed on the server of the query."""
        if self.__namespace:
            server = self.getServer ()
            databaseName, collectionName = self.__namespace.split ('.', 1)
            explainOutput = server.explainQuery (databaseName, collectionName, self.__body)
            print ('Cursor:', explainOutput ['cursor'])
            print ('Indexes:', end = '')
            for index in explainOutput ['indexBounds']:
                print (index, end = '')
            print ()
            print ('IndexOnly:', explainOutput ['indexOnly'])
            print ('MultiKey:', explainOutput ['isMultiKey'])
            print ('Miliseconds:', explainOutput ['millis'])
            print ('Documents:', explainOutput ['n'])
            print ('ChunkSkips:', explainOutput ['nChunkSkips'])
            print ('Yields:', explainOutput ['nYields'])
            print ('Scanned:', explainOutput ['nscanned'])
            print ('ScannedObjects:', explainOutput ['nscannedObjects'])
            if 'scanAndOrder' in explainOutput:
                print ('ScanAndOrder:', explainOutput ['scanAndOrder'])
            print ('Query:', json.dumps (self.__body, default = json_util.default, sort_keys = True, indent = 4))
            return True
        return False

class Server:
    def __init__ (self, name, address):
        assert len (name) < 14
        self.__name = name
        self.__address = address
        self.__connection = pymongo.Connection (address)
        self.__operationCount = 0
        self.__flushCount = 0

    def sortOrder (self):
        return self.__name

    def __getOperationCountChange (self, operationCounts):
        oldOperationCount = self.__operationCount
        self.__operationCount = sum ([value for key, value in operationCounts.items ()])
        return self.__operationCount - oldOperationCount

    def __getFlushCountChange (self, flushCount):
        oldFlushCount = self.__flushCount
        self.__flushCount = flushCount
        return self.__flushCount - oldFlushCount

    block = Block (['Server', 'QPS', 'Clients', 'Queue', 'Flushes', 'Connections', 'Memory'], 7)

    def line (self):
        success = False
        while not success:
            try:
                serverStatus = self.__connection.admin.command ('serverStatus')
                success = True
            except pymongo.errors.AutoReconnect: pass

        currentConnection = Value (serverStatus ['connections'] ['current'])
        totalConnection = Value (serverStatus ['connections'] ['available'] + serverStatus ['connections'] ['current'])
        residentMem = Value (serverStatus ['mem'] ['resident'])
        mappedMem = Value (serverStatus ['mem'] ['mapped'])
        cells = []
        cells.append (str (self))
        cells.append (str (Value (self.__getOperationCountChange (serverStatus ['opcounters']))))
        cells.append (str (Value (serverStatus ['globalLock'] ['activeClients'] ['total'])))
        cells.append (str (Value (serverStatus ['globalLock'] ['currentQueue'] ['total'])))
        cells.append (str (Value (self.__getFlushCountChange (serverStatus ['backgroundFlushing'] ['flushes']))))
        cells.append (str (currentConnection) + ' / ' + str (totalConnection))
        cells.append (str (residentMem) + ' / ' + str (mappedMem))
        return cells

    def explainQuery (self, databaseName, collectionName, query):
        database = getattr (self.__connection, databaseName)
        collection = getattr (database, collectionName)
        cursor = collection.find (query)
        return cursor.explain ()

    def currentOperations (self):
        success = False
        while not success:
            try:
                inprog = self.__connection.admin.current_op () ['inprog']
                success = True
            except pymongo.errors.AutoReconnect: pass

        for op in inprog:
            if op ['op'] == 'query':
                if 'secs_running' in op:
                    yield Query (self, op ['opid'], op ['ns'], op ['query'], op ['secs_running'])
                else:
                    yield Query (self, op ['opid'], op ['ns'], op ['query'])
            else:
                yield Operation (self, op ['opid'])

    def killOperation (self, opid):
        """Kill operation using the "mongo" executable on the shell. That is because I could not make it with
        pymongo."""
        os.system ('echo "db.killOp (' + str (opid) + ')" | mongo ' + self.__address)

    def __str__ (self):
        return self.__name

class ConsoleActivator:
    """Class to use with "with" statement to hide pressed buttons on the console."""
    def __enter__ (self):
        self.__settings = termios.tcgetattr (sys.stdin)
        tty.setcbreak (sys.stdin.fileno())
        return Console (self)

    def __exit__ (self, *ignored):
        termios.tcsetattr (sys.stdin, termios.TCSADRAIN, self.__settings)

class ConsoleDeactivator ():
    """Class to use with "with" statement as "wihout" statement for ConsoleActivator."""
    def __init__ (self, consoleActivator):
        self.__consoleActivator = consoleActivator

    def __enter__ (self):
        self.__consoleActivator.__exit__ ()

    def __exit__ (self, *ignored):
        self.__consoleActivator.__enter__ ()

class Console:
    """Main class for input and output."""

    def __init__ (self, consoleActivator):
        self.__consoleDeactivator = ConsoleDeactivator (consoleActivator)
        self.__blocks = (Server.block, Query.block)
        self.saveSize ()
        signal.signal (signal.SIGWINCH, self.saveSize)

    def saveSize (self, *ignored):
        self.__height, self.__width = struct.unpack ('hhhh', fcntl.ioctl(0, termios.TIOCGWINSZ , '\000' * 8)) [:2]

    def getButton (self):
        button = sys.stdin.read (1)
        if button in ('e', 'k', 'q'):
            return button

    def checkButton (self):
        if select.select ([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
            return self.getButton ()

    def refresh (self):
        """Prints the blocks with height and width left on the screen."""
        os.system ('clear')
        leftHeight = self.__height
        for block in self.__blocks:
            if leftHeight < 3:
                break
            height = block.height () if block.height () < leftHeight else leftHeight
            block.printLines (height, self.__width)
            print ()
            leftHeight -= height + 1

    def askForOperation (self):
        with self.__consoleDeactivator:
            print ()
            serverName = input ('Server: ')
            if serverName:
                opid = input ('OpId: ')
                if opid:
                    return serverName, opid

class Configuration:
    def filePath (self, default = False):
        return os.path.splitext (__file__) [0] + ('.default' if default else '') + '.conf'

    def servers (self):
        """Parses the configuration file using the ConfigParser class from default Python library. Two attempts to
        import the same class for Python 3 compatibility."""
        try:
            from ConfigParser import ConfigParser
        except ImportError:
            from configparser import ConfigParser
        configParser = ConfigParser ()
        if configParser.read (self.filePath ()):
            servers = []
            for section in configParser.sections ():
                servers.append (Server (section, configParser.get (section, 'address')))
            return servers

    def printInstructions (self):
        """Prints the default configuration file if exists."""
        print ('Please create a configuration file: ' + self.filePath ())
        try:
            with open (self.filePath (default = True)) as defaultConfigurationFile:
                print ('Like this:')
                print (defaultConfigurationFile.read ())
        except IOError: pass

if __name__ == '__main__':
    configuration = Configuration ()
    servers = configuration.servers ()
    if servers:
        button = None
        with ConsoleActivator () as console:
            while button != 'q':
                if not button:
                    printers = []
                    Server.block.reset ([server for server in servers ])
                    Query.block.reset ([operation for server in servers for operation in server.currentOperations ()])
                    console.refresh ()
                    sleep (1)
                    button = console.checkButton ()
                if button in ('e', 'k'):
                    operationInput = console.askForOperation ()
                    if operationInput:
                        operation = Query.block.findLine (operationInput)
                        if operation:
                            if button == 'e':
                                if isinstance (operation, Query):
                                    operation.printExplain ()
                                else:
                                    print ('Only queries with namespace can be explained.')
                            elif button == 'k':
                                operation.kill ()
                        else:
                            print ('Invalid operation.')
                        button = console.getButton ()
                    else:
                        button = None
    else:
        configuration.printInstructions ()
