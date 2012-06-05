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
    def __init__ (self, columnHeaders):
        self.__columnHeaders = columnHeaders
        self.__columnWidths = [6 for columnHeader in self.__columnHeaders]

    def reset (self, printables):
        self.__lines = []
        self.__lineClass = None
        for printable in printables:
            if not self.__lineClass:
                assert hasattr (printable, 'line')
                self.__lineClass = printable.__class__
            else:
                assert isinstance (printable, self.__lineClass)
            self.__lines.append (printable.line ())

    def __len__ (self):
        """Return line count plus one for header, one for blank line at buttom."""
        return len (self.__lines) + 2

    def __printLine (self, line, width, bold = False):
        """Prints the line, cuts the part after the width, sets self.__columnWidths to the longest cell."""
        for index, cell in enumerate (line):
            if width - 2 < len (self.__columnHeaders [index]):
                break
            cell = str (cell)
            if len (cell) + 2 > self.__columnWidths [index]:
                self.__columnWidths [index] = len (cell) + 2 if len (cell) + 2 < width else width - 3
            if bold and sys.stdout.isatty ():
                print ('\x1b[1m', end = '')
            print (cell.ljust (self.__columnWidths [index]) [:self.__columnWidths [index]], end = '')
            if bold and sys.stdout.isatty ():
                print ('\x1b[0m', end = '')
            width -= self.__columnWidths [index]
        print ()

    def printLines (self, height, width):
        """Prints the lines set with reset, cuts the ones after the height."""
        assert height > 2
        self.__printLine (self.__columnHeaders, width, True)
        height -= 1
        for line in self.__lines:
            if not height:
                break
            assert len (line) <= len (self.__columnHeaders)
            height -= 1
            self.__printLine (line, width)

    def findLine (self, cells):
        """Returns the printable from self.__lineClass saved with reset."""
        for line in self.__lines:
            different = False
            index = 0
            while not different and len (cells) > index:
                if str (line [index]) != cells [index]:
                    different = True
                index += 1
            if not different:
                return self.__lineClass (*line)

class Operation:
    def __init__ (self, server, opid, namespace = None, duration = None, query = None):
        self.__server = server
        self.__opid = opid
        self.__namespace = namespace
        self.__duration = duration
        self.__query = json.loads (query, object_hook = json_util.object_hook) if isinstance (query, str) else query

    def sortOrder (self):
        return self.__duration if self.__duration is not None else -1

    def line (self):
        cells = []
        cells.append (self.__server)
        cells.append (self.__opid)
        cells.append (self.__namespace) if self.__namespace is not None else None
        cells.append (self.__duration) if self.__duration is not None else None
        cells.append (json.dumps (self.__query, default = json_util.default)) if self.__query is not None else None
        return cells

    def kill (self):
        return self.__server.killOperation (self.__opid)

    def printExplain (self):
        """Prints the output of the explain command executed on the server of the query."""
        if self.__namespace and self.__query:
            databaseName, collectionName = self.__namespace.split ('.', 1)
            explainOutput = self.__server.explainQuery (databaseName, collectionName, self.__query)
            print ('Cursor:', explainOutput ['cursor'])
            print ('Indexes:', end = ' ')
            for index in explainOutput ['indexBounds']:
                print (index, end = ' ')
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
            print ('Query:', json.dumps (self.__query, default = json_util.default, sort_keys = True, indent = 4))
        else:
            print ('Only queries with namespace can be explained.')

class Server:
    def __init__ (self, name, address):
        self.__name = name
        self.__address = address
        self.__connection = pymongo.Connection (address)
        self.__operationCount = 0
        self.__flushCount = 0

    def __getOperationCountChange (self, operationCounts):
        oldOperationCount = self.__operationCount
        self.__operationCount = sum ([value for key, value in operationCounts.items ()])
        return self.__operationCount - oldOperationCount

    def __getFlushCountChange (self, flushCount):
        oldFlushCount = self.__flushCount
        self.__flushCount = flushCount
        return self.__flushCount - oldFlushCount

    def line (self):
        success = False
        while not success:
            try:
                serverStatus = self.__connection.admin.command ('serverStatus')
                success = True
            except pymongo.errors.AutoReconnect: pass

        currentConnection = Value (serverStatus ['connections'] ['current'])
        totalConnection = Value (serverStatus ['connections'] ['available'] + serverStatus ['connections'] ['current'])
        residentMem = Value (serverStatus ['mem'] ['resident'] * (10 ** 6))
        mappedMem = Value (serverStatus ['mem'] ['mapped'] * (10 ** 6))
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
                duration = op ['secs_running'] if 'secs_running' in op else 0
                yield Operation (self, op ['opid'], op ['ns'], duration, op ['query'])
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
        try:
            self.__settings = termios.tcgetattr (sys.stdin)
            tty.setcbreak (sys.stdin.fileno())
        except termios.error:
            self.__settings = None
        return Console (self)

    def __exit__ (self, *ignored):
        if self.__settings:
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
        self.saveSize ()
        signal.signal (signal.SIGWINCH, self.saveSize)

    def saveSize (self, *ignored):
        try:
            self.__height, self.__width = struct.unpack ('hhhh', fcntl.ioctl(0, termios.TIOCGWINSZ , '\000' * 8)) [:2]
        except IOError:
            self.__height, self.__width = 20, 80

    def getButton (self):
        button = sys.stdin.read (1)
        if button in ('e', 'k', 'q'):
            return button

    def checkButton (self):
        if select.select ([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
            return self.getButton ()

    def refresh (self, blocks):
        """Prints the blocks with height and width left on the screen."""
        os.system ('clear')
        leftHeight = self.__height
        for block in blocks:
            if leftHeight < 3:
                break
            height = len (block) if len (block) < leftHeight else leftHeight
            assert hasattr (block, 'printLines')
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

class QueryScreen:
    def __init__ (self, console, servers):
        self.__console = console
        self.__servers = servers
        self.__serverBlock = Block (('Server', 'QPS', 'Clients', 'Queue', 'Flushes', 'Connections', 'Memory'))
        self.__queryBlock = Block (('Server', 'OpId', 'Namespace', 'Sec', 'Query'))

    def refresh (self):
        self.__serverBlock.reset ([server for server in self.__servers ])
        operations = [operation for server in self.__servers for operation in server.currentOperations ()]
        self.__queryBlock.reset (sorted (operations, key = lambda operation: operation.sortOrder (), reverse = True))
        self.__console.refresh ((self.__serverBlock, self.__queryBlock))

    def action (self, button):
        while button in ('e', 'k'):
            operationInput = self.__console.askForOperation ()
            if operationInput:
                operation = self.__queryBlock.findLine (operationInput)
                if operation:
                    if button == 'e':
                        operation.printExplain ()
                    elif button == 'k':
                        operation.kill ()
                    button = self.__console.getButton ()
            else:
                button = None

if __name__ == '__main__':
    configuration = Configuration ()
    servers = configuration.servers ()
    if servers:
        button = None
        with ConsoleActivator () as console:
            queryScreen = QueryScreen (console, servers)
            while button != 'q':
                queryScreen.refresh ()
                sleep (1)
                button = console.checkButton ()
                queryScreen.action (button)
    else:
        configuration.printInstructions ()
