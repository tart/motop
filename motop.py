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
        """Print the line, cuts the part after the width, sets self.__columnWidths to the longest cell."""
        for index, cell in enumerate (line):
            if width <= len (self.__columnHeaders [index]):
                break
            cell = str (cell)
            if len (cell) + 2 >= self.__columnWidths [index]:
                self.__columnWidths [index] = len (cell) + 2 if len (cell) + 2 < width else width
            if bold and sys.stdout.isatty ():
                print ('\x1b[1m', end = '')
            print (cell.ljust (self.__columnWidths [index]) [:self.__columnWidths [index]], end = '')
            if bold and sys.stdout.isatty ():
                print ('\x1b[0m', end = '')
            width -= self.__columnWidths [index]
        print ()

    def printLines (self, height, width):
        """Print the lines set with reset, cuts the ones after the height."""
        assert height > 2
        self.__printLine (self.__columnHeaders, width, True)
        height -= 1
        for line in self.__lines:
            if height <= 1:
                break
            assert len (line) <= len (self.__columnHeaders)
            height -= 1
            self.__printLine (line, width)

    def findLines (self, condition):
        """Return the printables from self.__lineClass saved with reset."""
        return [self.__lineClass (*line) for line in self.__lines if condition (line)]

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
        """Print the query and the output of the explain command executed on the server."""
        if self.__namespace and self.__query:
            print ('Query:', json.dumps (self.__query, default = json_util.default, indent = 4))
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

    def __execute (self, procedure, *arguments):
        """Try 10 times to execute the procedure."""
        for tryCount in range (10):
            try:
                return procedure (*arguments)
            except pymongo.errors.AutoReconnect: pass

    def line (self):
        serverStatus = self.__execute (self.__connection.admin.command, 'serverStatus')
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
        cursor = self.__execute (collection.find, query)
        return self.__execute (cursor.explain)

    def currentOperations (self):
        for op in self.__execute (self.__connection.admin.current_op) ['inprog']:
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
        self.__saveSize ()
        signal.signal (signal.SIGWINCH, self.__saveSize)

    def __saveSize (self, *ignored):
        try:
            self.__height, self.__width = struct.unpack ('hhhh', fcntl.ioctl(0, termios.TIOCGWINSZ , '\000' * 8)) [:2]
        except IOError:
            self.__height, self.__width = 20, 80

    def checkButton (self, waitTime = None):
        """Check one character input. Waits for approximately waitTime parameter as seconds. Waits for input if no
        parameter given."""
        if waitTime:
            for timer in range (waitTime * 10):
                sleep (0.1)
                if select.select ([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
                    return sys.stdin.read (1)
        else:
            return sys.stdin.read (1)

    def refresh (self, blocks):
        """Print the blocks with height and width left on the screen."""
        os.system ('clear')
        leftHeight = self.__height
        for block in blocks:
            """Do not show the block if there are not left lines for header and a row"""
            if leftHeight <= 2:
                break
            height = len (block) if len (block) < leftHeight else leftHeight
            assert hasattr (block, 'printLines')
            block.printLines (height, self.__width)
            leftHeight -= height
            if leftHeight >= 2:
                print ()
                leftHeight -= 1

    def askForInput (self, *attributes):
        """Ask for input for given attributes in given order."""
        with self.__consoleDeactivator:
            print ()
            values = []
            for attribute in attributes:
                value = input (attribute + ': ')
                if not value:
                    break
                values.append (value)
        return values

class Configuration:
    def filePath (self, default = False):
        return os.path.splitext (__file__) [0] + ('.default' if default else '') + '.conf'

    def servers (self):
        """Parse the configuration file using the ConfigParser class from default Python library. Two attempts to
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
        """Print the default configuration file if exists."""
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
        """Perform actions for the pressed button."""
        while button in ('e', 'k'):
            """Kill or explain single operation."""
            operationInput = self.__console.askForInput ('Server', 'OpId')
            if len (operationInput) == 2:
                condition = lambda (line): str (line [0]) == operationInput [0] and str (line [1]) == operationInput [1]
                operations = self.__queryBlock.findLines (condition)
                if len (operations) == 1:
                    operation = operations [0]
                    if button == 'e':
                        operation.printExplain ()
                    elif button == 'k':
                        operation.kill ()
                    button = self.__console.checkButton ()
            else:
                button = None
        if button == 'K':
            """Batch kill operations."""
            durationInput = self.__console.askForInput ('Sec')
            if len (durationInput) == 1:
                condition = lambda (line): len (line) >= 3 and line [3] > int (durationInput [0])
                operations = self.__queryBlock.findLines (condition)
                for operation in operations:
                    operation.kill ()

if __name__ == '__main__':
    """Run the main program."""
    try:
        configuration = Configuration ()
        servers = configuration.servers ()
        if servers:
            button = None
            with ConsoleActivator () as console:
                queryScreen = QueryScreen (console, servers)
                while button != 'q':
                    queryScreen.refresh ()
                    button = console.checkButton (1)
                    try:
                        queryScreen.action (button)
                    except KeyboardInterrupt: pass
        else:
            configuration.printInstructions ()
    except KeyboardInterrupt: pass
