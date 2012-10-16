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
# The software is provided "as is" and the author disclaims all warranties with regard to the software including all
# implied warranties of merchantability and fitness. In no event shall the author be liable for any special, direct,
# indirect, or consequential damages or any damages whatsoever resulting from loss of use, data or profits, whether in
# an action of contract, negligence or other tortious action, arising out of or in connection with the use or
# performance of this software.
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
from datetime import datetime, timedelta

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
    def __init__ (self, *columnHeaders):
        self.__columnHeaders = columnHeaders
        self.__columnWidths = [6] * len (self.__columnHeaders)

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
        """Print the cells separated by 2 spaces, cut the part after the width."""
        for index, cell in enumerate (line):
            if width < len (self.__columnHeaders [index]):
                break
            cell = str (cell) if cell is not None else ''
            self.__columnWidths [index] = min (width, max (len (cell) + 2, self.__columnWidths [index]))
            if bold and sys.stdout.isatty ():
                print ('\x1b[1m', end = '')
            print (cell.ljust (self.__columnWidths [index]) [:self.__columnWidths [index]], end = '')
            if bold and sys.stdout.isatty ():
                print ('\x1b[0m', end = '')
            width -= self.__columnWidths [index]
        print ()

    def printLines (self, height, width):
        """Print the lines set with reset, cuts the ones after the height."""
        assert height > 1
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
    def __init__ (self, server, opid, state, duration = None, namespace = None, query = None):
        self.__server = server
        self.__opid = opid
        self.__state = state
        self.__duration = duration
        self.__namespace = namespace
        self.__query = json.loads (query, object_hook = json_util.object_hook) if isinstance (query, str) else query

    def sortOrder (self):
        return self.__duration if self.__duration is not None else -1

    block = Block ('Server', 'Opid', 'State', 'Sec', 'Namespace', 'Query')

    def line (self):
        cells = []
        cells.append (self.__server)
        cells.append (self.__opid)
        cells.append (self.__state)
        cells.append (self.__duration)
        cells.append (self.__namespace)
        if self.__query:
            if '$msg' in self.__query:
                cells.append (self.__query ['$msg'])
            else:
                cells.append (json.dumps (self.__query, default = json_util.default))
        return cells

    def kill (self):
        return self.__server.killOperation (self.__opid)

    def __queryParts (self):
        """Translate query parts to arguments of pymongo find method."""
        assert isinstance (self.__query, dict)
        if any ([key in ('query', '$query') for key in self.__query.keys ()]):
            queryParts = {}
            for key, value in self.__query.items ():
                if key in ('query', '$query'):
                    queryParts ['spec'] = value
                elif key in ('explain', '$explain'):
                    queryParts ['explain'] = True
                elif key in ('orderby', '$orderby'):
                    queryParts ['sort'] = [(key, value) for key, value in value.items ()]
                else:
                    raise Exception ('Unknown query part: ' + key)
            return queryParts
        return {'spec': self.__query}

    def examine (self):
        """Print the query parts."""
        queryParts = self.__queryParts ()
        for key, value in queryParts.items ():
            print (key.title () + ':', end = ' ')
            if isinstance (value, list):
                print (', '.join ([pair [0] + ': ' + str (pair [1]) for pair in value]))
            elif isinstance (value, dict):
                print (json.dumps (value, default = json_util.default, indent = 4))
            else:
                print (value)

    def explain (self):
        """Print the output of the explain command executed on the server."""
        if self.__namespace:
            databaseName, collectionName = self.__namespace.split ('.', 1)
            queryParts = self.__queryParts ()
            assert 'explain' not in queryParts
            explainOutput = self.__server.explainQuery (databaseName, collectionName, **queryParts)
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

class ReplicaSetMember:
    def __init__ (self, replicaSet, name, state, uptime, lag, increment, ping, server = None):
        self.__replicaSet = replicaSet
        self.__name = name
        self.__state = state.lower ()
        self.__uptime = uptime
        self.__lag = lag
        self.__increment = increment
        self.__ping = ping
        self.__server = server

    def __str__ (self):
        return self.__name

    def revise (self, otherMember):
        """Merge properties of the other replica set member with following rules."""
        if otherMember.__uptime is not None:
            if self.__uptime is None or self.__uptime < otherMember.__uptime:
                self.__uptime = otherMember.__uptime
        if otherMember.__replicaSet.masterState ():
            self.__lag = otherMember.__lag
        if self.__increment < otherMember.__increment:
            self.__increment = otherMember.__increment
        if otherMember.__ping is not None:
            if self.__ping is None or self.__ping < otherMember.__ping:
                self.__ping = otherMember.__ping
        if otherMember.__server is not None and self.__server is None:
            self.__server = otherMember.__server

    block = Block ('Server', 'Set', 'State', 'Uptime', 'Lag', 'Inc', 'Ping')

    def line (self):
        cells = []
        cells.append (str (self.__server) if self.__server else self.__name)
        cells.append (str (self.__replicaSet))
        cells.append (self.__state)
        cells.append (self.__uptime)
        cells.append (self.__lag)
        cells.append (self.__increment)
        cells.append (self.__ping)
        return cells

class ReplicaSet:
    def __init__ (self, name, state):
        self.__name = name
        self.__state = state
        self.__members = []

    def __str__ (self):
        return self.__name

    def masterState (self):
        return self.__state == 1

    def addMember (self, *args):
        self.__members.append (ReplicaSetMember (self, *args))

    def members (self):
        return self.__members

    def findMember (self, name):
        for member in self.__members:
            if str (member) == name:
                return member

    def revise (self, other):
        for member in self.__members:
            member.revise (other.findMember (str (member)))

class Server:
    __readPreference = pymongo.ReadPreference.SECONDARY
    def __init__ (self, name, address, hideReplicationOperations = False):
        self.__name = name
        self.__address = address
        self.__port = 27017
        self.__hideReplicationOperations = hideReplicationOperations
        self.__connection = pymongo.Connection (address, read_preference = self.__readPreference)
        self.__oldValues = {}

    def __str__ (self):
        return self.__name

    class ExecuteFailure (Exception): pass

    def __execute (self, procedure, *args, **kwargs):
        """Try 10 times to execute the procedure."""
        tryCount = 1
        while True:
            try:
                return procedure (*args, **kwargs)
            except pymongo.errors.AutoReconnect:
                tryCount += 1
                if tryCount >= 10:
                    raise self.ExecuteFailure ()
            except pymongo.errors.OperationFailure:
                raise self.ExecuteFailure ()

    def __getStatus (self):
        """Get serverStatus from MongoDB, calculate time difference with the last time."""
        status = self.__execute (self.__connection.admin.command, 'serverStatus')
        oldCheckTime = self.__oldValues ['checkTime'] if 'checkTime' in self.__oldValues else None
        self.__oldValues ['checkTime'] = datetime.now ()
        if oldCheckTime:
            self.__timespan = self.__oldValues ['checkTime'] - oldCheckTime
        return status

    def __statusChangePerSecond (self, name, value):
        """Calculate the difference of the value in one second with the last time by using time difference calculated
        on __getStatus."""
        oldValue = self.__oldValues [name] if name in self.__oldValues else None
        self.__oldValues [name] = value
        if oldValue:
            timespanSeconds = self.__timespan.seconds + (self.__timespan.microseconds / (10.0 ** 6))
            return Value ((value - oldValue) / timespanSeconds)

    block = Block ('Server', 'QPS', 'Client', 'Queue', 'Flush', 'Connection', 'Memory', 'Network I/O')

    def line (self):
        serverStatus = self.__getStatus ()
        currentConnection = Value (serverStatus ['connections'] ['current'])
        totalConnection = Value (serverStatus ['connections'] ['available'] + serverStatus ['connections'] ['current'])
        residentMem = Value (serverStatus ['mem'] ['resident'] * (10 ** 6))
        mappedMem = Value (serverStatus ['mem'] ['mapped'] * (10 ** 6))
        opcounters = serverStatus ['opcounters']
        networkInChange = self.__statusChangePerSecond ('networkIn', serverStatus ['network'] ['bytesIn'])
        networkOutChange = self.__statusChangePerSecond ('networkOut', serverStatus ['network'] ['bytesOut'])
        cells = []
        cells.append (str (self))
        cells.append (self.__statusChangePerSecond ('operation', sum (opcounters.values ())))
        cells.append (Value (serverStatus ['globalLock'] ['activeClients'] ['total']))
        cells.append (Value (serverStatus ['globalLock'] ['currentQueue'] ['total']))
        cells.append (self.__statusChangePerSecond ('flush', serverStatus ['backgroundFlushing'] ['flushes']))
        cells.append (str (currentConnection) + ' / ' + str (totalConnection))
        cells.append (str (residentMem) + ' / ' + str (mappedMem))
        cells.append (str (networkInChange) + ' / ' + str (networkOutChange))
        return cells

    def replicaSet (self):
        replicaSetStatus = self.__execute (self.__connection.admin.command, 'replSetGetStatus')
        replicaSet = ReplicaSet (replicaSetStatus ['set'], replicaSetStatus ['myState'])
        for member in replicaSetStatus ['members']:
            uptime = timedelta (seconds = member ['uptime']) if 'uptime' in member else None
            ping = member ['pingMs'] if 'pingMs' in member else None
            lag = replicaSetStatus ['date'] - member ['optimeDate']
            optime = member ['optime']
            if member ['name'] == self.__address + ':' + str (self.__port):
                replicaSet.addMember (member ['name'], member ['stateStr'], uptime, lag, optime.inc, ping, self)
            else:
                replicaSet.addMember (member ['name'], member ['stateStr'], uptime, lag, optime.inc, ping)
        return replicaSet

    def currentOperations (self):
        """Execute currentOp operation on the server. Filter and yield returning operations."""
        for op in self.__execute (self.__connection.admin.current_op) ['inprog']:
            if self.__hideReplicationOperations and op ['op'] == 'getmore' and 'local.oplog.' in op ['ns']:
                """Condition to find replication operation on the master."""
                continue
            if self.__hideReplicationOperations and op ['op'] and op ['ns'] in ('', 'local.sources'):
                """Condition to find replication operation on the slave. Do not look for more replication
                operations if one found."""
                continue
            duration = op ['secs_running'] if 'secs_running' in op else None
            yield Operation (self, op ['opid'], op ['op'], duration, op ['ns'], op ['query'] or None)

    def explainQuery (self, databaseName, collectionName, **kwargs):
        collection = getattr (getattr (self.__connection, databaseName), collectionName)
        cursor = self.__execute (collection.find, **kwargs)
        return self.__execute (cursor.explain)

    def killOperation (self, opid):
        """Kill operation using the "mongo" executable on the shell. That is because I could not make it with
        pymongo."""
        os.system ('echo "db.killOp (' + str (opid) + ')" | mongo --host ' + self.__address)

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
            while waitTime > 0:
                waitTime -= 0.1
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
    __filePath = os.path.splitext (__file__) [0] + '.conf'
    __defaultFilePath = os.path.splitext (__file__) [0] + '.default.conf'
    __booleanVariables = {'status': 'on', 'replicaSet': 'on', 'hideReplicationOperations': 'off'}

    def printInstructions (self):
        """Print the default configuration file if exists."""
        print ('Please create a configuration file: ' + self.__filePath)
        try:
            with open (self.__defaultFilePath) as defaultConfigurationFile:
                print ('Like this:')
                print (defaultConfigurationFile.read ())
        except IOError: pass

    def __init__ (self):
        """Parse the configuration file using the ConfigParser class from default Python library. Two attempts to
        import the same class for Python 3 compatibility."""
        try:
            from ConfigParser import SafeConfigParser
        except ImportError:
            from configparser import SafeConfigParser
        self.__configParser = SafeConfigParser (self.__booleanVariables)
        self.__configParser.read (self.__filePath)

    def sections (self):
        if self.__configParser:
            return self.__configParser.sections ()

    def booleanVariableTrueSections (self, variable):
        assert variable in self.__booleanVariables
        return [section for section in self.sections () if self.__configParser.getboolean (section, variable)]

    def servers (self):
        servers = []
        for section in self.sections ():
            servers.append (Server (section, self.__configParser.get (section, 'address'),
                                    self.booleanVariableTrueSections ('hideReplicationOperations')))
        return servers

class QueryScreen:
    def __init__ (self, console, servers, activeStatus, activeReplicaSet):
        self.__console = console
        self.__servers = servers
        self.__activeStatus = activeStatus
        self.__activeReplicaSet = activeReplicaSet
        self.__blocks = []
        if any ([str (server) in activeStatus for server in servers]):
            self.__blocks.append (Server.block)
        if any ([str (server) in activeReplicaSet for server in servers]):
            self.__blocks.append (ReplicaSetMember.block)
        self.__blocks.append (Operation.block)

    def __replicaSets (self):
        """Return unique replica sets of the servers."""
        replicaSets = []
        def add (replicaSet):
            """Merge same replica sets by revising the existent one."""
            for existentReplicaSet in replicaSets:
                if str (existentReplicaSet) == str (replicaSet):
                    return existentReplicaSet.revise (replicaSet)
            return replicaSets.append (replicaSet)
        for server in self.__servers:
            if str (server) in self.__activeReplicaSet:
                try:
                    add (server.replicaSet ())
                except Server.ExecuteFailure:
                    self.__activeReplicaSet.remove (str (server))
                    if not any ([str (server) in self.__activeReplicaSet for server in self.__servers]):
                        self.__blocks.remove (ReplicaSetMember.block)
        return replicaSets

    def __refresh (self):
        Server.block.reset (server for server in self.__servers if str (server) in self.__activeStatus)
        ReplicaSetMember.block.reset ([member for replicaSet in self.__replicaSets () if str for member in replicaSet.members ()])
        operations = [operation for server in self.__servers for operation in server.currentOperations ()]
        Operation.block.reset (sorted (operations, key = lambda operation: operation.sortOrder (), reverse = True))
        self.__console.refresh (self.__blocks)

    def __askForOperation (self):
        operationInput = self.__console.askForInput ('Server', 'Opid')
        if len (operationInput) == 2:
            condition = lambda line: str (line [0]) == operationInput [0] and str (line [1]) == operationInput [1]
            operations = Operation.block.findLines (condition)
            if len (operations) == 1:
                return operations [0]

    def __explainAction (self):
        operation = self.__askForOperation ()
        if operation:
            operation.examine ()
            operation.explain ()

    def __killAction (self):
        operation = self.__askForOperation ()
        if operation:
            operation.kill ()

    def __batchKillAction (self):
        durationInput = self.__console.askForInput ('Sec')
        if durationInput:
            condition = lambda line: len (line) >= 3 and line [3] > int (durationInput [0])
            operations = Opeation.block.findLines (condition)
            for operation in operations:
                operation.kill ()

    def action (self):
        """Refresh the screen, perform actions for the pressed button."""
        button = None
        while button != 'q':
            self.__refresh ()
            button = self.__console.checkButton (1)
            while button in ('e', 'k'):
                if button == 'e':
                    self.__explainAction ()
                elif button == 'k':
                    self.__killAction ()
                button = self.__console.checkButton ()
            if button == 'K':
                self.__batchKillAction ()

if __name__ == '__main__':
    """Run the main program."""
    configuration = Configuration ()
    if configuration.sections ():
        with ConsoleActivator () as console:
            queryScreen = QueryScreen (console, configuration.servers (),
                                       configuration.booleanVariableTrueSections ('status'),
                                       configuration.booleanVariableTrueSections ('replicaSet'))
            try:
                queryScreen.action ()
            except KeyboardInterrupt: pass
    else:
        configuration.printInstructions ()
