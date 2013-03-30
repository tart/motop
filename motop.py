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
import signal
import json
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

class ExecuteFailure (Exception):
    def __init__ (self, procedure):
        self.__procedure = procedure

    def __str__ (self):
        return str (self.__procedure)

class Server:
    defaultPort = 27017
    readPreference = pymongo.ReadPreference.SECONDARY

    def __connect (self):
        try:
            if pymongo.version_tuple >= (2, 4):
                self.__connection = pymongo.MongoClient (self.__address, read_preference = self.readPreference)
            else:
                self.__connection = pymongo.Connection (self.__address, read_preference = self.readPreference)
        except pymongo.errors.AutoReconnect as error:
            self.__connection = None
            self.__lastError = error
        if self.__username and self.__password:
            self.__connection.admin.authenticate (self.__username, self.__password)

    def __init__ (self, name, address, username, password):
        self.__name = name
        self.__address = address
        self.__username = username
        self.__password = password
        self.__oldValues = {}
        self.__connect ()

    def __str__ (self):
        return self.__name

    def sameServer (self, name):
        if self.__name == name:
            return True
        if self.__address == name:
            return True
        if ':' not in self.__address and self.__address + ':' + str (self.defaultPort) == name:
            return True
        return False

    def connected (self):
        return bool (self.__connection)

    def __execute (self, procedure, *args, **kwargs):
        """Try 10 times to execute the procedure."""
        tryCount = 1
        while tryCount < 10:
            tryCount += 1
            try:
                return procedure (*args, **kwargs)
            except pymongo.errors.AutoReconnect as error:
                self.__lastError = error
                sleep (0.1)
            except pymongo.errors.OperationFailure as error:
                self.__lastError = error
                break
        raise ExecuteFailure (procedure)

    def lastError (self):
        return self.__lastError

    def __statusChangePerSecond (self, name, value):
        """Calculate the difference of the value in one second with the last time by using time difference calculated
        on __getStatus."""
        oldValue = self.__oldValues [name] if name in self.__oldValues else None
        self.__oldValues [name] = value
        if oldValue:
            timespanSeconds = self.__timespan.seconds + (self.__timespan.microseconds / (10.0 ** 6))
            return Value ((value - oldValue) / timespanSeconds)
        return 0

    def status (self):
        """Get serverStatus from MongoDB, calculate time difference with the last time."""
        status = self.__execute (self.__connection.admin.command, 'serverStatus')
        oldCheckTime = self.__oldValues ['checkTime'] if 'checkTime' in self.__oldValues else None
        self.__oldValues ['checkTime'] = datetime.now ()
        if oldCheckTime:
            self.__timespan = self.__oldValues ['checkTime'] - oldCheckTime
        values = {}
        opcounters = status ['opcounters']
        values ['qPS'] = self.__statusChangePerSecond ('qPS', sum (opcounters.values ()))
        values ['activeClients'] = Value (status ['globalLock'] ['activeClients'] ['total'])
        values ['currentQueue'] = Value (status ['globalLock'] ['currentQueue'] ['total'])
        values ['flushes'] = self.__statusChangePerSecond ('flushes', status ['backgroundFlushing'] ['flushes'])
        values ['currentConn'] = Value (status ['connections'] ['current'])
        values ['totalConn'] = Value (status ['connections'] ['available'] + status ['connections'] ['current'])
        values ['residentMem'] = Value (status ['mem'] ['resident'] * (10 ** 6))
        values ['mappedMem'] = Value (status ['mem'] ['mapped'] * (10 ** 6))
        values ['bytesIn'] = self.__statusChangePerSecond ('bytesIn', status ['network'] ['bytesIn'])
        values ['bytesOut'] = self.__statusChangePerSecond ('bytesOut', status ['network'] ['bytesOut'])
        return values

    def replicationInfo (self):
        """Find replication source from the local collection."""
        sources = self.__execute (self.__connection.local.sources.find)
        for source in sources:
            values = {}
            values ['source'] = source ['host']
            values ['sourceType'] = source ['source']
            syncedTo = source ['syncedTo']
            values ['syncedTo'] = syncedTo.as_datetime ()
            values ['increment'] = syncedTo.inc
            return values

    def replicaSetMembers (self):
        """Execute replSetGetStatus operation on the server. Filter arbiters. Calculate the lag. Add relation to the
        member which is the server itself. Return the replica set."""
        try:
            replicaSetStatus = self.__execute (self.__connection.admin.command, 'replSetGetStatus')
        except ExecuteFailure: pass
        else:
            for member in replicaSetStatus ['members']:
                if 'statusStr' not in member or member ['statusStr'] not in ['ARBITER']:
                    values ['set'] = replicaSetStatus ['set']
                    values ['name'] = member ['name']
                    values ['state'] = member ['stateStr']
                    values ['uptime'] = timedelta (seconds = member ['uptime']) if 'uptime' in member else None
                    values ['ping'] = member ['pingMs'] if 'pingMs' in member else None
                    if 'optime' in member and 'optimeDate' in member:
                        values ['lag'] = replicaSetStatus ['date'] - member ['optimeDate']
                        values ['optime'] = member ['optime']
                    yield values

    def currentOperations (self, hideReplicationOperations = False):
        """Execute currentOp operation on the server. Filter and yield returning operations."""
        for op in self.__execute (self.__connection.admin.current_op) ['inprog']:
            if hideReplicationOperations:
                if op ['op'] == 'getmore' and 'local.oplog.' in op ['ns']:
                    """Condition to find replication operation on the master."""
                    continue
                if op ['op'] and op ['ns'] in ('', 'local.sources'):
                    """Condition to find replication operation on the slave. Do not look for more replication
                    operations if one found."""
                    continue
            values = {}
            values ['opid'] = op ['opid']
            values ['state'] = op ['op']
            values ['duration'] = op ['secs_running'] if 'secs_running' in op else None
            values ['namespace'] = op ['ns']
            if "query" in op:
                if isinstance (op ['query'], str) and op ['query'] [0] == '{' and op ['query'] [-1] == '}':
                    values ['query'] = json.loads (op ['query'], object_hook = json_util.object_hook)
                else:
                    values ['query'] = op ['query']
            yield values

    def explainQuery (self, namespace, findParameters):
        databaseName, collectionName = namespace.split ('.', 1)
        collection = getattr (getattr (self.__connection, databaseName), collectionName)
        cursor = self.__execute (collection.find, **findParameters)
        return self.__execute (cursor.explain)

    def killOperation (self, opid):
        """Kill operation using the "mongo" executable on the shell. That is because I could not make it with
        pymongo."""
        command = "echo 'db.killOp ({0})' | mongo".format (str (opid))
        command += ' ' + self.__address + '/admin'
        if self.__username:
            command += ' --username ' + self.__username
        if self.__password:
            command += ' --password ' + self.__password
        os.system (command)

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

    def checkButton (self, waitTime = None):
        """Check one character input. Waits for approximately waitTime parameter as seconds. Wait for input if no
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
            if not len (block):
                """Do not show the block if there are no lines."""
                continue
            if leftHeight <= 2:
                """Do not show the block if there are not enough lines left for header and a row."""
                break
            height = len (block) + 2 if len (block) + 2 < leftHeight else leftHeight
            block.print (height, self.__width)
            leftHeight -= height
            if leftHeight >= 2:
                print ()
                leftHeight -= 1

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

class StatusBlock (Block):
    columnHeaders = ['Server', 'QPS', 'Client', 'Queue', 'Flush', 'Connection', 'Memory', 'Network I/O']

    def __init__ (self, servers):
        Block.__init__ (self, self.columnHeaders)
        self.__servers = servers

    def reset (self):
        lines = []
        for server in self.__servers:
            cells = []
            cells.append (server)
            if server.connected ():
                status = server.status ()
                cells.append (status ['qPS'])
                cells.append (status ['activeClients'])
                cells.append (status ['currentQueue'])
                cells.append (status ['flushes'])
                cells.append ((status ['currentConn'], status ['totalConn']))
                cells.append ((status ['residentMem'], status ['mappedMem']))
                cells.append ((status ['bytesIn'], status ['bytesOut']))
            else:
                cells.append (server.lastError ())
            lines.append (cells)
        Block.reset (self, lines)

class ServerBasedBlock (Block):
    def __init__ (self, servers):
        Block.__init__ (self, self.columnHeaders)
        self.__servers = servers
        self.__hiddenServers = []

    def findServer (self, name):
        for server in self.__servers:
            if server.sameServer (name):
                return server

    def connectedServers (self):
        return [server for server in self.__servers if server.connected () and server not in self.__hiddenServers]

    def hideServer (self, server):
        self.__hiddenServers.append (server)

class ReplicationInfoBlock (ServerBasedBlock):
    columnHeaders = ['Server', 'Source', 'SyncedTo', 'Inc']

    def reset (self):
        lines = []
        for server in self.connectedServers ():
            replicationInfo = server.replicationInfo ()
            if replicationInfo:
                cells = []
                cells.append (server)
                cells.append ((replicationInfo ['sourceType'], self.findServer (replicationInfo ['source'])))
                cells.append (replicationInfo ['syncedTo'])
                cells.append (replicationInfo ['increment'])
                lines.append (cells)
            else:
                self.hideServer (server)
        Block.reset (self, lines)

class ReplicaSetMemberBlock (ServerBasedBlock):
    columnHeaders = ['Server', 'Set', 'State', 'Uptime', 'Lag', 'Inc', 'Ping']

    def __add (self, line):
        """Merge same lines by revising the existent one."""
        for existentLine in self.__lines:
            if existentLine ['set'] == line ['set'] and existentLine ['name'] == line ['name']:
                for key in line.keys ():
                    if line [key]:
                        if existentLine [key] < line [key]:
                            existentLine [key] = line [key]
                return
        self.__lines.append (line)

    def reset (self):
        self.__lines = []
        for server in self.connectedServers ():
            replicaSetMembers = server.replicaSetMembers ()
            if replicaSetMembers:
                for member in replicaSetMembers:
                    cells = []
                    cells.append (self.findServer (member ['name']) or member ['name'])
                    cells.append (member ['set'])
                    cells.append (member ['state'])
                    cells.append (member ['uptime'])
                    cells.append (member ['ping'])
                    if 'lag' in member:
                        cells.append (member ['lag'])
                    if 'optime' in member:
                        cells.append (member ['optime'])
                    self.add (cells)
            else:
                self.hideServer (server)
        Block.reset (self, self.__lines)

class Query:
    def __init__ (self, **parts):
        """Translate query parts to arguments of pymongo find method."""
        self.__parts = {}
        if any ([key in ('query', '$query') for key in parts.keys ()]):
            for key, value in parts.items ():
                if key [0] == '$':
                    key = key [1:]
                if key == 'query':
                    key = 'spec'
                if key == 'orderby':
                    key = 'sort'
                    value = list (value.items ())
                if key == 'explain':
                    self.__explain = True
                self.__parts [key] = value
        else:
            self.__parts ['spec'] = parts

    def __str__ (self):
        return json.dumps (self.__parts, default = json_util.default)

    def print (self):
        """Print formatted query parts."""
        for key, value in self.__parts.items ():
            print (key.title () + ':', end = ' ')
            if isinstance (value, list):
                print (', '.join ([pair [0] + ': ' + str (pair [1]) for pair in value]))
            elif isinstance (value, dict):
                print (json.dumps (value, default = json_util.default, indent = 4))
            else:
                print (value)

    def printExplain (self, server, namespace):
        """Print the output of the explain command executed on the server."""
        explainOutput = server.explainQuery (namespace, self.__parts)
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

class OperationBlock (Block):
    columnHeaders = ['Server', 'Opid', 'State', 'Sec', 'Namespace', 'Query']

    def __init__ (self, servers, replicationOperationServers):
        Block.__init__ (self, self.columnHeaders)
        self.__servers = servers
        self.__replicationOperationServers = replicationOperationServers

    def reset (self):
        self.__lines = []
        for server in self.__servers:
            if server.connected ():
                hideReplicationOperations = server not in self.__replicationOperationServers
                for operation in server.currentOperations (hideReplicationOperations):
                    cells = []
                    cells.append (server)
                    cells.append (operation ['opid'])
                    cells.append (operation ['state'])
                    cells.append (operation ['duration'])
                    cells.append (operation ['namespace'])
                    if operation ['query']:
                        if '$msg' in operation ['query']:
                            cells.append (operation ['query'] ['$msg'])
                        else:
                            cells.append (Query (**operation ['query']))
                    self.__lines.append (cells)
        self.__lines.sort (key = lambda line: line [3] or -1, reverse = True)
        Block.reset (self, self.__lines)

    def __findServer (self, serverName):
        for server in self.__servers:
            if str (server) == serverName:
                return server

    def __findLine (self, serverName, opid):
        for line in self.__lines:
            if str (line [0]) == serverName and str (line [1]) == opid:
                return line

    def explainQuery (self, *parameters):
        line = self.__findLine (*parameters)
        if line [4] and line [5] and isinstance (line [5], Query):
            query = line [5]
            query.print ()
            query.printExplain (line [0], line [4])

    def kill (self, serverName, opid):
        server = self.__findServer (serverName)
        server.killOperation (opid)

    def batchKill (self, second):
        """Kill operations running more than given seconds from top to bottom."""
        second = int (second)
        for line in self.__lines:
            if line [3] < second:
                """Do not look futher as the list is reverse ordered by seconds."""
                break
            server = line [0]
            server.killOperation (line [1])

class QueryScreen:
    def __init__ (self, console, chosenServers, autoKillSeconds = None):
        self.__console = console
        self.__blocks = []
        self.__blocks.append (StatusBlock (chosenServers ['status']))
        self.__blocks.append (ReplicationInfoBlock (chosenServers ['replicationInfo']))
        self.__blocks.append (ReplicaSetMemberBlock (chosenServers ['replicaSet']))
        self.__operationBlock = OperationBlock (chosenServers ['operations'], chosenServers ['replicationOperations'])
        self.__blocks.append (self.__operationBlock)
        self.__autoKillSeconds = autoKillSeconds

    def action (self):
        """Reset the blocks, refresh the console, perform actions for the pressed button."""
        button = None
        while button != 'q':
            for block in self.__blocks:
                block.reset ()
            self.__console.refresh (self.__blocks)
            button = self.__console.checkButton (1)
            while button in ('e', 'k'):
                if button == 'e':
                    self.__operationBlock.explainQuery (*self.__console.askForInput ('Server', 'Opid'))
                elif button == 'k':
                    self.__operationBlock.kill (*self.__console.askForInput ('Server', 'Opid'))
                button = self.__console.checkButton ()
            if button == 'K':
                self.__operationBlock.batchKill (*self.__console.askForInput ('Sec'))
            if self.__autoKillSeconds is not None:
                self.__operationBlock.batchKill (self.__autoKillSeconds)

class Configuration:
    defaultFile = os.path.splitext (__file__) [0] + '.conf'
    optionalVariables = ['username', 'password']
    choices = ['status', 'replicationInfo', 'replicaSet', 'operations', 'replicationOperations']

    def __init__ (self, filePath, hosts = [], username = None, password = None):
        """Parse the configuration file using the ConfigParser class from default Python library. Merge the arguments
        with the configuration variables."""
        """Two attempts to import the same class for Python 3 compatibility."""
        try:
            from ConfigParser import SafeConfigParser
        except ImportError:
            from configparser import SafeConfigParser
        defaults = [(variable, None) for variable in self.optionalVariables]
        defaults += [(choice, 'on') for choice in self.choices]
        self.__parser = SafeConfigParser (dict (defaults))
        if self.__parser.read (filePath):
            self.__sections = []
            for host in hosts:
                for section in self.__parser.sections ():
                    if section == host or self.__parser.get (section, 'address') == host:
                        self.__sections.append (section)
            if not self.__sections:
                """If none of the hosts match the sections in the configuration, do not use hosts."""
                self.__sections = self.__parser.sections ()
        else:
            self.__sections = hosts
            self.__username = username
            self.__password = password

    def chosenServers (self, choice):
        """Return servers for the given choice if they are in configuration, return all if configuration does not
        exists."""
        servers = []
        for section in self.__sections:
            if self.__parser.sections ():
                if self.__parser.getboolean (section, choice):
                    address = self.__parser.get (section, 'address')
                    username = self.__parser.get (section, 'username')
                    password = self.__parser.get (section, 'password')
                    servers.append (Server (section, address, username, password))
            else:
                servers.append (Server (section, section, self.__username, self.__password))
        return servers

class Motop:
    """Realtime monitoring tool for several MongoDB servers. Shows current operations ordered by durations every
    second."""
    version = 1.2

    def parseArguments (self):
        """Create ArgumentParser instance. Return parsed arguments."""
        from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
        parser = ArgumentParser (formatter_class = ArgumentDefaultsHelpFormatter, description = self.__doc__)
        parser.add_argument ('hosts', metavar = 'host', nargs = '*', default = ['localhost:27017'],
                help = 'address of the server or section name on the configuration file')
        parser.add_argument ('-u', '--username', dest = 'username', help = 'username for authentication')
        parser.add_argument ('-p', '--password', dest = 'password', help = 'password for authentication')
        parser.add_argument ('-c', '--conf', dest = 'conf', default = Configuration.defaultFile,
                help = 'path of configuration file')
        parser.add_argument ('-V', '--version', action = 'version', version = 'Motop ' + str (self.version))
        parser.add_argument ('-K', '--auto-kill', dest = 'autoKillSeconds',
                help = 'seconds to kill operations automatically')
        return parser.parse_args ()

    def __init__ (self):
        """Parse arguments and the configuration file. Activate console. Get servers from the configuration file or
        from arguments. Show the query screen."""
        arguments = self.parseArguments ()
        config = Configuration (arguments.conf, arguments.hosts, arguments.username, arguments.password)
        with Console () as console:
            queryScreen = QueryScreen (console, {choice: config.chosenServers (choice) for choice in config.choices},
                    autoKillSeconds = arguments.autoKillSeconds)
            try:
                queryScreen.action ()
            except KeyboardInterrupt: pass

if __name__ == '__main__':
    """Run the main program."""
    Motop ()

