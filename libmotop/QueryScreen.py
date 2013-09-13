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
import json
from bson import json_util

"""Class imports"""
from .Block import Block

class StatusBlock (Block):
    columnHeaders = ['Server', 'QPS', 'Active', 'Queue', 'Flush', 'Connection', 'Network I/O', 'Memory', 'Page Faults']

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
                cells.append ((status ['bytesIn'], status ['bytesOut']))
                cells.append ((status ['residentMem'], status ['mappedMem']))
                if 'pageFault' in status:
                    cells.append (status ['pageFault'])
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
                source = self.findServer (replicationInfo ['source']) or replicationInfo ['source']
                cells.append ((replicationInfo ['sourceType'], source))
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
        print ()
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
    columnHeaders = ['Server', 'Opid', 'Client', 'State', 'Sec', 'Namespace', 'Query']

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
                    cells.append (operation ['client'])
                    cells.append (operation ['state'])
                    cells.append (operation ['duration'])
                    cells.append (operation ['namespace'])
                    if 'query' in operation:
                        if '$msg' in operation ['query']:
                            cells.append (operation ['query'] ['$msg'])
                        else:
                            cells.append (Query (**operation ['query']))
                    self.__lines.append (cells)
        self.__lines.sort (key = lambda line: line [4] or -1, reverse = True)
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
        if len(line) > 5 and line [5] and isinstance (line [6], Query):
            query = line [6]
            print(query)
            query.print ()
            query.printExplain (line [0], line [5])

    def kill (self, serverName, opid):
        server = self.__findServer (serverName)
        server.killOperation (opid)

    def batchKill (self, second):
        """Kill operations running more than given seconds from top to bottom."""
        second = int (second)
        for line in self.__lines:
            if line [4] < second:
                """Do not look further as the list is reverse ordered by seconds."""
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

            "Pause:"
            if button == 'p':
                button = self.__console.waitButton ()

            "Single operation actions:"
            if button in ('e', 'k'):
                inputValues = self.__console.askForInput ('Server', 'Opid')
                if inputValues:
                    if len (inputValues) == 2:
                        if button == 'e':
                            self.__operationBlock.explainQuery (*inputValues)
                        elif button == 'k':
                            self.__operationBlock.kill (*inputValues)
                    button = self.__console.waitButton ()

            "Batch kill actions:"
            if button == 'K':
                inputValues = self.__console.askForInput ('Sec')
                if inputValues:
                    self.__operationBlock.batchKill (*inputValues)
            if self.__autoKillSeconds is not None:
                self.__operationBlock.batchKill (self.__autoKillSeconds)

