#!/usr/bin/env python
##
# Tart Database Operations
# "Top" Clone for MongoDB
#
# @author  Emre Hasegeli <emre.hasegeli@tart.com.tr>
# @date    2012-05-19
##

import sys
import os
import termios
import json
from bson import json_util

class Operation:
    def __init__ (self, server, **properties):
        self.server = server
        self.opid = properties ['opid']
        self.active = properties ['active']
        self.type = properties ['op']
        self.namespace = properties ['ns']
        if self.type == 'query':
            self.query = properties ['query']
            self.duration = properties ['secs_running'] if properties.has_key ('secs_running') else 0

    def line (self):
        string = str.ljust (str (self.server), 16)
        string += str.ljust (str (self.opid), 10)
        string += str.ljust (str (self.active), 8)
        string += str.ljust (str (self.type), 10)
        string += str.ljust (str (self.namespace), 20)
        if self.type == 'query':
            string += str.ljust (str (self.duration), 6)
            string += json.dumps (self.query, default = json_util.default) [:80]
        return string

    def getDuration (self):
        return self.duration if hasattr (self, 'duration') else None

    def getQueryJSON (self):
        if self.type == 'query':
            return json.dumps (self.query, default = json_util.default, sort_keys = True, indent = 4)

    def explain (self):
        if self.type == 'query' and self.namespace:
            databaseName, collectionName = self.namespace.split ('.', 1)
            return self.server.explainQuery (databaseName, collectionName, self.query)

    def kill (self):
        return self.server.killOperation (self.opid)

class NonBlockingConsole (object):
    def __enter__ (self):
        self.old_settings = termios.tcgetattr (sys.stdin)
        import tty
        tty.setcbreak (sys.stdin.fileno())
        return self

    def __exit__ (self, type, value, traceback):
        termios.tcsetattr (sys.stdin, termios.TCSADRAIN, self.old_settings)

    def getInput (self):
        import select
        if select.select ([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
            return sys.stdin.read (1)

class Server:
    def __init__ (self, address):
        from pymongo import Connection
        self.__address = address
        self.__connection = Connection (address)

    def explainQuery (self, databaseName, collectionName, query):
        database = getattr (self.__connection, databaseName)
        collection = getattr (database, collectionName)
        cursor = collection.find (query)
        return cursor.explain ()

    def getOperations (self):
        return [Operation (self, **op)for op in self.__connection.local.current_op () ['inprog']]

    def killOperation (self, opid):
        os.system ('echo "db.killOp (' + str (opid) + ')" | mongo ' + self.__address)

    def __str__ (self):
        return self.__address

class Frame:
    def __init__ (self, operations):
        self.__operations = sorted (operations, key = lambda operation: operation.getDuration (), reverse = True)
        os.system ('clear')
        print 'Server          OpId      Active  Type      Namespace           Sec   Query'
        for operation in self.__operations:
            print operation.line ()

    def askForOperation (self):
        print
        server = raw_input ('Server: ',)
        if server:
            opid = raw_input ('OpId: ',)
            if opid:
                for operation in self.__operations:
                    if str (operation.server) == server and str (operation.opid) == opid:
                        return operation

    def explainQuery (self):
        operation = self.askForOperation ()
        if operation:
            explainOutput = operation.explain ()
            print 'Cursor:', explainOutput ['cursor']
            print 'Indexes:',
            for index in explainOutput ['indexBounds']:
                print index,
            print
            print 'IndexOnly:', explainOutput ['indexOnly']
            print 'MultiKey:', explainOutput ['isMultiKey']
            print 'Miliseconds:', explainOutput ['millis']
            print 'Documents:', explainOutput ['n']
            print 'ChunkSkips:', explainOutput ['nChunkSkips']
            print 'Yields:', explainOutput ['nYields']
            print 'Scanned:', explainOutput ['nscanned']
            print 'ScannedObjects:', explainOutput ['nscannedObjects']
            if explainOutput.has_key ('scanAndOrder'):
                print 'ScanAndOrder:', explainOutput ['scanAndOrder']
            print 'Query:', operation.getQueryJSON ()

    def killOperation (self):
        operation = self.askForOperation ()
        if operation:
            operation.kill ()

serverAddress = ('10.42.2.207', '10.42.2.121', '10.42.2.122', '10.42.2.123')

if __name__ == '__main__':
    servers = []
    for serverAddres in serverAddress:
        servers.append (Server (serverAddres))

    from time import sleep
    running = True
    while running:
        with NonBlockingConsole () as nonBlockingConsole:
            frame = Frame ([operation for server in servers for operation in server.getOperations ()])
            sleep (1)
            input = nonBlockingConsole.getInput ()
        if input == 'q':
            running = False
        elif input == 'e':
            frame.explainQuery ()
            raw_input ()
        elif input == 'k':
            frame.killOperation ()
            sleep (1)
