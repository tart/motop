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

class Query:
    def __init__ (self, server, opId, active, body = None, duration = None):
        self.opId = opId
        self.active = active
        self.server = server
        self.body = body
        self.duration = duration

    def line (self):
        string = str (self.server) + '\t'
        string += str (self.opId) + '\t'
        if len (str (self.opId)) < 8:
            string += '\t'
        string += str (self.active) + '\t'
        string += str (self.duration) + '\t\t'
        if self.body:
            import json
            string += json.dumps (self.body) [:80]
        return string

    def getBody (self):
        if self.body:
            import json
            return json.dumps (self.body, sort_keys = True, indent = 4)

class NonBlockingConsole (object):
    def __enter__ (self):
        import termios
        self.old_settings = termios.tcgetattr (sys.stdin)
        import tty
        tty.setcbreak (sys.stdin.fileno())
        return self

    def __exit__ (self, type, value, traceback):
        import termios
        termios.tcsetattr (sys.stdin, termios.TCSADRAIN, self.old_settings)

    def getInput (self):
        import select
        if select.select ([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
            return sys.stdin.read (1)

class Server:
    def __init__ (self, address, databaseName):
        from pymongo import Connection
        from pymongo.database import Database
        self.__address = address
        self.__database = Database (Connection (address), 'tuttur')

    def getQueries (self):
        queries = []
        for op in self.__database.current_op () ['inprog']:
            opId = op ['opid']
            body = None
            if op.has_key ('query'):
                body = op ['query']
            duration = None
            if op.has_key ('secs_running'):
                duration = op ['secs_running']
            queries.append((Query (self, opId, op ['active'], body, duration)))
        return queries

    def __str__ (self):
        return self.__address

class QueryScreen:
    def __init__ (self, queries):
        self.__queries = sorted (queries, key = lambda query: query.duration, reverse = True)

    def showQueries (self):
        os.system ('clear')
        print 'Server\t\tOpId\t\tActive\tDuration\tQuery'
        for query in self.__queries:
            print query.line ()

    def findQuery (self, server, opId):
        for query in self.__queries:
            if str (query.server) == server and str (query.opId) == opId:
                return query

    def showQuery (self, server, opId):
        query = self.findQuery (server, opId)
        if query:
            print query.getBody ()

serverAddress = ('10.42.2.207', '10.42.2.121', '10.42.2.122', '10.42.2.123')

if __name__ == '__main__':
    servers = []
    for serverAddres in serverAddress:
        servers.append (Server (serverAddres, 'test'))

    running = True
    while running:
        with NonBlockingConsole () as nonBlockingConsole:
            screen = QueryScreen ([query for server in servers for query in server.getQueries ()])
            screen.showQueries ()
            from time import sleep
            sleep (1)

            input = nonBlockingConsole.getInput ()
        if input == 'q':
            running = False
        elif input == 'e':
            showingQuery = True
            while showingQuery:
                server = raw_input ('Server: ')
                if server:
                    opId = raw_input ('OpId: ')
                    screen.showQuery (server, opId)
                else:
                    showingQuery = False
