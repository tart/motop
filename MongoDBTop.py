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
from bson import json_utile

class Operation:
    def __init__ (self, server, opid, active, query = None, duration = None):
        self.opid = opid
        self.active = active
        self.server = server
        self.query = query
        self.duration = duration

    def line (self):
        string = str (self.server) + '\t'
        string += str (self.opid) + '\t'
        if len (str (self.opid)) < 8:
            string += '\t'
        string += str (self.active) + '\t'
        string += str (self.duration) + '\t\t'
        if self.query:
            string += json.dumps (self.query, default = json_util.default) [:80]
        return string

    def getQueryJSON (self):
        if self.query:
            return json.dumps (self.query, default = json_util.default, sort_keys = True, indent = 4)

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
        from pymongo.database import Database
        self.__address = address
        self.__database = Database (Connection (address), 'test')

    def getOperations (self):
        operations = []
        for op in self.__database.current_op () ['inprog']:
            opid = op ['opid']
            body = None
            if op.has_key ('query'):
                query = op ['query']
            duration = None
            if op.has_key ('secs_running'):
                duration = op ['secs_running']
            operations.append((Operation (self, opid, op ['active'], query, duration)))
        return operations

    def killOperation (self, opid):
        os.system ('echo "db.killOp (' + str (opid) + ')" | mongo ' + self.__address)

    def __str__ (self):
        return self.__address

class OperationScreenFrame:
    def __init__ (self, operations):
        self.__operations = sorted (operations, key = lambda operation: operation.duration, reverse = True)
        os.system ('clear')
        print 'Server\t\tOpId\t\tActive\tDuration\tQuery'
        for operation in self.__operations:
            print operation.line ()

    def askForOperation (self):
        server = raw_input ('Server: ')
        if server:
            opid = raw_input ('OpId: ')
            if opid:
                for operation in self.__operations:
                    if str (operation.server) == server and str (operation.opid) == opid:
                        return operation

    def showQuery (self):
        running = True
        while running:
            operation = self.askForOperation ()
            if operation:
                print operation.getQueryJSON ()
            else:
                running = False

    def killOperation (self):
        if self.askForOperation ():
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
            frame = OperationScreenFrame ([operation for server in servers for operation in server.getOperations ()])
            input = nonBlockingConsole.getInput ()
            sleep (1)
            if not input:
                input = nonBlockingConsole.getInput ()
        if input == 'q':
            running = False
        elif input == 'e':
            frame.showQuery ()
        elif input == 'k':
            frame.killOperation ()
            sleep (1)
