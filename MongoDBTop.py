#!/usr/bin/env python
##
# Tart Database Operations
# "Top" Clone for MongoDB
#
# @author  Emre Hasegeli <emre.hasegeli@tart.com.tr>
# @date    2012-05-19
##

from __future__ import print_function
import __builtin__
if hasattr (__builtin__, 'raw_input'):
    __builtin__.input = __builtin__.raw_input

import sys
import os
import tty
import termios
import select
import json
from bson import json_util

class Value (int):
    def __str__ (self):
        if self > 10 ** 12:
            return str (round (self / 10 ** 12)) [:-2] + 'T'
        if self > 10 ** 9:
            return str (round (self / 10 ** 9)) [:-2] + 'G'
        if self > 10 ** 6:
            return str (round (self / 10 ** 6)) [:-2] + 'M'
        if self > 10 ** 3:
            return str (round (self / 10 ** 3)) [:-2] + 'K'
        return int.__str__ (self)

class Printable:
    def line (self): pass
    def sortOrder (self): pass

class Operation (Printable):
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

    def line (self):
        cells = Operation.line (self)
        cells.append (str (self.__namespace))
        cells.append (str (self.__duration))
        cells.append (json.dumps (self.__body, default = json_util.default) [:80])
        return cells

    def printExplain (self):
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

class Server (Printable):
    def __init__ (self, name, address):
        from pymongo import Connection
        assert len (name) < 14
        self.__name = name
        self.__address = address
        self.__connection = Connection (address)

    def sortOrder (self):
        return self.__name

    def line (self):
        serverStatus = self.__connection.admin.command ('serverStatus')
        currentConnection = Value (serverStatus ['connections'] ['current'])
        availableConnection = Value (serverStatus ['connections'] ['available'])
        residentMem = Value (serverStatus ['mem'] ['resident'])
        mappedMem = Value (serverStatus ['mem'] ['mapped'])
        cells = []
        cells.append (str (self))
        cells.append (str (currentConnection) + ' / ' + str (availableConnection))
        cells.append (str (residentMem) + ' / ' + str (mappedMem))
        return cells

    def explainQuery (self, databaseName, collectionName, query):
        database = getattr (self.__connection, databaseName)
        collection = getattr (database, collectionName)
        cursor = collection.find (query)
        return cursor.explain ()

    def currentOperations (self):
        for op in self.__connection.admin.current_op () ['inprog']:
            if op ['op'] == 'query':
                if 'secs_running' in op:
                    yield Query (self, op ['opid'], op ['ns'], op ['query'], op ['secs_running'])
                else:
                    yield Query (self, op ['opid'], op ['ns'], op ['query'])
            else:
                yield Operation (self, op ['opid'])

    def killOperation (self, opid):
        os.system ('echo "db.killOp (' + str (opid) + ')" | mongo ' + self.__address)

    def __str__ (self):
        return self.__name

class ListPrinter:
    def __init__ (self, columnHeaders):
        self.__columnHeaders = columnHeaders
        self.__columnWidths = [len (columnHeader) + 2 for columnHeader in self.__columnHeaders]

    def reset (self, printables):
        self.__printables = sorted (printables, key = lambda printable: printable.sortOrder (), reverse = True)
        for printable in self.__printables:
            assert isinstance (printable, Printable)
            for index, cell in enumerate (printable.line ()):
                assert isinstance (cell, str)
                if len (cell) + 2 > self.__columnWidths [index]:
                    self.__columnWidths [index] = len (cell) + 2

    def printLines (self):
        for index, columnHeader in enumerate (self.__columnHeaders):
            print (columnHeader.ljust (self.__columnWidths [index]), end = '')
        print ()
        for printable in self.__printables:
            for index, cell in enumerate (printable.line ()):
                print (cell.ljust (self.__columnWidths [index]), end = '')
            print ()

    def getLine (self, line):
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

class ConsoleActivator:
    def __enter__ (self):
        self.__settings = termios.tcgetattr (sys.stdin)
        tty.setcbreak (sys.stdin.fileno())
        return Console (self)

    def __exit__ (self, *ignored):
        termios.tcsetattr (sys.stdin, termios.TCSADRAIN, self.__settings)

class ConsoleDeactivator ():
    def __init__ (self, consoleActivator):
        self.__consoleActivator = consoleActivator

    def __enter__ (self):
        self.__consoleActivator.__exit__ ()

    def __exit__ (self, *ignored):
        self.__consoleActivator.__enter__ ()

class Console:
    def __init__ (self, consoleActivator):
        self.__consoleDeactivator = ConsoleDeactivator (consoleActivator)

    def getButton (self):
        button = sys.stdin.read (1)
        if button in ('e', 'k', 'q'):
            return button

    def checkButton (self):
        if select.select ([sys.stdin], [], [], 0) == ([sys.stdin], [], []):
            return self.getButton ()

    def askForOperation (self):
        with self.__consoleDeactivator:
            print ()
            serverName = input ('Server: ')
            if serverName:
                opid = input ('OpId: ')
                if opid:
                    return serverName, opid

servers = {Server ('MongoDBMaster', '10.42.2.207'),
           Server ('MongoDB01' , '10.42.2.121'),
           Server ('MongoDB02', '10.42.2.122'),
           Server ('MongoDB03', '10.42.2.123'),
           Server ('DBAlpha', '10.42.2.206')}

if __name__ == '__main__':
    serversPrinter = ListPrinter (['Server', 'Connections', 'Memory'])
    operationsPrinter = ListPrinter (['Server', 'OpId', 'Namespace', 'Sec', 'Query'])
    from time import sleep
    button = None
    with ConsoleActivator () as console:
        while button != 'q':
            if not button:
                serversPrinter.reset ([server for server in servers ])
                operationsPrinter.reset ([operation for server in servers for operation in server.currentOperations ()])
                os.system ('clear')
                serversPrinter.printLines ()
                print ()
                operationsPrinter.printLines ()
                sleep (1)
                button = console.checkButton ()
            if button in ('e', 'k'):
                operationInput = console.askForOperation ()
                if operationInput:
                    currentOperation = operationsPrinter.getLine (operationInput)
                    if currentOperation:
                        if button == 'e':
                            if isinstance (currentOperation, Query):
                                currentOperation.printExplain ()
                            else:
                                print ('Only queries with namespace can be explained.')
                        elif button == 'k':
                            currentOperation.kill ()
                    else:
                        print ('Invalid operation.')
                    button = console.getButton ()
                else:
                    button = None
