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

"""Library imports"""
import os
import sys
import time
import json
import pymongo
from bson import json_util
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
                time.sleep (0.1)
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
        values ['bytesIn'] = self.__statusChangePerSecond ('bytesIn', status ['network'] ['bytesIn'])
        values ['bytesOut'] = self.__statusChangePerSecond ('bytesOut', status ['network'] ['bytesOut'])
        values ['residentMem'] = Value (status ['mem'] ['resident'] * (10 ** 6))
        values ['mappedMem'] = Value (status ['mem'] ['mapped'] * (10 ** 6))
        if 'page_faults' in status ['extra_info']:
            values ['pageFault'] = self.__statusChangePerSecond ('pageFault', status ['extra_info'] ['page_faults'])
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
            values ['client'] = op ['client']
            values ['opid'] = op ['opid']
            values ['state'] = op ['op']
            values ['duration'] = op ['secs_running'] if 'secs_running' in op else None
            values ['namespace'] = op ['ns']
            if 'query' in op:
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

