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
import pymongo

class Server:
    def __init__(self, name, address, username=None, password=None):
        self.__name = name
        self.__address = address
        self.__username = username
        self.__password = password
        self.tryToConnect()

    connectionClass = pymongo.MongoClient if pymongo.version_tuple >= (2, 4) else pymongo.Connection
    connectionParemeters = {'connectTimeoutMS': 1000, 'read_preference': pymongo.ReadPreference.SECONDARY}

    def tryToConnect(self):
        self.__connection = None
        try:
            self.__connection = self.connectionClass(self.__address, **self.connectionParemeters)
        except pymongo.errors.ConnectionFailure as error:
            self.__lastError = error

        if self.__username and self.__password:
            self.__connection.admin.authenticate(self.__username, self.__password)

    def __str__(self):
        return self.__name

    def sameServer(self, name):
        if self.__name == name:
            return True
        if self.__address == name:
            return True
        if ':' not in self.__address and self.__address + ':' + str(self.connectionClass.PORT) == name:
            return True
        return False

    def connected(self):
        return self.__connection is not None

    def __execute(self, procedure, *args, **kwargs):
        """Try 10 times to execute the procedure."""
        for tryCount in range(10):
            try:
                return procedure(*args, **kwargs)
            except pymongo.errors.AutoReconnect as error:
                self.__lastError = error
                time.sleep(0.1)
            except pymongo.errors.OperationFailure as error:
                self.__lastError = error
                break

    def __executeYield(self, *args, **kwargs):
        """Execute the procedure and yield items until get next item fails."""
        try:
            for item in self.__execute(*args, **kwargs):
                yield item
        except pymongo.errors.AutoReconnect as error:
            self.__lastError = error

    def lastError(self):
        return self.__lastError

    def status(self):
        if self.connected():
            result = self.__execute(self.__connection.admin.command, 'serverStatus')

            if result:
                return Result(result)

    def replicationInfo(self):
        """Find replication source from the local collection."""
        for source in self.__executeYield(self.__connection.local.sources.find):
            return Result(source)

    def replicaSetMembers(self):
        """Execute replSetGetStatus operation on the server. Filter arbiters. Calculate the lag. Add relation to the
        member which is the server itself. Return the replica set."""
        replicaSetStatus = self.__execute(self.__connection.admin.command, 'replSetGetStatus')
        if replicaSetStatus:
            for member in replicaSetStatus['members']:
                if member.get('statusStr') not in ['ARBITER']:
                    member['set'] = replicaSetStatus.get('set')
                    member['date'] = replicaSetStatus.get('date')

                    yield Result(member)

    def currentOperations(self, hideReplicationOperations=False):
        """Execute currentOp operation on the server. Filter and yield returning operations."""
        operations = self.__execute(self.__connection.admin.current_op)
        if operations:
            for op in operations['inprog']:
                if hideReplicationOperations:
                    if op.get('op') == 'getmore' and op.get('ns').startswith('local.oplog.'):
                        """Condition to find replication operation on the master."""
                        continue
                    if op.get('op') and op.get('ns') in ('', 'local.sources'):
                        """Condition to find replication operation on the slave."""
                        continue

                yield Result(op)

    def explainQuery(self, namespace, findParameters):
        databaseName, collectionName = namespace.split('.', 1)
        collection = getattr(getattr(self.__connection, databaseName), collectionName)
        cursor = self.__execute(collection.find, **findParameters)
        return self.__execute(cursor.explain)

    def killOperation(self, opid):
        """Kill operation using the "mongo" executable on the shell. That is because I could not make it with
        pymongo."""
        command = "echo 'db.killOp({0})' | mongo".format(str(opid))
        command += ' ' + self.__address + '/admin'
        if self.__username:
            command += ' --username ' + self.__username
        if self.__password:
            command += ' --password ' + self.__password

        exitCode = os.system(command)
        return exitCode == 0

class Result(dict):
    def deepget(self, arg, *args):
        if isinstance(arg, tuple):
            return [self.deepget(a, *args) for a in arg]

        if arg in self:
            if args:
                return Result(self[arg]).deepget(*args)
            return self[arg]

        return None

    def deepgetDiff(self, other, *args):
        value = self.deepget(*args)
        otherValue = other.deepget(*args)
        if value and otherValue:
            return value - otherValue
        return 0

