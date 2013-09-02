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
try:
    import __builtin__
    __builtin__.input = __builtin__.raw_input
except ImportError: pass

"""Class imports"""
from .Console import Console
from .Server import Server
from .QueryScreen import QueryScreen

class Configuration:
    defaultFile = '/etc/motop.conf'
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
    version = 2.3

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
            chosenServers = {}
            for choice in config.choices:
                chosenServers [choice] = config.chosenServers (choice)
            queryScreen = QueryScreen (console, chosenServers, autoKillSeconds = arguments.autoKillSeconds)
            try:
                queryScreen.action ()
            except KeyboardInterrupt: pass

