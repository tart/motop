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

"""Class imports"""
from libmotop.console import Console
from libmotop.server import Server
from libmotop.queryscreen import QueryScreen

"""Two attempts to import the same class for Python 3 compatibility."""
try:
    from ConfigParser import SafeConfigParser
except ImportError:
    from configparser import SafeConfigParser

"""Metadata"""
__name__ = 'motop'
__version__ = 3.0
__doc__ = '"Top" clone for MongoDB.'

"""Main configuration"""
configFile = '/etc/motop.conf'
optionalVariables = ('username', 'password')
choices = ('status', 'replicationInfo', 'replicaSet', 'operations', 'replicationOperations')

def version():
    return __name__ + ' ' + str(__version__)

def parseArguments():
    """Create ArgumentParser instance. Return parsed arguments."""
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter, description=__doc__)
    parser.add_argument('hosts', metavar='host', nargs='*', default=('localhost:27017',),
            help='address of the server or section name on the configuration file')
    parser.add_argument('-u', '--username', dest='username', help='username for authentication')
    parser.add_argument('-p', '--password', dest='password', help='password for authentication')
    parser.add_argument('-c', '--conf', dest='conf', default=configFile,
            help='path of configuration file')
    parser.add_argument('-V', '--version', action='version', version=version())
    parser.add_argument('-K', '--auto-kill', dest='autoKillSeconds',
            help='seconds to kill operations automatically')
    return parser.parse_args()

def commonHosts(config, arguments):
    hosts = []
    for host in arguments.hosts:
        for section in config.sections():
            if section == host or config.get(section, 'address') == host:
                self.__sections.append(section)
    if not hosts:
        """If none of the hosts match the sections in the config, do not use hosts."""
        return config.sections()
    return hosts

def chooseServers(config, arguments, choice):
    """Return servers for the given choice if they are in config, return all if config does not
    exists."""
    if not config.sections():
        return [Server(host, host, arguments.username, arguments.password) for host in arguments.hosts]

    servers = []
    for section in commonHosts(config, arguments):
        if config.sections():
            if not config.has_option(section, choice) or config.getboolean(section, choice):
                address = config.get(section, 'address')
                username = config.get(section, 'username') if config.has_option(section, 'username') else None
                password = config.get(section, 'password') if config.has_option(section, 'password') else None
                servers.append(Server(section, address, username, password))
    return servers

def run():
    """Get the arguments and parse the config file. Activate console. Get servers from the config file
    or from arguments. Show the query screen."""
    arguments = parseArguments()
    config = SafeConfigParser()
    config.read(arguments.conf)

    chosenServers = {}
    for choice in choices:
        chosenServers[choice] = chooseServers(config, arguments, choice)

    with Console() as console:
        queryScreen = QueryScreen(console, chosenServers, autoKillSeconds=arguments.autoKillSeconds)
        try:
            queryScreen.action()
        except KeyboardInterrupt: pass

