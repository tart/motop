motop
=====

Realtime monitoring tool for several MongoDB servers. Shows current operations ordered by durations every second.

Actions
-------

q   Quit

e   Explain the query

k   Kill operation using "mongo" executable

K   Kill operations older than given seconds using "mongo" executable

Dependencies
------------

* python 2.6 or greater
* pymongo 2.0 or greater [1]

[1] http://pypi.python.org/pypi/pymongo/

Configuration
-------------

Configuration file can be created by copying motop.default.conf to motop.conf. Section are used for servers. DEFAULT
section can be used for all servers. Servers in the configuration can be filtered with arguments.

address                 The address of the server

username                The username to authenticate to the server

password                The password to authenticate to the server

status                  Paramer to show status for the server (default: on)

replicationInfo         Paramer to show replication info for the server (default: on)

replicaSet              Paramer to show replica set status for the server (default: on)

operations              Paramer to show operations for the server (default: on)

replicationOperations   Paramer to show constantly appeared replication operations on the masters and the slaves
                        (default: on)

License
-------

This tool is released under the ISC License, whose text is included to the source file. The ISC License is registered
with and approved by the Open Source Initiative [1].

[1] http://opensource.org/licenses/isc-license.txt

