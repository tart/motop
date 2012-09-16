motop
=====

Realtime monitoring tool for several MongoDB servers. Shows current operations ordered by durations every second.

## Actions

q   Quit

e   Explain the query

k   Kill operation using "mongo" executable

K   Kill operations older than given seconds using "mongo" executable

## Dependencies

* python 2.6 or greater
* pymongo 2.0 or greater [1]

[1] http://pypi.python.org/pypi/pymongo/

## Configuration

Configuration file can be created by copying motop.default.conf to motop.conf. Section are used for servers. DEFAULT
section can be used for all servers.

address                     The address of the server

hideStatus                  Paramer to disable status check for the server

hideReplicaSet              Paramer to disable replica set status check for the server

hideReplicationOperations   Paramer to hide constantly appeared replication operations on the masters and the slaves

## License

This tool is released under the ISC License, whose text is included to the source file. The ISC License is registered
with and approved by the Open Source Initiative [1].

[1] http://opensource.org/licenses/isc-license.txt
