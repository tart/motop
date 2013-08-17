motop
=====

Realtime monitoring tool for several MongoDB servers. Shows current
operations ordered by durations every second.


Usage
-----

Install with easy_install::

    easy_intall motop

Download and install::

    ./setup.py install

Help::

    motop -h

Monitor several servers::

    motop 192.168.124.50 192.158.124.51


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


Configuration (Optional)
------------------------

Configuration file can be created to /etc/motop.conf.

There can be multiple sections on the configuration. Each section
can include these parameters::

    address                 Address of the server (required)

    username                Username to authenticate to the server

    password                Password to authenticate to the server

    status                  Show status (default: on)

    replicationInfo         Show replication status (default: on)

    replicaSet              Show replica set status (default: on)

    operations              Show operations (default: on)

    replicationOperations   Show constantly appeared replication operations
                            on the masters and the slaves (default: on)

"DEFAULT" is the special section. Parameters can be set as default
in this section.

The names of the sections will be used as server names. These names
can alse be used on arguments of the executable.

Example configuration::

    [MongoDB01]
    address=10.42.2.121
    replicationOperations=off

    [MongoDB02]
    address=10.42.2.122

    [MongoDB03]
    address=10.42.2.123

    [MongoDB04]
    address=10.42.2.124
    username=foo
    password=bar


License
-------

This tool is released under the ISC License, whose text is included to the
source files. The ISC License is registered with and approved by the
Open Source Initiative [1].

[1] http://opensource.org/licenses/isc-license.txt

