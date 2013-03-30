#!/usr/bin/env python

from setuptools import setup
from libmotop.Motop import Motop

setup (name = 'motop',
        version = str (Motop.version),
        packages = ['libmotop'],
        scripts = ['motop'],
        author = 'Emre Hasegeli',
        author_email = 'hasegeli@tart.com.tr',
        license = 'ICS',
        url = 'https://github.com/tart/motop',
        platforms = 'POSIX',
        description = 'Realtime monitoring tool for several MongoDB servers.',
        keywords = 'mongo realtime monitoring examine explain kill operations',
        install_requires = ['pymongo'])

